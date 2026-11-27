"""`jobs run` — one bounded crawl pass, single process, Postgres-only.

reclaim stale claims -> enqueue every enabled board -> N worker coroutines
claim (FOR UPDATE SKIP LOCKED) + crawl + upsert -> close stale -> prune caps
-> write a crawl_run summary row.
"""
from __future__ import annotations

import asyncio
import time

import asyncpg

from src.pgpipe import cleanup, queue
from src.pgpipe.config import settings
from src.pgpipe.db import with_retry
from src.pgpipe.cleanup import MixGate
from src.pgpipe.dbguard import SizeGuard
from src.pgpipe.http import make_client
from src.pgpipe.ingest import crawl_board, record_board_failure
from src.pgpipe.throttle import Politeness


async def _worker(
    name: str,
    pool: asyncpg.Pool,
    http,
    pol: Politeness,
    deadline: float,
    stats: dict,
    shard: tuple[int, int] | None = None,
    guard: SizeGuard | None = None,
    mix: MixGate | None = None,
) -> None:
    while time.monotonic() < deadline:
        row = await with_retry(lambda: queue.claim_one(pool, shard), what=f"{name}-claim")
        if row is None:
            return  # queue drained
        qid, attempts, slug = row["queue_id"], row["attempts"], row["board_slug"]
        try:
            async with pol.slot(row["board_url"]):
                res = await asyncio.wait_for(
                    crawl_board(pool, row, http, guard, mix),
                    timeout=settings.board_timeout_for(row["monitor_type"]),
                )
            await with_retry(lambda: queue.finish_ok(pool, qid), what=f"{name}-finish")
            stats["ok"] += 1
            stats["upserted"] += res["upserted"]
            stats["dropped_nontech"] += res["dropped_nontech"]
            stats["dropped_country"] += res["dropped_country"]
            stats["suppressed"] += res["suppressed"]
        except Exception as exc:  # noqa: BLE001 - queue records + retries
            msg = f"{type(exc).__name__}: {exc}"
            await with_retry(lambda: record_board_failure(pool, slug, msg), what=f"{name}-fail")
            await with_retry(lambda: queue.finish_err(pool, qid, attempts, msg), what=f"{name}-finish")
            stats["failed"] += 1
            stats["errors"].append(f"{slug}: {msg}")


async def run(
    pool: asyncpg.Pool,
    *,
    time_budget_seconds: float,
    shard: tuple[int, int] | None = None,
    cleanup_after: bool = True,
) -> dict:
    """One bounded pass. `shard=(i, n)` crawls only boards whose hash falls in
    shard i of n (matrix jobs); `cleanup_after=False` skips close/delete/prune
    (run those once, in a final job: `jobs finalize`)."""
    started = time.monotonic()
    deadline = started + time_budget_seconds

    run_id = await pool.fetchval(
        "INSERT INTO crawl_run (started_at) VALUES (now()) RETURNING id"
    )

    reclaimed = await queue.reclaim_stale(pool)
    queue_purged = await queue.purge_finished(pool)
    enqueued = await queue.enqueue_due(pool, shard)
    pending = await pool.fetchval(
        "SELECT count(*) FROM crawl_queue q JOIN job_board b ON b.board_slug = q.board_slug "
        "WHERE q.status='pending'" + queue.shard_clause(shard)
    )

    guard = SizeGuard(pool)
    mix = MixGate(pool)
    stats = {"ok": 0, "failed": 0, "upserted": 0, "dropped_nontech": 0,
             "dropped_country": 0, "suppressed": 0, "errors": []}
    pol = Politeness()
    http = make_client()
    try:
        await asyncio.gather(
            *[
                _worker(f"w{i}", pool, http, pol, deadline, stats, shard, guard, mix)
                for i in range(settings.concurrency)
            ]
        )
    finally:
        await http.aclose()

    remaining = await pool.fetchval(
        "SELECT count(*) FROM crawl_queue q JOIN job_board b ON b.board_slug = q.board_slug "
        "WHERE q.status='pending'" + queue.shard_clause(shard)
    )
    closed = deleted_old = pruned = 0
    if cleanup_after:
        closed = await cleanup.close_stale(pool)
        deleted_old = await cleanup.hard_delete_stale(pool, force=stats["ok"] > 0)  # not seen in RETAIN_DAYS or posted > MAX_POSTING_AGE_DAYS ago
        pruned = await cleanup.prune_per_company(pool)

    kept = stats["upserted"]
    dropped = stats["dropped_nontech"]
    seen = kept + dropped
    notes = (
        f"shard={shard} queue_purged={queue_purged} db_mb={guard.size_mb:.0f} "
        f"suppressed_by_size_guard={stats['suppressed']} dropped_country={stats['dropped_country']} "
        f"reclaimed={reclaimed} enqueued={enqueued} remaining_pending={remaining} "
        f"deleted_old={deleted_old} "
        f"tech_filter: kept={kept} dropped_nontech={dropped} "
        f"({round(100.0 * kept / seen, 1) if seen else 0.0}% kept of {seen} seen)"
    )
    await pool.execute(
        """
        UPDATE crawl_run SET finished_at = now(),
            boards_total = $2, boards_ok = $3, boards_failed = $4,
            postings_upserted = $5, postings_closed = $6, postings_pruned = $7,
            notes = $8
        WHERE id = $1
        """,
        run_id,
        int(pending or 0),
        stats["ok"],
        stats["failed"],
        stats["upserted"],
        closed,
        pruned,
        notes,
    )

    return {
        "run_id": run_id,
        "elapsed_s": round(time.monotonic() - started, 1),
        "reclaimed": reclaimed,
        "enqueued": enqueued,
        "boards_attempted": stats["ok"] + stats["failed"],
        "boards_ok": stats["ok"],
        "boards_failed": stats["failed"],
        "boards_remaining_pending": int(remaining or 0),
        "postings_upserted": stats["upserted"],
        "postings_dropped_nontech": dropped,
        "postings_dropped_country": stats["dropped_country"],
        "postings_suppressed_db_full": stats["suppressed"],
        "db_mb": round(guard.size_mb, 1),
        "shard": list(shard) if shard else None,
        "postings_closed": closed,
        "postings_deleted_old": deleted_old,
        "postings_pruned": pruned,
        "errors": stats["errors"],
    }
