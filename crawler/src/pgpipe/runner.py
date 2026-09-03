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
) -> None:
    while time.monotonic() < deadline:
        row = await queue.claim_one(pool)
        if row is None:
            return  # queue drained
        qid, attempts, slug = row["queue_id"], row["attempts"], row["board_slug"]
        try:
            async with pol.slot(row["board_url"]):
                res = await asyncio.wait_for(
                    crawl_board(pool, row, http), timeout=settings.board_timeout_seconds
                )
            await queue.finish_ok(pool, qid)
            stats["ok"] += 1
            stats["upserted"] += res["upserted"]
            stats["dropped_nontech"] += res["dropped_nontech"]
        except Exception as exc:  # noqa: BLE001 - queue records + retries
            msg = f"{type(exc).__name__}: {exc}"
            await record_board_failure(pool, slug, msg)
            await queue.finish_err(pool, qid, attempts, msg)
            stats["failed"] += 1
            stats["errors"].append(f"{slug}: {msg}")


async def run(pool: asyncpg.Pool, *, time_budget_seconds: float) -> dict:
    started = time.monotonic()
    deadline = started + time_budget_seconds

    run_id = await pool.fetchval(
        "INSERT INTO crawl_run (started_at) VALUES (now()) RETURNING id"
    )

    reclaimed = await queue.reclaim_stale(pool)
    enqueued = await queue.enqueue_due(pool)
    pending = await pool.fetchval("SELECT count(*) FROM crawl_queue WHERE status='pending'")

    stats = {"ok": 0, "failed": 0, "upserted": 0, "dropped_nontech": 0, "errors": []}
    pol = Politeness()
    http = make_client()
    try:
        await asyncio.gather(
            *[
                _worker(f"w{i}", pool, http, pol, deadline, stats)
                for i in range(settings.concurrency)
            ]
        )
    finally:
        await http.aclose()

    remaining = await pool.fetchval("SELECT count(*) FROM crawl_queue WHERE status='pending'")
    closed = await cleanup.close_stale(pool)
    deleted_old = await cleanup.hard_delete_stale(pool)  # enforce the 2-day age cap every pass
    pruned = await cleanup.prune_per_company(pool)

    kept = stats["upserted"]
    dropped = stats["dropped_nontech"]
    seen = kept + dropped
    notes = (
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
        "postings_closed": closed,
        "postings_deleted_old": deleted_old,
        "postings_pruned": pruned,
        "errors": stats["errors"],
    }
