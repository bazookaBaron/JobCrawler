"""`jobs run` — one bounded crawl pass, single process, Postgres-only.

reclaim stale claims -> enqueue every enabled board -> N worker coroutines
claim (FOR UPDATE SKIP LOCKED) + crawl + upsert (+ close jobs missing from a
successfully crawled board) -> close stale (36h / 30h linkedin) -> cap per
company (closes, never deletes) -> expire rows outside the keep window (after
seen_posting remembers them) -> row cap -> prune seen_posting -> record
board health + log disabled/failing boards -> write a crawl_run summary row.
Inside the keep window (PGPIPE_KEEP_DAYS) ids stay stable.
"""
from __future__ import annotations

import asyncio
import time

import asyncpg
import structlog

from src.pgpipe import cleanup, queue
from src.pgpipe.config import settings
from src.pgpipe.http import make_client
from src.pgpipe.ingest import crawl_board, record_board_failure
from src.pgpipe.throttle import Politeness

log = structlog.get_logger()


async def _worker(
    name: str,
    pool: asyncpg.Pool,
    http,
    pol: Politeness,
    deadline: float,
    stats: dict,
    run_id: int | None = None,
) -> None:
    while time.monotonic() < deadline:
        row = await queue.claim_one(pool)
        if row is None:
            return  # queue drained
        qid, attempts, slug = row["queue_id"], row["attempts"], row["board_slug"]
        try:
            async with pol.slot(row["board_url"]):
                res = await asyncio.wait_for(
                    crawl_board(pool, row, http, run_id), timeout=settings.board_timeout_seconds
                )
            await queue.finish_ok(pool, qid)
            stats["ok"] += 1
            stats["upserted"] += res["upserted"]
            stats["dropped_nontech"] += res["dropped_nontech"]
            stats["closed_missing"] += res.get("closed_missing", 0)
            if res.get("gone"):
                stats["gone"].append(slug)
        except Exception as exc:  # noqa: BLE001 - queue records + retries
            msg = f"{type(exc).__name__}: {exc}"
            await record_board_failure(pool, slug, msg)
            await queue.finish_err(pool, qid, attempts, msg)
            stats["failed"] += 1
            stats["errors"].append(f"{slug}: {msg}")


_FAILING_THRESHOLD = 5


async def _record_board_health(pool: asyncpg.Pool, run_id: int, gone: list[str]) -> dict:
    """Log boards disabled as gone this run + boards failing persistently, both
    to stdout/structlog (end-of-run summary) and to <schema>.board_health."""
    rows = await pool.fetch(
        """
        SELECT board_slug, consecutive_failures, last_error FROM job_board
        WHERE is_enabled AND consecutive_failures >= $1
        ORDER BY consecutive_failures DESC, board_slug
        """,
        _FAILING_THRESHOLD,
    )
    failing = [r["board_slug"] for r in rows]
    for r in rows:
        await pool.execute(
            """
            INSERT INTO board_health (board_slug, run_id, event, detail)
            SELECT $1, $2, 'failing', $3
            WHERE NOT EXISTS (
                SELECT 1 FROM board_health h
                WHERE h.board_slug = $1 AND h.event = 'failing'
                  AND h.created_at > now() - interval '24 hours')
            """,
            r["board_slug"], run_id,
            f"{r['consecutive_failures']} consecutive failures: {(r['last_error'] or '')[:500]}",
        )
    if gone:
        log.warning("board_health.disabled_gone", count=len(gone), boards=gone)
        print(f"WARNING: {len(gone)} board(s) disabled as GONE this run: {', '.join(gone)}")
    if failing:
        log.warning("board_health.failing", count=len(failing), boards=failing[:50])
        print(f"WARNING: {len(failing)} board(s) failing >= {_FAILING_THRESHOLD} runs in a row: "
              f"{', '.join(failing[:50])}")
    return {"gone": gone, "failing": failing}


async def run(pool: asyncpg.Pool, *, time_budget_seconds: float) -> dict:
    started = time.monotonic()
    deadline = started + time_budget_seconds

    run_id = await pool.fetchval(
        "INSERT INTO crawl_run (started_at) VALUES (now()) RETURNING id"
    )

    reclaimed = await queue.reclaim_stale(pool)
    enqueued = await queue.enqueue_due(pool)
    pending = await pool.fetchval("SELECT count(*) FROM crawl_queue WHERE status='pending'")

    stats = {
        "ok": 0, "failed": 0, "upserted": 0, "dropped_nontech": 0,
        "closed_missing": 0, "gone": [], "errors": [],
    }
    pol = Politeness()
    http = make_client()
    try:
        await asyncio.gather(
            *[
                _worker(f"w{i}", pool, http, pol, deadline, stats, run_id)
                for i in range(settings.concurrency)
            ]
        )
    finally:
        await http.aclose()

    remaining = await pool.fetchval("SELECT count(*) FROM crawl_queue WHERE status='pending'")
    closed = stats["closed_missing"] + await cleanup.close_stale(pool)
    pruned = await cleanup.prune_per_company(pool)
    deleted_old = await cleanup.hard_delete_stale(pool)  # outside the keep window
    capped = await cleanup.enforce_row_cap(pool)
    seen_pruned = await cleanup.prune_seen(pool)
    health = await _record_board_health(pool, run_id, stats["gone"])

    kept = stats["upserted"]
    dropped = stats["dropped_nontech"]
    seen = kept + dropped
    notes = (
        f"reclaimed={reclaimed} enqueued={enqueued} remaining_pending={remaining} "
        f"deleted_old={deleted_old} capped={capped} seen_pruned={seen_pruned} closed_missing={stats['closed_missing']} "
        f"boards_gone={len(stats['gone'])} boards_failing={len(health['failing'])} "
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
        "postings_capped": capped,
        "seen_pruned": seen_pruned,
        "boards_disabled_gone": stats["gone"],
        "boards_failing": health["failing"],
        "postings_pruned": pruned,
        "errors": stats["errors"],
    }
