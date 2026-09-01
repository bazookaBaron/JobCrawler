"""Postgres work queue — the Redis replacement.

<schema>.crawl_queue holds one live row per board. Workers claim with
SELECT ... FOR UPDATE SKIP LOCKED so concurrent workers (or an overlapping
Actions run) never process the same board twice. Stale 'claimed' rows are
reset by reclaim_stale() at the start of every run.
"""
from __future__ import annotations

import random

import asyncpg

from src.pgpipe.config import settings

# Claim the oldest pending task, optionally restricted to (or excluded from)
# the priority monitor_types. `{extra}` is only ever spliced with the two
# fixed fragments below — never user input; the type list is passed as $1.
_CLAIM_SELECT = """
    SELECT q.id, q.board_slug
    FROM crawl_queue q
    JOIN job_board b ON b.board_slug = q.board_slug
    WHERE q.status = 'pending'{extra}
    ORDER BY q.id
    FOR UPDATE OF q SKIP LOCKED
    LIMIT 1
"""
_ONLY_PRIORITY = " AND b.monitor_type = ANY($1::text[])"
_NON_PRIORITY = " AND NOT (b.monitor_type = ANY($1::text[]))"


async def reclaim_stale(pool: asyncpg.Pool) -> int:
    """Reset claims older than reclaim_after_minutes back to 'pending'."""
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            f"""
            WITH stale AS (
                UPDATE crawl_queue
                SET status = 'pending', claimed_at = NULL, claimed_by = NULL
                WHERE status = 'claimed'
                  AND claimed_at < now() - interval '{int(settings.reclaim_after_minutes)} minutes'
                RETURNING 1
            )
            SELECT count(*) FROM stale
            """
        )
    return int(row or 0)


async def enqueue_due(pool: asyncpg.Pool) -> int:
    """Insert a 'pending' row for every enabled board that has no live task.

    The partial unique index crawl_queue_live_board_uniq makes this safe to
    call repeatedly and race-free.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            """
            WITH ins AS (
                INSERT INTO crawl_queue (board_slug, company_slug, status)
                SELECT b.board_slug, b.company_slug, 'pending'
                FROM job_board b
                WHERE b.is_enabled
                  AND NOT EXISTS (
                      SELECT 1 FROM crawl_queue q
                      WHERE q.board_slug = b.board_slug
                        AND q.status IN ('pending', 'claimed')
                  )
                ON CONFLICT DO NOTHING
                RETURNING 1
            )
            SELECT count(*) FROM ins
            """
        )
    return int(row or 0)


def _claim_plan() -> tuple[tuple[str, list], ...]:
    """Which SELECT(s) to try, in order. With priority weighting on, pick a
    preferred group per call (priority vs. the long tail) by weight, and keep
    the other as a fallback so neither group starves when one drains."""
    prio = list(settings.priority_monitor_types)
    weight = settings.priority_weight
    if not prio or not (0.0 < weight < 1.0):
        return ((_CLAIM_SELECT.format(extra=""), []),)
    if random.random() < weight:
        first, second = _ONLY_PRIORITY, _NON_PRIORITY
    else:
        first, second = _NON_PRIORITY, _ONLY_PRIORITY
    return (
        (_CLAIM_SELECT.format(extra=first), [prio]),
        (_CLAIM_SELECT.format(extra=second), [prio]),
    )


async def claim_one(pool: asyncpg.Pool) -> asyncpg.Record | None:
    """Atomically claim a pending task (biased toward settings.priority_
    monitor_types — see _claim_plan). Returns the board row joined with
    job_board, or None when the queue is drained."""
    async with pool.acquire() as conn, conn.transaction():
        task = None
        for sql, params in _claim_plan():
            task = await conn.fetchrow(sql, *params)
            if task is not None:
                break
        if task is None:
            return None
        return await conn.fetchrow(
            """
            UPDATE crawl_queue q
            SET status = 'claimed', claimed_at = now(), claimed_by = $2,
                attempts = attempts + 1
            FROM job_board b
            WHERE q.id = $1 AND b.board_slug = q.board_slug
            RETURNING q.id AS queue_id, q.attempts,
                      b.board_slug, b.company_slug, b.board_url,
                      b.monitor_type, b.monitor_config
            """,
            task["id"],
            settings.worker_id,
        )


async def finish_ok(pool: asyncpg.Pool, queue_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE crawl_queue SET status='done', finished_at=now(), last_error=NULL "
            "WHERE id=$1",
            queue_id,
        )


async def finish_err(pool: asyncpg.Pool, queue_id: int, attempts: int, err: str) -> None:
    """Retry (back to 'pending') until max_attempts, then park as 'error'."""
    terminal = attempts >= settings.max_attempts
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE crawl_queue
            SET status = $2,
                claimed_at = NULL, claimed_by = NULL,
                finished_at = CASE WHEN $2 = 'error' THEN now() ELSE NULL END,
                last_error = $3
            WHERE id = $1
            """,
            queue_id,
            "error" if terminal else "pending",
            err[:2000],
        )
