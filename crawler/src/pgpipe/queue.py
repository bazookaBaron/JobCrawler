"""Postgres work queue — the Redis replacement.

<schema>.crawl_queue holds one live row per board. Workers claim with
SELECT ... FOR UPDATE SKIP LOCKED so concurrent workers (or an overlapping
Actions run) never process the same board twice. Stale 'claimed' rows are
reset by reclaim_stale() at the start of every run.
"""
from __future__ import annotations

import asyncpg

from src.pgpipe.config import settings


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


async def claim_one(pool: asyncpg.Pool) -> asyncpg.Record | None:
    """Atomically claim the oldest pending task. Returns the board row
    (joined with job_board) or None when the queue is drained."""
    async with pool.acquire() as conn, conn.transaction():
        task = await conn.fetchrow(
            """
            SELECT id, board_slug
            FROM crawl_queue
            WHERE status = 'pending'
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """
        )
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
