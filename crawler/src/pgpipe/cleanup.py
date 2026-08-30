"""Retention: close stale postings, cap per company, hard-delete very old rows."""
from __future__ import annotations

import asyncpg

from src.pgpipe.config import settings


async def close_stale(pool: asyncpg.Pool, days: int | None = None) -> int:
    """Mark postings not seen in `days` as status='closed'. Run at end of each run."""
    d = int(days if days is not None else settings.close_after_days)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            f"""
            WITH upd AS (
                UPDATE job_posting
                SET status = 'closed'
                WHERE status <> 'closed'
                  AND last_seen_at < now() - interval '{d} days'
                RETURNING 1
            )
            SELECT count(*) FROM upd
            """
        )
    return int(n or 0)


async def prune_per_company(pool: asyncpg.Pool, cap: int | None = None) -> int:
    """Keep only the newest `cap` postings per company (by first_seen_at)."""
    c = int(cap if cap is not None else settings.per_company_cap)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            """
            WITH ranked AS (
                SELECT id, row_number() OVER (
                    PARTITION BY company_slug
                    ORDER BY first_seen_at DESC, id DESC
                ) AS rn
                FROM job_posting
            ),
            del AS (
                DELETE FROM job_posting
                WHERE id IN (SELECT id FROM ranked WHERE rn > $1)
                RETURNING 1
            )
            SELECT count(*) FROM del
            """,
            c,
        )
    return int(n or 0)


async def hard_delete_stale(pool: asyncpg.Pool, days: int | None = None) -> int:
    """DELETE postings not seen in `days` days. Daily cleanup workflow."""
    d = int(days if days is not None else settings.delete_after_days)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            f"""
            WITH del AS (
                DELETE FROM job_posting
                WHERE last_seen_at < now() - interval '{d} days'
                RETURNING 1
            )
            SELECT count(*) FROM del
            """
        )
    return int(n or 0)
