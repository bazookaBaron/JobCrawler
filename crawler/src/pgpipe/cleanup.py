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
    """DELETE postings older than `days` days by first_seen_at — the hard age
    cap on the board. Runs at the end of every crawl pass and again in the
    daily cleanup workflow. Keyed on age (not last_seen) so a missed crawl
    can't evict a genuinely fresh posting; anything not seen live for `days`
    days is already older than `days` and gets swept here too.
    """
    d = int(days if days is not None else settings.max_age_days)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            f"""
            WITH del AS (
                DELETE FROM job_posting
                WHERE first_seen_at < now() - interval '{d} days'
                RETURNING 1
            )
            SELECT count(*) FROM del
            """
        )
    return int(n or 0)
