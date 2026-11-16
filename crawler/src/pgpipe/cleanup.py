"""Retention: close stale postings, cap per company, hard-delete OLD CLOSED rows.

Invariant: an *open* posting is never deleted, so its id (and therefore its
posted_at / first_seen_at) is stable for as long as the job is live.

A posting is closed when
  * its board was crawled successfully in a run and the job was missing
    (done per board inside ingest.crawl_board), or
  * it has not been seen for `close_after_hours` (36h; boards crawl every 12h)
    or, for source='linkedin', `close_after_hours_linkedin` (30h).
Only rows that have been closed for more than `max_age_days` (7) days are
hard-deleted. Reopening (a closed job reappearing) just flips status back.
"""
from __future__ import annotations

import asyncpg

from src.pgpipe.config import settings


async def close_stale(
    pool: asyncpg.Pool,
    hours: int | None = None,
    linkedin_hours: int | None = None,
) -> int:
    """Close open postings not seen within the window. Runs at end of each run."""
    h = int(hours if hours is not None else settings.close_after_hours)
    lh = int(linkedin_hours if linkedin_hours is not None else settings.close_after_hours_linkedin)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            """
            WITH upd AS (
                UPDATE job_posting
                SET status = 'closed', closed_at = now()
                WHERE status = 'open'
                  AND last_seen_at < now() - make_interval(
                        hours => CASE WHEN source = 'linkedin' THEN $2::int ELSE $1::int END)
                RETURNING 1
            )
            SELECT count(*) FROM upd
            """,
            h,
            lh,
        )
    return int(n or 0)


async def prune_per_company(pool: asyncpg.Pool, cap: int | None = None) -> int:
    """Keep only the newest `cap` OPEN postings per company (by posted_at).
    Extra rows are CLOSED, never deleted, so ids stay stable."""
    c = int(cap if cap is not None else settings.per_company_cap)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            """
            WITH ranked AS (
                SELECT id, row_number() OVER (
                    PARTITION BY company_slug
                    ORDER BY coalesce(posted_at, first_seen_at) DESC, id DESC
                ) AS rn
                FROM job_posting
                WHERE status = 'open'
            ),
            upd AS (
                UPDATE job_posting
                SET status = 'closed', closed_at = now()
                WHERE id IN (SELECT id FROM ranked WHERE rn > $1)
                RETURNING 1
            )
            SELECT count(*) FROM upd
            """,
            c,
        )
    return int(n or 0)


async def hard_delete_stale(pool: asyncpg.Pool, days: int | None = None) -> int:
    """DELETE postings that have been CLOSED for more than `days` days (7).
    Open rows are never touched."""
    d = int(days if days is not None else settings.max_age_days)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            """
            WITH del AS (
                DELETE FROM job_posting
                WHERE status = 'closed'
                  AND coalesce(closed_at, last_seen_at) < now() - make_interval(days => $1::int)
                RETURNING 1
            )
            SELECT count(*) FROM del
            """,
            d,
        )
    return int(n or 0)
