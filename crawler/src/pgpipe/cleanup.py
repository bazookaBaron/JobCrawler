"""Retention: close stale postings, cap per company, expire OLD rows.

job_posting is a short window, not an archive: it keeps only the last
`keep_days` (PGPIPE_KEEP_DAYS, default 2) days of rows by eff_at, open or
closed. Before any row is deleted its identity is written to the compact
`seen_posting` table (first_seen_at / posted_at survive), and ingest refuses to
re-insert a job whose seen_posting date is outside the window, so expiring a row
never makes the job look new again. Inside the window an id stays stable.

A posting is closed when
  * its board was crawled successfully in a run and the job was missing
    (done per board inside ingest.crawl_board), or
  * it has not been seen for `close_after_hours` (36h; boards crawl every 12h)
    or, for source='linkedin', `close_after_hours_linkedin` (30h).
Rows closed for more than `max_age_days` (7) are deleted too, but with the
default keep window the keep window always expires them first.
Reopening (a closed job reappearing) just flips status back.

Safety nets: `enforce_row_cap` (PGPIPE_MAX_ROWS) and `prune_seen`
(PGPIPE_SEEN_TTL_DAYS, 45).
"""
from __future__ import annotations

import logging

import asyncpg

from src.pgpipe.config import settings

log = logging.getLogger(__name__)


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
    Extra rows are CLOSED, not deleted."""
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


# Remember rows (identity + dates) in seen_posting BEFORE they are deleted.
_ENSURE_SEEN = """
INSERT INTO seen_posting (key, first_seen_at, posted_at, last_seen_at)
SELECT DISTINCT ON (k) k, first_seen_at, posted_at, last_seen_at
FROM (SELECT posting_key(linkedin_job_id, source, company_slug, external_id, url) AS k,
             first_seen_at, posted_at, last_seen_at
      FROM job_posting WHERE id = ANY($1::bigint[])) s
ORDER BY k, first_seen_at
ON CONFLICT (key) DO UPDATE SET
    first_seen_at = LEAST(seen_posting.first_seen_at, EXCLUDED.first_seen_at),
    posted_at     = COALESCE(seen_posting.posted_at, EXCLUDED.posted_at),
    last_seen_at  = GREATEST(seen_posting.last_seen_at, EXCLUDED.last_seen_at)
"""


async def _expire(conn: asyncpg.Connection, ids: list[int]) -> int:
    """seen_posting first, then DELETE, in one transaction."""
    if not ids:
        return 0
    async with conn.transaction():
        await conn.execute(_ENSURE_SEEN, ids)
        n = await conn.fetchval(
            "WITH d AS (DELETE FROM job_posting WHERE id = ANY($1::bigint[]) RETURNING 1) "
            "SELECT count(*) FROM d",
            ids,
        )
    return int(n or 0)


async def hard_delete_stale(
    pool: asyncpg.Pool, days: int | None = None, keep_days: int | None = None
) -> int:
    """DELETE rows outside the retention window: eff_at older than `keep_days`
    (open or closed), or CLOSED for more than `days` days. Each deleted row is
    remembered in seen_posting first."""
    d = int(days if days is not None else settings.max_age_days)
    k = int(keep_days if keep_days is not None else settings.keep_days)
    async with pool.acquire() as conn:
        ids = [
            r["id"]
            for r in await conn.fetch(
                """
                SELECT id FROM job_posting
                WHERE coalesce(eff_at, posted_at, first_seen_at)
                          < now() - make_interval(days => $1::int)
                   OR (status = 'closed'
                       AND coalesce(closed_at, last_seen_at) < now() - make_interval(days => $2::int))
                """,
                k,
                d,
            )
        ]
        return await _expire(conn, ids)


async def enforce_row_cap(pool: asyncpg.Pool, max_rows: int | None = None) -> int:
    """Safety cap: if job_posting has more than `max_rows` rows, delete the
    oldest (by eff_at) down to the cap. Returns the number deleted."""
    cap = int(max_rows if max_rows is not None else settings.max_rows)
    if cap <= 0:
        return 0
    async with pool.acquire() as conn:
        total = int(await conn.fetchval("SELECT count(*) FROM job_posting") or 0)
        excess = total - cap
        if excess <= 0:
            return 0
        ids = [
            r["id"]
            for r in await conn.fetch(
                "SELECT id FROM job_posting "
                "ORDER BY coalesce(eff_at, posted_at, first_seen_at) ASC, id ASC LIMIT $1",
                excess,
            )
        ]
        n = await _expire(conn, ids)
    log.warning(
        "row cap hit: job_posting had %s rows > PGPIPE_MAX_ROWS=%s; deleted %s oldest",
        total, cap, n,
    )
    return n


async def prune_seen(pool: asyncpg.Pool, ttl_days: int | None = None) -> int:
    """Forget seen_posting entries not seen for `ttl_days` (45) days."""
    t = int(ttl_days if ttl_days is not None else settings.seen_ttl_days)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            "WITH d AS (DELETE FROM seen_posting "
            "WHERE last_seen_at < now() - make_interval(days => $1::int) RETURNING 1) "
            "SELECT count(*) FROM d",
            t,
        )
    return int(n or 0)
