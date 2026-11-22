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
    """DELETE rows not seen by a crawl in `days` days (RETAIN_DAYS, default 3)
    OR whose real posting date (posted_at) is older than MAX_POSTING_AGE_DAYS
    (default 30). A still-open job is re-seen daily (last_seen_at = now()) and
    stays; closed/removed jobs vanish within `days` days."""
    d = int(days if days is not None else settings.retain_days)
    m = int(settings.max_posting_age_days)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            f"""
            WITH del AS (
                DELETE FROM job_posting
                WHERE last_seen_at < now() - interval '{d} days'
                   OR posted_at < now() - interval '{m} days'
                RETURNING 1
            )
            SELECT count(*) FROM del
            """
        )
    return int(n or 0)


# --- role mix / geography enforcement ---------------------------------------

def mix_caps(tech: int, india: int) -> tuple[int, int]:
    """(product_cap, us_cap) row counts implied by the configured shares:
    product <= share of (tech+product)  =>  cap = tech * s / (1 - s); same for US vs IN.
    A share <= 0 (or >= 1) disables that cap (returned as a huge number)."""
    big = 10**12
    ps, us = settings.product_share, settings.us_share
    pc = int(tech * ps / (1 - ps)) if 0 < ps < 1 else big
    uc = int(india * us / (1 - us)) if 0 < us < 1 else big
    return pc, uc


async def reclassify(pool: asyncpg.Pool, only_null: bool = True, batch: int = 2000) -> int:
    """Set role_family / role_category from the title for rows that lack it
    (or all rows when only_null=False). Returns rows updated."""
    from src.pgpipe.role_classify import classify_role

    where = "WHERE role_family IS NULL" if only_null else ""
    rows = await pool.fetch(f"SELECT id, title, department FROM job_posting {where}")
    upd = [(classify_role(r["title"], r["department"]), r["id"]) for r in rows]
    n = 0
    for i in range(0, len(upd), batch):
        chunk = upd[i:i + batch]
        async with pool.acquire() as conn:
            await conn.executemany(
                "UPDATE job_posting SET role_family = $1, role_category = $2 WHERE id = $3",
                [(fc[0], fc[1], rid) for fc, rid in chunk],
            )
        n += len(chunk)
    return n


async def mix_counts(pool: asyncpg.Pool) -> dict:
    r = await pool.fetchrow(
        """
        SELECT count(*) AS total,
               count(*) FILTER (WHERE role_family = 'tech') AS tech,
               count(*) FILTER (WHERE role_family = 'product') AS product,
               count(*) FILTER (WHERE country = 'IN') AS india,
               count(*) FILTER (WHERE country = 'US') AS us
        FROM job_posting
        """
    )
    return {k: int(v or 0) for k, v in dict(r).items()}


async def enforce_mix(pool: asyncpg.Pool, dry_run: bool = False) -> dict:
    """Drop rows outside the pool rules and trim to the configured mix:
      1. role_family 'other' / unclassified, and countries outside settings.countries;
      2. product-associated rows beyond product_share of stored rows (oldest first);
      3. US rows beyond us_share of stored rows (oldest first);
    steps 2-3 repeat until stable (they interact). Newest posted_at is kept."""
    out = {"dropped_other_role": 0, "dropped_other_country": 0, "trimmed_product": 0, "trimmed_us": 0,
           "before": await mix_counts(pool)}
    cc = list(settings.countries)
    if settings.role_filter:
        q = "DELETE FROM job_posting WHERE role_family IS NULL OR role_family NOT IN ('tech','product')"
        out["dropped_other_role"] = (await _count_or_delete(pool, q, dry_run))
    q = "DELETE FROM job_posting WHERE country IS NULL OR NOT (country = ANY($1::text[]))"
    out["dropped_other_country"] = await _count_or_delete(pool, q, dry_run, cc)
    if dry_run:
        out["after"] = out["before"]
        return out
    for _ in range(6):
        c = await mix_counts(pool)
        pcap, ucap = mix_caps(c["tech"], c["india"])
        changed = 0
        if settings.role_filter and c["product"] > pcap:
            n = await _trim(pool, "role_family = 'product'", pcap)
            out["trimmed_product"] += n
            changed += n
        c = await mix_counts(pool)
        pcap, ucap = mix_caps(c["tech"], c["india"])
        if c["us"] > ucap:
            n = await _trim(pool, "country = 'US'", ucap)
            out["trimmed_us"] += n
            changed += n
        if not changed:
            break
    out["after"] = await mix_counts(pool)
    return out


async def _count_or_delete(pool, sql: str, dry_run: bool, *args) -> int:
    if dry_run:
        return int(await pool.fetchval(sql.replace("DELETE FROM", "SELECT count(*) FROM", 1), *args) or 0)
    async with pool.acquire() as conn:
        res = await conn.execute(sql, *args)
    return int(res.split()[-1])


async def _trim(pool, cond: str, cap: int) -> int:
    """Keep the `cap` newest rows matching cond; delete the rest."""
    async with pool.acquire() as conn:
        res = await conn.execute(
            f"""
            DELETE FROM job_posting WHERE id IN (
                SELECT id FROM (
                    SELECT id, row_number() OVER (
                        ORDER BY posted_at DESC NULLS LAST, first_seen_at DESC, id DESC) AS rn
                    FROM job_posting WHERE {cond}
                ) x WHERE rn > $1)
            """,
            cap,
        )
    return int(res.split()[-1])


class MixGate:
    """Ingest-side companion to enforce_mix: once product / US rows are at their
    cap, an incoming row is only worth inserting if it is newer than the oldest
    row we would keep (otherwise finalize would delete it again — pure churn).
    Counts refresh at most every `ttl` seconds; finalize remains authoritative."""

    def __init__(self, pool: asyncpg.Pool, ttl: float = 60.0) -> None:
        self._pool = pool
        self._ttl = ttl
        self._at = -1e9
        self._bound: dict[str, object] = {}

    async def refresh(self) -> None:
        import time
        if time.monotonic() - self._at < self._ttl:
            return
        self._at = time.monotonic()
        c = await mix_counts(self._pool)
        pcap, ucap = mix_caps(c["tech"], c["india"])
        bound: dict[str, object] = {}
        for key, cond, cap, n in (("product", "role_family = 'product'", pcap, c["product"]),
                                  ("US", "country = 'US'", ucap, c["us"])):
            if n >= cap:
                if cap <= 0:
                    bound[key] = "all"
                else:
                    bound[key] = await self._pool.fetchval(
                        f"SELECT posted_at FROM job_posting WHERE {cond} "
                        "ORDER BY posted_at DESC NULLS LAST, first_seen_at DESC, id DESC OFFSET $1 LIMIT 1",
                        cap - 1,
                    )
        self._bound = bound

    def allow(self, role_family: str | None, country: str | None, posted_at) -> bool:
        from datetime import datetime, timezone
        ts = posted_at or datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        for key, hit in (("product", role_family == "product"), ("US", country == "US")):
            if not hit or key not in self._bound:
                continue
            b = self._bound[key]
            if b == "all" or (b is not None and ts < b):
                return False
        return True
