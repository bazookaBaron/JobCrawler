"""Crawl one board: call the monitor's discover() directly, map each
DiscoveredJob to a compact row, tech-filter, seniority-tag, upsert.
No scraper, no description fetch, no R2, no enrichment, no Redis.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import asyncpg
import httpx

from src.core.monitors import BoardGoneError, DiscoveredJob, get_discoverer
from src.pgpipe.config import settings
from src.pgpipe.country import classify_country
from src.pgpipe.dbguard import SizeGuard
from src.pgpipe.monitors_icims import discover_icims
from src.pgpipe.monitors_workable import discover_workable
from src.pgpipe.monitors_workday import discover_workday
from src.pgpipe.posted_at import parse_posted_at
from src.pgpipe.seniority import seniority_of
from src.pgpipe.cleanup import MixGate
from src.pgpipe.role_classify import classify_role
from src.pgpipe.tech_filter import TECH_ONLY, is_tech_role

_EMPLOYMENT_MAP = {
    "full-time": "full_time", "fulltime": "full_time", "full time": "full_time",
    "part-time": "part_time", "parttime": "part_time", "part time": "part_time",
    "contract": "contract", "contractor": "contract", "temporary": "temporary",
    "intern": "internship", "internship": "internship",
}


def _norm_employment(v: str | None) -> str | None:
    if not v:
        return None
    return _EMPLOYMENT_MAP.get(v.strip().lower(), v.strip().lower())


def _aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _first(seq: list[str] | None) -> str | None:
    return seq[0] if seq else None


def _department(job: DiscoveredJob) -> str | None:
    md = job.metadata or {}
    for key in ("department", "departments", "team", "category"):
        val = md.get(key)
        if isinstance(val, list) and val:
            return str(val[0])
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def _titles(job: DiscoveredJob) -> list[str]:
    out: list[str] = []
    if job.title:
        out.append(job.title)
    for loc in (job.localizations or {}).values():
        t = (loc or {}).get("title")
        if t and t not in out:
            out.append(t)
    return out


def _rows_from_result(result: object) -> list[DiscoveredJob]:
    if isinstance(result, list):
        return [j for j in result if isinstance(j, DiscoveredJob)]
    # MonitorResult (truncated / hybrid rich): pull rich jobs if present
    jbu = getattr(result, "jobs_by_url", None)
    if isinstance(jbu, dict):
        return [j for j in jbu.values() if isinstance(j, DiscoveredJob)]
    return []


def _is_http_url(url: str | None) -> bool:
    """Only absolute http(s) URLs are stored (the webapp renders them as apply links)."""
    if not url:
        return False
    u = urlparse(url.strip())
    return u.scheme.lower() in ("http", "https") and bool(u.netloc)


def _to_record(company_slug: str, board_slug: str, source: str, job: DiscoveredJob) -> tuple | None:
    if not _is_http_url(job.url):
        return None
    titles = _titles(job)
    locations = job.locations or []
    primary_location = _first(locations)
    raw_date_posted = job.date_posted or None
    return (
        company_slug,
        board_slug,
        source,
        job.source_identity or (job.metadata or {}).get("requisition_id"),
        job.url,
        _first(titles),
        json.dumps(titles),
        primary_location,
        json.dumps(locations),
        (job.job_location_type or None),
        _norm_employment(job.employment_type),
        _department(job),
        raw_date_posted,
        seniority_of(_first(titles)),
        classify_country(primary_location, locations),
        parse_posted_at(raw_date_posted),
        *classify_role(_first(titles), _department(job)),   # rec[16]=role_family, rec[17]=role_category
    )


# Columns refreshed from the crawl on conflict; a row is only rewritten when one of
# them changed or last_seen_at is older than settings.touch_min_hours (see config).
_REFRESH_COLS = (
    "board_slug", "source", "title", "titles", "location", "locations", "location_type",
    "employment_type", "department", "date_posted", "seniority", "country",
    "role_family", "role_category",
)
_STALE_SEEN = (
    f"job_posting.status <> 'open' OR "
    f"job_posting.last_seen_at < now() - interval '{int(settings.touch_min_hours)} hours'"
)

_UPSERT = f"""
INSERT INTO job_posting
    (company_slug, board_slug, source, external_id, url, title, titles,
     location, locations, location_type, employment_type, department, date_posted,
     seniority, country, posted_at, role_family, role_category, first_seen_at, last_seen_at, status)
VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,
        COALESCE($16::timestamptz, now()), $17, $18, now(), now(), 'open')
ON CONFLICT (company_slug, url) DO UPDATE SET
    board_slug      = EXCLUDED.board_slug,
    source          = EXCLUDED.source,
    external_id     = COALESCE(EXCLUDED.external_id, job_posting.external_id),
    title           = EXCLUDED.title,
    titles          = EXCLUDED.titles,
    location        = EXCLUDED.location,
    locations       = EXCLUDED.locations,
    location_type   = EXCLUDED.location_type,
    employment_type = EXCLUDED.employment_type,
    department      = EXCLUDED.department,
    date_posted     = EXCLUDED.date_posted,
    seniority       = EXCLUDED.seniority,
    country         = EXCLUDED.country,
    role_family     = EXCLUDED.role_family,
    role_category   = EXCLUDED.role_category,
    -- A source that stops reporting date_posted on a later crawl shouldn't
    -- blow away a real parsed value we already have.
    -- ($16 itself, not EXCLUDED.posted_at: the insert value falls back to now()
    -- and would otherwise re-date undated postings to "just posted" every crawl.)
    posted_at       = COALESCE($16::timestamptz, job_posting.posted_at),
    last_seen_at    = now(),
    status          = 'open'
WHERE {_STALE_SEEN}
   OR ({", ".join("job_posting." + c for c in _REFRESH_COLS)})
      IS DISTINCT FROM ({", ".join("EXCLUDED." + c for c in _REFRESH_COLS)})
   OR (EXCLUDED.external_id IS NOT NULL
       AND EXCLUDED.external_id IS DISTINCT FROM job_posting.external_id)
   OR ($16::timestamptz IS NOT NULL
       AND $16::timestamptz IS DISTINCT FROM job_posting.posted_at)
"""


# DB-full mode: refresh last_seen_at on rows we already hold, insert nothing.
_TOUCH = (
    "UPDATE job_posting SET last_seen_at = now(), status = 'open' "
    f"WHERE company_slug = $1 AND url = $2 AND ({_STALE_SEEN})"
)


def keep_country(country: str | None) -> bool:
    """Location gate: only postings classified into settings.countries (IN/US)."""
    return country in settings.countries


async def _discover(monitor_type: str, board_dict: dict, http: httpx.AsyncClient):
    # Compact rich Workday monitor (src/pgpipe/monitors_workday.py) — NOT
    # jobseek's URL-only src/core/monitors/workday.py.
    if monitor_type == "workday":
        return await discover_workday(board_dict, http)
    # jobseek's icims monitor is URL-only; use the rich listing parser.
    if monitor_type == "icims":
        return await discover_icims(board_dict, http)
    # jobseek's workable monitor is URL-only; use the one-call widget API.
    if monitor_type == "workable":
        return await discover_workable(board_dict, http)
    return await get_discoverer(monitor_type)(board_dict, http)


async def crawl_board(
    pool: asyncpg.Pool, board: asyncpg.Record, http: httpx.AsyncClient,
    guard: SizeGuard | None = None,
    mix: MixGate | None = None,
) -> dict[str, int]:
    """Fetch + upsert one board. Returns {"upserted", "dropped_nontech",
    "dropped_country", "suppressed"}.
    Raises on failure (caller records + retries via the queue).
    BoardGoneError disables the board."""
    monitor_type = board["monitor_type"]
    cfg = board["monitor_config"]
    if isinstance(cfg, str):
        cfg = json.loads(cfg or "{}")
    board_dict = {
        "board_url": board["board_url"],
        "metadata": cfg or {},
        "board_slug": board["board_slug"],
        "company_slug": board["company_slug"],
    }

    try:
        result = await _discover(monitor_type, board_dict, http)
    except BoardGoneError as gone:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE job_board SET is_enabled=false, last_error=$2, "
                "last_attempt_at=now(), updated_at=now() WHERE board_slug=$1",
                board["board_slug"], f"gone: {gone}",
            )
        return {"upserted": 0, "dropped_nontech": 0, "dropped_country": 0, "suppressed": 0}

    jobs = _rows_from_result(result)
    records: list[tuple] = []
    dropped = 0
    dropped_country = 0
    dropped_mix = 0
    if mix is not None:
        await mix.refresh()
    # Skip postings whose real posting date is older than the window; keep undated.
    oldest = datetime.now(timezone.utc) - timedelta(days=settings.max_posting_age_days)
    for j in jobs:
        rec = _to_record(board["company_slug"], board["board_slug"], monitor_type, j)
        if rec is None:
            continue
        if rec[15] is not None and _aware(rec[15]) < oldest:  # rec[15]=posted_at
            continue
        if not keep_country(rec[14]):  # rec[14]=country; IN/US only
            dropped_country += 1
            continue
        if settings.role_filter and rec[16] not in ("tech", "product"):  # non-tech corporate role
            dropped += 1
            continue
        if TECH_ONLY and not is_tech_role(rec[5], rec[11]):  # legacy strict filter (env PGPIPE_TECH_ONLY)
            dropped += 1
            continue
        if mix is not None and not mix.allow(rec[16], rec[14], rec[15]):  # at product/US cap & older than kept rows
            dropped_mix += 1
            continue
        records.append(rec)

    suppressed = 0
    blocked = bool(guard and records and await guard.refresh())
    async with pool.acquire() as conn, conn.transaction():
        if records and blocked:
            # DB over MAX_DB_MB: keep existing rows fresh, insert nothing new.
            suppressed = len(records)
            await conn.executemany(_TOUCH, [(r[0], r[4]) for r in records])
        elif records:
            await conn.executemany(_UPSERT, records)
        await conn.execute(
            """
            UPDATE job_board
            SET last_attempt_at = now(), last_success_at = now(),
                last_error = NULL, consecutive_failures = 0,
                last_job_count = $2, updated_at = now()
            WHERE board_slug = $1
            """,
            board["board_slug"], len(records),
        )
    return {
        "upserted": 0 if blocked else len(records),
        "dropped_nontech": dropped,
        "dropped_country": dropped_country,
        "suppressed": suppressed,
    }


async def record_board_failure(pool: asyncpg.Pool, board_slug: str, err: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE job_board
            SET last_attempt_at = now(), last_error = $2,
                consecutive_failures = consecutive_failures + 1, updated_at = now()
            WHERE board_slug = $1
            """,
            board_slug, err[:2000],
        )
