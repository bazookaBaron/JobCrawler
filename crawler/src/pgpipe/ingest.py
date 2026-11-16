"""Crawl one board: call the monitor's discover() directly, map each
DiscoveredJob to a compact row, tech-filter, seniority-tag, upsert.
No scraper, no description fetch, no R2, no enrichment, no Redis.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import asyncpg
import httpx

from src.core.monitors import BoardGoneError, DiscoveredJob, get_discoverer
from src.pgpipe.country import classify_country
from src.pgpipe.monitors_workday import discover_workday
from src.pgpipe.posted_at import is_relative_date, parse_posted_at
from src.pgpipe.seniority import seniority_of
from src.pgpipe.tech_filter import TECH_ONLY, is_tech_role
from src.pgpipe.urlnorm import canonical_url

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


def _to_record(
    company_slug: str,
    board_slug: str,
    source: str,
    job: DiscoveredJob,
    run_id: int | None = None,
    now: datetime | None = None,
) -> tuple | None:
    """Map a DiscoveredJob to an upsert tuple. Returns None for rows that must
    be dropped (no URL, or a non-http(s) URL)."""
    url = canonical_url(job.url)
    if not url:
        return None
    now = now or datetime.now(timezone.utc)
    titles = _titles(job)
    locations = job.locations or []
    primary_location = _first(locations)
    raw_date_posted = job.date_posted or None
    posted = parse_posted_at(raw_date_posted, now)
    # Undated or relative ("Posted 3 Days Ago") dates are estimates: they may
    # seed posted_at on insert but must never overwrite it on a later crawl.
    keep_existing = posted is None or is_relative_date(raw_date_posted)
    return (
        company_slug,
        board_slug,
        source,
        job.source_identity or (job.metadata or {}).get("requisition_id"),
        url,
        _first(titles),
        json.dumps(titles),
        primary_location,
        json.dumps(locations),
        (job.job_location_type or None),
        _norm_employment(job.employment_type),
        _department(job),
        raw_date_posted,
        seniority_of(_first(titles)),
        classify_country(primary_location, locations, _first(titles)),
        posted,
        keep_existing,
        run_id,
    )


# posted_at: first insert uses the real date, else first_seen (now()). On
# conflict a real absolute date wins; undated/relative rows keep what we had.
_UPSERT = """
INSERT INTO job_posting
    (company_slug, board_slug, source, external_id, url, title, titles,
     location, locations, location_type, employment_type, department, date_posted,
     seniority, country, posted_at, first_seen_at, last_seen_at, status,
     last_seen_run_id)
VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,
        COALESCE($16, now()), now(), now(), 'open', $18)
ON CONFLICT (company_slug, url) DO UPDATE SET
    board_slug       = EXCLUDED.board_slug,
    source           = EXCLUDED.source,
    external_id      = COALESCE(EXCLUDED.external_id, job_posting.external_id),
    title            = EXCLUDED.title,
    titles           = EXCLUDED.titles,
    location         = EXCLUDED.location,
    locations        = EXCLUDED.locations,
    location_type    = EXCLUDED.location_type,
    employment_type  = EXCLUDED.employment_type,
    department       = EXCLUDED.department,
    date_posted      = EXCLUDED.date_posted,
    seniority        = EXCLUDED.seniority,
    country          = EXCLUDED.country,
    posted_at        = CASE WHEN $17::boolean
                            THEN COALESCE(job_posting.posted_at, EXCLUDED.posted_at)
                            ELSE EXCLUDED.posted_at END,
    last_seen_at     = now(),
    last_seen_run_id = EXCLUDED.last_seen_run_id,
    closed_at        = NULL,
    status           = 'open'
"""

# A job whose URL changed but whose provider id did not keeps its row (and id).
_RELINK_URL = """
UPDATE job_posting jp SET url = v.url
FROM unnest($1::text[], $2::text[], $3::text[], $4::text[])
     AS v(company_slug, source, external_id, url)
WHERE jp.company_slug = v.company_slug AND jp.source = v.source
  AND jp.external_id = v.external_id AND jp.url <> v.url
  AND NOT EXISTS (SELECT 1 FROM job_posting x
                  WHERE x.company_slug = jp.company_slug AND x.url = v.url)
"""

# Board crawled OK this run -> any of its open jobs we did not just touch is gone.
_CLOSE_MISSING = """
UPDATE job_posting SET status = 'closed', closed_at = now()
WHERE board_slug = $1 AND status = 'open' AND source <> 'linkedin'
  AND last_seen_run_id IS DISTINCT FROM $2
"""

# Don't treat a possibly-capped listing as complete (workday caps at 2000).
_MAX_TRUSTED_LISTING = 1990


def _dedupe_records(records: list[tuple]) -> list[tuple]:
    """Collapse rows that would collide on a unique key inside one batch
    ((company_slug, url) or (source, company_slug, external_id)); first wins."""
    seen_url: set[tuple] = set()
    seen_ext: set[tuple] = set()
    out: list[tuple] = []
    for r in records:
        k_url = (r[0], r[4])
        k_ext = (r[2], r[0], r[3]) if r[3] else None
        if k_url in seen_url or (k_ext and k_ext in seen_ext):
            continue
        seen_url.add(k_url)
        if k_ext:
            seen_ext.add(k_ext)
        out.append(r)
    return out


async def _discover(monitor_type: str, board_dict: dict, http: httpx.AsyncClient):
    # Compact rich Workday monitor (src/pgpipe/monitors_workday.py) — NOT
    # jobseek's URL-only src/core/monitors/workday.py.
    if monitor_type == "workday":
        return await discover_workday(board_dict, http)
    return await get_discoverer(monitor_type)(board_dict, http)


async def crawl_board(
    pool: asyncpg.Pool,
    board: asyncpg.Record,
    http: httpx.AsyncClient,
    run_id: int | None = None,
) -> dict:
    """Fetch + upsert one board. Returns counts (+ "gone" when the board was
    retired). Raises on failure (caller records + retries via the queue).
    BoardGoneError disables the board and logs it to board_health."""
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
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "UPDATE job_board SET is_enabled=false, last_error=$2, "
                "last_attempt_at=now(), updated_at=now() WHERE board_slug=$1",
                board["board_slug"], f"gone: {gone}",
            )
            await conn.execute(
                "INSERT INTO board_health (board_slug, run_id, event, detail) "
                "VALUES ($1, $2, 'gone', $3)",
                board["board_slug"], run_id, str(gone)[:1000],
            )
        return {"upserted": 0, "dropped_nontech": 0, "closed_missing": 0, "gone": True}

    jobs = _rows_from_result(result)
    now = datetime.now(timezone.utc)
    records: list[tuple] = []
    dropped = 0
    for j in jobs:
        rec = _to_record(
            board["company_slug"], board["board_slug"], monitor_type, j, run_id, now
        )
        if rec is None:
            continue
        if TECH_ONLY and not is_tech_role(rec[5], rec[11]):  # rec[5]=title, rec[11]=department
            dropped += 1
            continue
        records.append(rec)
    records = _dedupe_records(records)

    # Only a complete, non-empty listing may close missing jobs.
    complete = (
        run_id is not None
        and isinstance(result, list)
        and 0 < len(jobs) < _MAX_TRUSTED_LISTING
    )

    closed_missing = 0
    async with pool.acquire() as conn, conn.transaction():
        if records:
            with_ext = [r for r in records if r[3]]
            if with_ext:
                await conn.execute(
                    _RELINK_URL,
                    [r[0] for r in with_ext], [r[2] for r in with_ext],
                    [r[3] for r in with_ext], [r[4] for r in with_ext],
                )
            await conn.executemany(_UPSERT, records)
        if complete:
            res = await conn.execute(_CLOSE_MISSING, board["board_slug"], run_id)
            closed_missing = int(res.split()[-1]) if res and res.startswith("UPDATE") else 0
        await conn.execute(
            """
            UPDATE job_board
            SET last_attempt_at = now(), last_success_at = now(),
                last_success_run_id = COALESCE($3, last_success_run_id),
                last_error = NULL, consecutive_failures = 0,
                last_job_count = $2, updated_at = now()
            WHERE board_slug = $1
            """,
            board["board_slug"], len(records), run_id,
        )
    return {
        "upserted": len(records),
        "dropped_nontech": dropped,
        "closed_missing": closed_missing,
        "gone": False,
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
