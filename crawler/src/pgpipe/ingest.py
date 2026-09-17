"""Crawl one board: call the monitor's discover() directly, map each
DiscoveredJob to a compact row, tech-filter, seniority-tag, upsert.
No scraper, no description fetch, no R2, no enrichment, no Redis.
"""
from __future__ import annotations

import json

import asyncpg
import httpx

from src.core.monitors import BoardGoneError, DiscoveredJob, get_discoverer
from src.pgpipe.country import classify_country
from src.pgpipe.monitors_workday import discover_workday
from src.pgpipe.posted_at import parse_posted_at
from src.pgpipe.seniority import seniority_of
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


def _to_record(company_slug: str, board_slug: str, source: str, job: DiscoveredJob) -> tuple | None:
    if not job.url:
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
    )


_UPSERT = """
INSERT INTO job_posting
    (company_slug, board_slug, source, external_id, url, title, titles,
     location, locations, location_type, employment_type, department, date_posted,
     seniority, country, posted_at, first_seen_at, last_seen_at, status)
VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,
        COALESCE($16, now()), now(), now(), 'open')
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
    -- A source that stops reporting date_posted on a later crawl shouldn't
    -- blow away a real parsed value we already have.
    posted_at       = COALESCE(EXCLUDED.posted_at, job_posting.posted_at),
    last_seen_at    = now(),
    status          = 'open'
"""


async def _discover(monitor_type: str, board_dict: dict, http: httpx.AsyncClient):
    # Compact rich Workday monitor (src/pgpipe/monitors_workday.py) — NOT
    # jobseek's URL-only src/core/monitors/workday.py.
    if monitor_type == "workday":
        return await discover_workday(board_dict, http)
    return await get_discoverer(monitor_type)(board_dict, http)


async def crawl_board(
    pool: asyncpg.Pool, board: asyncpg.Record, http: httpx.AsyncClient
) -> dict[str, int]:
    """Fetch + upsert one board. Returns {"upserted", "dropped_nontech"}.
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
        return {"upserted": 0, "dropped_nontech": 0}

    jobs = _rows_from_result(result)
    records: list[tuple] = []
    dropped = 0
    for j in jobs:
        rec = _to_record(board["company_slug"], board["board_slug"], monitor_type, j)
        if rec is None:
            continue
        if TECH_ONLY and not is_tech_role(rec[5], rec[11]):  # rec[5]=title, rec[11]=department
            dropped += 1
            continue
        records.append(rec)

    async with pool.acquire() as conn, conn.transaction():
        if records:
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
    return {"upserted": len(records), "dropped_nontech": dropped}


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
