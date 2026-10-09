"""DB side of `report build`: one transaction that overwrites the single daily
row and folds the day into its ISO week's row.

Re-running the same day is safe: the earlier run's rollup is subtracted from
the week before the new one is added, and day-over-day comparisons use the
stored previous-day baseline / job_state instead of the earlier same-day run.
"""

from __future__ import annotations

import json
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import structlog

from src.pgpipe.config import CRAWLER_ROOT
from src.report import rollup as rollup_mod
from src.report import sections, state
from src.report.config import SCHEMA_SQL

log = structlog.get_logger()

SECTION_COLS = (
    "profiles",
    "salary",
    "experience",
    "geography",
    "skills",
    "companies",
    "attributes",
)
TREND_DIMS = ("job_family", "profile", "title", "country", "technology", "company")


def code_version() -> str:
    sha = os.environ.get("GITHUB_SHA", "").strip()
    if sha:
        return sha[:12]
    try:
        return (CRAWLER_ROOT / "VERSION").read_text(encoding="utf-8").strip()
    except OSError:
        return "dev"


async def apply_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL.read_text(encoding="utf-8"))


def _j(v) -> Any:
    if v is None:
        return None
    return v if isinstance(v, (dict, list)) else json.loads(v)


def compact_from_sections(row: dict | asyncpg.Record | None) -> dict | None:
    """Rebuild the comparison baseline {kpi, dims} from stored section JSON."""
    if not row:
        return None
    dims: dict[str, list[dict]] = {}
    for col in SECTION_COLS:
        sec = _j(row[col]) or {}
        for name, val in sec.items():
            if name in sections._SECTION_DIMS.get(col, ()) and isinstance(val, list):
                dims[name] = val
    return {"kpi": _j(row["kpi"]), "dims": sections.compact(dims)}


async def read_previous_daily(pool: asyncpg.Pool) -> dict | None:
    row = await pool.fetchrow("SELECT * FROM report_daily.report WHERE id = 1")
    return dict(row) if row else None


def comparison_base(
    prev: dict | None, today: date
) -> tuple[state.JobState | None, dict | None, date | None]:
    """-> (job_state to diff against, baseline to compare with, its date)."""
    if not prev:
        return None, None, None
    if prev["report_date"] == today:
        base = _j(prev["baseline"])
        return state.decode(prev["prev_job_state"]), base, (base or {}).get("date")
    base = compact_from_sections(prev)
    if base:
        base["date"] = prev["report_date"].isoformat()
    return state.decode(prev["job_state"]), base, prev["report_date"]


def _headline(kpi: dict) -> list:
    return [
        kpi.get(k)
        for k in (
            "total_jobs",
            "unique_roles",
            "companies_hiring",
            "countries",
            "new_jobs",
            "removed_jobs",
            "remote_pct",
            "hybrid_pct",
            "onsite_pct",
            "salary_disclosure_pct",
            "salary_avg_usd",
            "salary_median_usd",
            "salary_p10_usd",
            "salary_p25_usd",
            "salary_p75_usd",
            "salary_p90_usd",
            "yoe_median",
            "visa_offered_pct",
        )
    ]


_DAILY_UPSERT = """
INSERT INTO report_daily.report (
    id, report_date, iso_week, generated_at, status, code_version,
    total_jobs, unique_roles, companies_hiring, countries, new_jobs, removed_jobs,
    remote_pct, hybrid_pct, onsite_pct, salary_disclosure_pct, salary_avg_usd,
    salary_median_usd, salary_p10_usd, salary_p25_usd, salary_p75_usd, salary_p90_usd,
    yoe_median, visa_offered_pct,
    kpi, kpi_deltas, coverage, fx_rates, profiles, salary, experience, geography,
    skills, companies, attributes, data_quality, rollup, baseline, job_state, prev_job_state)
VALUES (1, $1, $2, now(), $3, $4,
    $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
    $23::jsonb, $24::jsonb, $25::jsonb, $26::jsonb, $27::jsonb, $28::jsonb, $29::jsonb,
    $30::jsonb, $31::jsonb, $32::jsonb, $33::jsonb, $34::jsonb, $35::jsonb, $36::jsonb,
    $37, $38)
ON CONFLICT (id) DO UPDATE SET
    report_date = EXCLUDED.report_date, iso_week = EXCLUDED.iso_week,
    generated_at = EXCLUDED.generated_at, status = EXCLUDED.status,
    code_version = EXCLUDED.code_version, total_jobs = EXCLUDED.total_jobs,
    unique_roles = EXCLUDED.unique_roles, companies_hiring = EXCLUDED.companies_hiring,
    countries = EXCLUDED.countries, new_jobs = EXCLUDED.new_jobs,
    removed_jobs = EXCLUDED.removed_jobs, remote_pct = EXCLUDED.remote_pct,
    hybrid_pct = EXCLUDED.hybrid_pct, onsite_pct = EXCLUDED.onsite_pct,
    salary_disclosure_pct = EXCLUDED.salary_disclosure_pct,
    salary_avg_usd = EXCLUDED.salary_avg_usd, salary_median_usd = EXCLUDED.salary_median_usd,
    salary_p10_usd = EXCLUDED.salary_p10_usd, salary_p25_usd = EXCLUDED.salary_p25_usd,
    salary_p75_usd = EXCLUDED.salary_p75_usd, salary_p90_usd = EXCLUDED.salary_p90_usd,
    yoe_median = EXCLUDED.yoe_median, visa_offered_pct = EXCLUDED.visa_offered_pct,
    kpi = EXCLUDED.kpi, kpi_deltas = EXCLUDED.kpi_deltas, coverage = EXCLUDED.coverage,
    fx_rates = EXCLUDED.fx_rates, profiles = EXCLUDED.profiles, salary = EXCLUDED.salary,
    experience = EXCLUDED.experience, geography = EXCLUDED.geography,
    skills = EXCLUDED.skills, companies = EXCLUDED.companies,
    attributes = EXCLUDED.attributes, data_quality = EXCLUDED.data_quality,
    rollup = EXCLUDED.rollup, baseline = EXCLUDED.baseline,
    job_state = EXCLUDED.job_state, prev_job_state = EXCLUDED.prev_job_state
"""

_WEEKLY_UPSERT = """
INSERT INTO report_weekly.report (
    iso_week, week_start, week_end, status, days_covered, report_dates,
    first_run_at, last_run_at, avg_coverage_pct, code_version,
    total_jobs_avg, unique_roles_avg, companies_hiring_avg, new_jobs_sum, removed_jobs_sum,
    remote_pct, hybrid_pct, onsite_pct, salary_disclosure_pct, salary_avg_usd,
    salary_median_usd, salary_p10_usd, salary_p25_usd, salary_p75_usd, salary_p90_usd,
    new_listing_salary_median_usd, yoe_median, visa_offered_pct,
    daily_series, kpi, kpi_deltas, coverage, profiles, salary, experience, geography,
    skills, companies, attributes, data_quality, rollup)
VALUES ($1, $2, $3, 'in_progress', $4, $5, now(), now(), $6, $7,
    $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25,
    $26::jsonb, $27::jsonb, $28::jsonb, $29::jsonb, $30::jsonb, $31::jsonb, $32::jsonb,
    $33::jsonb, $34::jsonb, $35::jsonb, $36::jsonb, $37::jsonb, $38::jsonb)
ON CONFLICT (iso_week) DO UPDATE SET
    status = 'in_progress', days_covered = EXCLUDED.days_covered,
    report_dates = EXCLUDED.report_dates, last_run_at = now(),
    avg_coverage_pct = EXCLUDED.avg_coverage_pct, code_version = EXCLUDED.code_version,
    total_jobs_avg = EXCLUDED.total_jobs_avg, unique_roles_avg = EXCLUDED.unique_roles_avg,
    companies_hiring_avg = EXCLUDED.companies_hiring_avg,
    new_jobs_sum = EXCLUDED.new_jobs_sum, removed_jobs_sum = EXCLUDED.removed_jobs_sum,
    remote_pct = EXCLUDED.remote_pct, hybrid_pct = EXCLUDED.hybrid_pct,
    onsite_pct = EXCLUDED.onsite_pct, salary_disclosure_pct = EXCLUDED.salary_disclosure_pct,
    salary_avg_usd = EXCLUDED.salary_avg_usd, salary_median_usd = EXCLUDED.salary_median_usd,
    salary_p10_usd = EXCLUDED.salary_p10_usd, salary_p25_usd = EXCLUDED.salary_p25_usd,
    salary_p75_usd = EXCLUDED.salary_p75_usd, salary_p90_usd = EXCLUDED.salary_p90_usd,
    new_listing_salary_median_usd = EXCLUDED.new_listing_salary_median_usd,
    yoe_median = EXCLUDED.yoe_median, visa_offered_pct = EXCLUDED.visa_offered_pct,
    daily_series = EXCLUDED.daily_series, kpi = EXCLUDED.kpi,
    kpi_deltas = EXCLUDED.kpi_deltas, coverage = EXCLUDED.coverage,
    profiles = EXCLUDED.profiles, salary = EXCLUDED.salary,
    experience = EXCLUDED.experience, geography = EXCLUDED.geography,
    skills = EXCLUDED.skills, companies = EXCLUDED.companies,
    attributes = EXCLUDED.attributes, data_quality = EXCLUDED.data_quality,
    rollup = EXCLUDED.rollup
"""


def _dump(v) -> str:
    return json.dumps(v, separators=(",", ":"), default=str)


def _apply_trends(sec: dict, trend: dict[str, dict | None]) -> None:
    """Add d_jobs_4w / d_jobs_pct_12w ... to the weekly rows of TREND_DIMS."""
    for suffix, base in trend.items():
        if not base:
            continue
        for col, dims in sections._SECTION_DIMS.items():
            for dim in dims:
                if dim in TREND_DIMS and dim in sec.get(col, {}):
                    sections.add_deltas(sec[col][dim], base["dims"].get(dim, {}), suffix)


async def write(
    pool: asyncpg.Pool,
    *,
    today: date,
    iso_week: str,
    week_start: date,
    status: str,
    coverage: dict,
    fx_rates: dict,
    day: dict,
    prev: dict | None,
    base: dict | None,
    quality: dict,
) -> dict:
    """day: build.compute_day() output. Returns a small summary."""
    roll = day["rollup"]
    cmp: state.Comparison = day["comparison"]
    version = code_version()

    kpi, sec, _ = sections.build_sections(roll, base)
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(hashtext('report_build'))")
        cur = await conn.fetchrow(
            "SELECT report_date, generated_at FROM report_daily.report WHERE id = 1 FOR UPDATE"
        )
        if prev and cur and cur["generated_at"] != prev["generated_at"]:
            raise RuntimeError("report_daily.report changed during the build; re-run")

        last_week = await conn.fetchrow(
            "SELECT * FROM report_weekly.report WHERE iso_week < $1 "
            "ORDER BY week_start DESC LIMIT 1",
            iso_week,
        )
        last_week = dict(last_week) if last_week else None
        prev_week_compact = compact_from_sections(last_week)

        week_avg = dict((prev_week_compact or {}).get("kpi") or {})
        wdays = max(1, week_avg.get("days") or 1)
        for k in ("new_jobs", "removed_jobs"):
            if week_avg.get(k) is not None:
                week_avg[k] = round(week_avg[k] / wdays, 1)
        daily_deltas = {
            "prev_day_date": (base or {}).get("date"),
            "vs_prev_day": sections.kpi_delta(kpi, (base or {}).get("kpi")),
            "prev_week": last_week["iso_week"] if last_week else None,
            "vs_prev_week_avg": sections.kpi_delta(kpi, week_avg or None),
        }
        same_day = bool(prev and prev["report_date"] == today)
        await conn.execute(
            _DAILY_UPSERT,
            today,
            iso_week,
            status,
            version,
            *_headline(kpi),
            _dump(kpi),
            _dump(daily_deltas),
            _dump(coverage),
            _dump(fx_rates),
            *[_dump(sec[c]) for c in SECTION_COLS],
            _dump(quality),
            _dump(roll),
            _dump(base) if base else None,
            state.encode(cmp.new_state),
            ((prev or {}).get("prev_job_state" if same_day else "job_state")),
        )

        # ---- weekly fold -------------------------------------------------
        wk = await conn.fetchrow(
            "SELECT rollup, report_dates, daily_series FROM report_weekly.report "
            "WHERE iso_week = $1 FOR UPDATE",
            iso_week,
        )
        wroll = _j(wk["rollup"]) if wk and wk["rollup"] else {}
        dates = list(wk["report_dates"]) if wk else []
        series = _j(wk["daily_series"]) if wk else []
        if same_day and today in dates and prev and prev["rollup"]:
            rollup_mod.merge(wroll, _j(prev["rollup"]), -1)
        rollup_mod.merge(wroll, roll, +1)
        wroll["v"] = 1
        if today not in dates:
            dates.append(today)
        dates.sort()
        wroll["days"] = len(dates)

        point = {
            "date": today.isoformat(),
            "coverage_pct": coverage["coverage_pct"],
            **{k: kpi.get(k) for k in sections.HEADLINE_KEYS},
        }
        series = [p for p in series if p.get("date") != today.isoformat()] + [point]
        series.sort(key=lambda p: p["date"])
        cov_vals = [p["coverage_pct"] for p in series if p.get("coverage_pct") is not None]

        trend = {}
        for weeks, suffix in ((4, "_4w"), (12, "_12w")):
            t = await conn.fetchrow(
                "SELECT * FROM report_weekly.report WHERE week_start = $1",
                week_start - timedelta(weeks=weeks),
            )
            trend[suffix] = compact_from_sections(dict(t)) if t else None
        wkpi, wsec, _ = sections.build_sections(wroll, prev_week_compact)
        _apply_trends(wsec, trend)
        weekly_deltas = {
            "prev_week": last_week["iso_week"] if last_week else None,
            "vs_prev_week": sections.kpi_delta(wkpi, (prev_week_compact or {}).get("kpi")),
            "vs_4_weeks": sections.kpi_delta(wkpi, (trend["_4w"] or {}).get("kpi")),
            "vs_12_weeks": sections.kpi_delta(wkpi, (trend["_12w"] or {}).get("kpi")),
        }
        wcoverage = {
            "days": [{"date": p["date"], "coverage_pct": p["coverage_pct"]} for p in series],
            "latest": coverage,
        }
        wquality = {
            **quality,
            "days_covered": len(dates),
            "note": "jobs/companies are averages per covered day; new/removed are week totals",
        }
        await conn.execute(
            _WEEKLY_UPSERT,
            iso_week,
            week_start,
            week_start + timedelta(days=6),
            len(dates),
            dates,
            round(sum(cov_vals) / len(cov_vals), 2) if cov_vals else None,
            version,
            wkpi["total_jobs"],
            wkpi["unique_roles"],
            wkpi["companies_hiring"],
            wkpi["new_jobs"],
            wkpi["removed_jobs"],
            wkpi["remote_pct"],
            wkpi["hybrid_pct"],
            wkpi["onsite_pct"],
            wkpi["salary_disclosure_pct"],
            wkpi["salary_avg_usd"],
            wkpi["salary_median_usd"],
            wkpi["salary_p10_usd"],
            wkpi["salary_p25_usd"],
            wkpi["salary_p75_usd"],
            wkpi["salary_p90_usd"],
            wkpi["new_listing_salary_median_usd"],
            wkpi["yoe_median"],
            wkpi["visa_offered_pct"],
            _dump(series),
            _dump(wkpi),
            _dump(weekly_deltas),
            _dump(wcoverage),
            *[_dump(wsec[c]) for c in SECTION_COLS],
            _dump(wquality),
            _dump(wroll),
        )

        # A new week started: freeze every older in-progress week.
        finalized = await conn.fetch(
            "UPDATE report_weekly.report SET status = 'final', rollup = NULL "
            "WHERE iso_week < $1 AND status = 'in_progress' RETURNING iso_week",
            iso_week,
        )

    return {
        "report_date": today.isoformat(),
        "iso_week": iso_week,
        "status": status,
        "same_day_rerun": same_day,
        "week_days_covered": len(dates),
        "weeks_finalized": [r["iso_week"] for r in finalized],
        "kpi": {
            k: kpi.get(k)
            for k in (
                "total_jobs",
                "companies_hiring",
                "new_jobs",
                "removed_jobs",
                "salary_disclosure_pct",
                "salary_median_usd",
            )
        },
    }


def write_local(out: Path, payload: dict) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(_dump(payload), encoding="utf-8")
