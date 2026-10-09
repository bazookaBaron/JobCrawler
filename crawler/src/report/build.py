"""`report build` — shards -> today's report -> report_daily + report_weekly.

load shard parquets + board statuses
dedupe jobs (same company + provider id across boards)
enrich: company meta, FX -> USD, bands, salary premium vs profile median
compare with yesterday's job_state -> exact new / removed
rollup -> sections -> store (one transaction)
"""

from __future__ import annotations

import csv
import functools
import json
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import structlog

from src.pgpipe.config import DATA_DIR
from src.report import rollup, salary, sections, state
from src.report.config import REPORT_DATA_DIR, report_settings
from src.report.crawl import SCHEMA, load_boards
from src.report.derive import _EPOCH

log = structlog.get_logger()

_SIZE_LABELS = {
    "1": "1-10",
    "2": "11-50",
    "3": "51-200",
    "4": "201-500",
    "5": "501-1k",
    "6": "1k-5k",
    "7": "5k-10k",
    "8": "10k+",
}


@functools.cache
def _company_meta() -> pl.DataFrame:
    industries: dict[str, str] = {}
    with open(DATA_DIR / "industries.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            industries[row["id"]] = row["name"]
    rows = []
    this_year = date.today().year
    with open(REPORT_DATA_DIR / "companies.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            fy = (row.get("founded_year") or "").strip()
            age = "unknown"
            if fy.isdigit():
                a = this_year - int(fy)
                age = (
                    "<5y"
                    if a < 5
                    else "5-10y"
                    if a < 10
                    else "10-20y"
                    if a < 20
                    else "20-50y"
                    if a < 50
                    else "50y+"
                )
            rows.append(
                {
                    "company_slug": row["slug"],
                    "industry": industries.get((row.get("industry") or "").strip(), "Unknown"),
                    "company_size": _SIZE_LABELS.get(
                        (row.get("employee_count_range") or "").strip(), "unknown"
                    ),
                    "company_age": age,
                }
            )
    return pl.DataFrame(rows)


def load_shards(in_dir: Path) -> tuple[pl.DataFrame, list[dict], set[int]]:
    """-> (jobs, board statuses, shard indexes received)."""
    frames, statuses, got = [], [], set()
    for p in sorted(in_dir.rglob("jobs-*.parquet")):
        frames.append(pl.read_parquet(p))
    for p in sorted(in_dir.rglob("boards-*.json")):
        data = json.loads(p.read_text(encoding="utf-8"))
        statuses.extend(data.get("boards", []))
        got.add(int(data["shard"]))
    df = pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame(schema=SCHEMA)
    return df, statuses, got


def coverage(statuses: list[dict], shards: int, got: set[int], raw: int, unique: int) -> dict:
    all_boards = load_boards()
    by_slug = {s["board_slug"]: s for s in statuses}
    missing_shards = sorted(set(range(shards)) - got)
    failed, by_monitor = [], {}
    ok = gone = 0
    for b in all_boards:
        s = by_slug.get(b["board_slug"])
        st = s["status"] if s else "not_crawled"
        m = by_monitor.setdefault(b["monitor_type"], {"ok": 0, "failed": 0})
        if st == "ok":
            ok += 1
            m["ok"] += 1
        elif st == "gone":
            gone += 1
            m["failed"] += 1
        else:
            m["failed"] += 1
            failed.append(
                {"board_slug": b["board_slug"], "status": st, "error": (s or {}).get("error")}
            )
    total = len(all_boards)
    ok_companies = {s["company_slug"] for s in statuses if s["status"] == "ok"}
    return {
        "boards_total": total,
        "boards_ok": ok,
        "boards_failed": total - ok - gone,
        "boards_gone": gone,
        "coverage_pct": sections.pct(ok, total - gone) or 0.0,
        "companies_total": len({b["company_slug"] for b in all_boards}),
        "companies_ok": len(ok_companies),
        "shards_expected": shards,
        "shards_received": sorted(got),
        "shards_missing": missing_shards,
        "postings_raw": raw,
        "postings_unique": unique,
        "by_monitor": by_monitor,
        "failed_boards_sample": failed[:100],
    }


def failed_board_hashes(statuses: list[dict]) -> set[int]:
    ok = {s["board_slug"] for s in statuses if s["status"] == "ok"}
    return {state.board_hash(b["board_slug"]) for b in load_boards() if b["board_slug"] not in ok}


def enrich(df: pl.DataFrame, usd_per: dict[str, float], today: int) -> pl.DataFrame:
    rates = pl.DataFrame({"sal_currency": list(usd_per), "_rate": list(usd_per.values())})
    df = df.join(_company_meta(), on="company_slug", how="left").join(
        rates, on="sal_currency", how="left"
    )
    mid = (pl.col("sal_min") + pl.col("sal_max")) / 2
    ok_range = (pl.col("sal_min") > 0) & (
        pl.col("sal_max") / pl.col("sal_min") <= salary.MAX_RANGE_RATIO
    )
    usd = mid * pl.col("_rate")
    df = df.with_columns(
        pl.when(ok_range & usd.is_between(salary.MIN_ANNUAL_USD, salary.MAX_ANNUAL_USD))
        .then(usd)
        .otherwise(None)
        .alias("sal_usd"),
    ).with_columns(
        pl.when(pl.col("sal_usd").is_not_null()).then(mid).otherwise(None).alias("sal_local"),
    )
    # salary premium: this job's pay relative to its profile's median today
    med = (
        df.filter(pl.col("sal_usd").is_not_null())
        .group_by("profile")
        .agg(pl.col("sal_usd").median().alias("_pmed"), pl.len().alias("_pn"))
        .filter(pl.col("_pn") >= 5)
    )
    df = df.join(med.select("profile", "_pmed"), on="profile", how="left")
    y = pl.col("yoe_min")
    age = today - pl.col("posted_day")
    usd_c = pl.col("sal_usd")
    return df.with_columns(
        pl.lit("all").alias("_all"),
        (usd_c / pl.col("_pmed")).alias("rel"),
        pl.when(y.is_null())
        .then(pl.lit("unspecified"))
        .when(y < 1)
        .then(pl.lit("0-1"))
        .when(y < 3)
        .then(pl.lit("1-3"))
        .when(y < 5)
        .then(pl.lit("3-5"))
        .when(y < 8)
        .then(pl.lit("5-8"))
        .when(y < 12)
        .then(pl.lit("8-12"))
        .otherwise(pl.lit("12+"))
        .alias("yoe_band"),
        pl.when(y.is_null())
        .then(None)
        .when(y >= 20)
        .then(pl.lit("20+"))
        .otherwise(y.floor().cast(pl.Int32).cast(pl.Utf8))
        .alias("yoe_years"),
        pl.when(pl.col("posted_day").is_null())
        .then(pl.lit("unknown"))
        .when(age <= 1)
        .then(pl.lit("0-1d"))
        .when(age <= 7)
        .then(pl.lit("2-7d"))
        .when(age <= 14)
        .then(pl.lit("8-14d"))
        .when(age <= 30)
        .then(pl.lit("15-30d"))
        .when(age <= 90)
        .then(pl.lit("31-90d"))
        .otherwise(pl.lit("90d+"))
        .alias("posting_age"),
        pl.when(usd_c.is_null())
        .then(pl.lit("undisclosed"))
        .when(usd_c < 25_000)
        .then(pl.lit("<25k"))
        .when(usd_c < 50_000)
        .then(pl.lit("25-50k"))
        .when(usd_c < 75_000)
        .then(pl.lit("50-75k"))
        .when(usd_c < 100_000)
        .then(pl.lit("75-100k"))
        .when(usd_c < 150_000)
        .then(pl.lit("100-150k"))
        .when(usd_c < 200_000)
        .then(pl.lit("150-200k"))
        .when(usd_c < 300_000)
        .then(pl.lit("200-300k"))
        .otherwise(pl.lit("300k+"))
        .alias("salary_band"),
        pl.when(pl.col("equity"))
        .then(pl.lit("mentioned"))
        .otherwise(pl.lit("not_mentioned"))
        .alias("equity_flag"),
        pl.when(pl.col("clearance"))
        .then(pl.lit("required"))
        .otherwise(pl.lit("not_mentioned"))
        .alias("clearance_flag"),
        pl.when(pl.col("n_countries") > 1)
        .then(pl.lit("multi_country"))
        .when(pl.col("n_locations") > 1)
        .then(pl.lit("multi_city"))
        .when(pl.col("n_locations") == 1)
        .then(pl.lit("single"))
        .otherwise(pl.lit("none_listed"))
        .alias("multi_location"),
        pl.when(pl.col("city").is_not_null())
        .then(pl.col("city") + pl.lit(", ") + pl.col("country").fill_null("?"))
        .otherwise(None)
        .alias("city_key"),
    )


def iso_week(d: date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def week_bounds(d: date) -> tuple[date, date]:
    start = d - timedelta(days=d.weekday())
    return start, start + timedelta(days=6)


def compute_day(
    df: pl.DataFrame,
    statuses: list[dict],
    usd_per: dict[str, float],
    prev_state: state.JobState | None,
    baseline: dict | None,
    today: date,
) -> dict:
    """Everything for today's daily row except DB I/O."""
    today_n = (today - _EPOCH).days
    raw = df.height
    df = df.unique(subset="job_key", keep="first", maintain_order=True)
    failed = failed_board_hashes(statuses)
    cmp = state.compare(
        prev_state,
        df.get_column("job_key").to_list(),
        [state.board_hash(b) for b in df.get_column("board_slug").to_list()],
        df.get_column("posted_day").to_list(),
        failed,
        today_n,
    )
    df = df.with_columns(pl.Series("is_new", cmp.is_new, dtype=pl.Boolean))
    df = enrich(df, usd_per, today_n)

    extra = {
        "roles": df.select(pl.struct("company_slug", "title_clean").n_unique()).item(),
        "companies": df.get_column("company_slug").n_unique(),
        "countries": df.get_column("country").drop_nulls().n_unique(),
        "removed": cmp.removed or 0,
        "carried": cmp.carried,
        "baseline_days": 1 if cmp.removed is None else 0,
    }
    roll = rollup.build(df, extra, cmp.removed_days)
    # removed per bucket = yesterday's jobs + today's new - today's jobs
    if baseline and cmp.removed is not None:
        for dim, buckets in roll["dims"].items():
            prev_dim = (baseline.get("dims") or {}).get(dim, {})
            for b, s in buckets.items():
                pj = (prev_dim.get(b) or {}).get("jobs", 0)
                rm = int(round(pj + s.get("n", 0) - s.get("j", 0)))
                if rm > 0:
                    s["rm"] = rm
    return {
        "df_unique": df.height,
        "postings_raw": raw,
        "rollup": roll,
        "comparison": cmp,
        "quality": _quality(df, usd_per),
    }


def _quality(df: pl.DataFrame, usd_per: dict[str, float]) -> dict:
    n = max(1, df.height)
    cur = df.get_column("sal_currency").drop_nulls().unique().to_list()
    return {
        "jobs": df.height,
        "thin_salary_threshold": sections.THIN_SALARY_N,
        "salary_sources": dict(df.group_by("sal_source").len().iter_rows()),
        "salary_parsed_pct": sections.pct(df.get_column("sal_min").is_not_null().sum(), n),
        "salary_usable_pct": sections.pct(df.get_column("sal_usd").is_not_null().sum(), n),
        "currencies_without_fx": sorted(c for c in cur if c not in usd_per),
        "country_unresolved_pct": sections.pct(df.get_column("country").is_null().sum(), n),
        "profile_other_pct": sections.pct((df.get_column("profile") == "other").sum(), n),
        "seniority_unspecified_pct": sections.pct(
            (df.get_column("seniority") == "unspecified").sum(), n
        ),
        "description_missing_pct": sections.pct((df.get_column("desc_len") == 0).sum(), n),
        "percentile_method": "log-binned histograms (1% bins; 4% in two-way tables)",
    }


def status_for(cov: dict) -> str:
    return "ok" if cov["coverage_pct"] >= report_settings.partial_below_pct else "partial"
