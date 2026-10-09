"""`python -m src.report.cli` — the market-report job.

crawl --shard I --shards N --out DIR   crawl one shard -> DIR/jobs-I.parquet + boards-I.json
build --in DIR --shards N              merge shards, write report_daily (1 row)
                                       and fold into report_weekly (1 row/week)
      [--dry-run --out FILE]           compute only, no DB: dump the report JSON
migrate                                apply src/report/schema.sql (idempotent)
show                                   print today's headline + recent weeks
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from src.pgpipe.db import connect
from src.pgpipe.http import make_client
from src.report import build, crawl, salary, sections, state, store


def _p(obj: object) -> None:
    print(json.dumps(obj, indent=2, default=str))


async def _cmd_crawl(shard: int, shards: int, out: Path) -> int:
    _p(await crawl.crawl_shard(shard, shards, out))
    return 0


async def _fx(prev: dict | None) -> tuple[dict, str]:
    http = make_client()
    try:
        rates = await salary.fetch_usd_rates(http)
    finally:
        await http.aclose()
    if rates:
        return rates, "ecb+open.er-api.com"
    prev_fx = store._j(prev["fx_rates"]) if prev else None
    if prev and prev_fx and prev_fx.get("usd_per"):
        return prev_fx["usd_per"], f"fallback: rates from {prev['report_date']}"
    raise SystemExit(
        "No FX rates available (ECB and open.er-api.com both failed, no previous rates)"
    )


async def _cmd_build(in_dir: Path, shards: int, day: date, dry_run: bool, out: Path | None) -> int:
    df, statuses, got = build.load_shards(in_dir)
    if df.height == 0:
        raise SystemExit(f"No jobs found under {in_dir}; refusing to overwrite the report")
    iso_week = build.iso_week(day)
    week_start, _ = build.week_bounds(day)

    pool = None if dry_run else await connect()  # type: ignore[assignment]
    try:
        if pool:
            await store.apply_schema(pool)
        prev = await store.read_previous_daily(pool) if pool else None
        usd_per, fx_source = await _fx(prev)
        prev_state, base, _ = store.comparison_base(prev, day)
        result = build.compute_day(df, statuses, usd_per, prev_state, base, day)
        cov = build.coverage(statuses, shards, got, result["postings_raw"], result["df_unique"])
        status = build.status_for(cov)
        fx = {"source": fx_source, "as_of": day.isoformat(), "usd_per": usd_per}
        quality = {
            **result["quality"],
            "baseline_run": prev_state is None,
            "jobs_carried_from_failed_boards": result["comparison"].carried,
        }

        if dry_run:
            kpi, sec, _ = sections.build_sections(result["rollup"], base)
            payload = {
                "report_date": day.isoformat(),
                "iso_week": iso_week,
                "status": status,
                "kpi": kpi,
                "coverage": cov,
                "data_quality": quality,
                **sec,
            }
            if out:
                store.write_local(out, payload)
            _p(
                {
                    "dry_run": True,
                    "status": status,
                    "kpi": kpi,
                    "coverage_pct": cov["coverage_pct"],
                    "rollup_bytes": len(store._dump(result["rollup"])),
                    "sections_bytes": len(store._dump(sec)),
                    "job_state_bytes": len(state.encode(result["comparison"].new_state)),
                }
            )
            return 0

        assert pool is not None
        summary = await store.write(
            pool,
            today=day,
            iso_week=iso_week,
            week_start=week_start,
            status=status,
            coverage=cov,
            fx_rates=fx,
            day=result,
            prev=prev,
            base=base,
            quality=quality,
        )
        _p(summary)
    finally:
        if pool:
            await pool.close()
    return 0


async def _cmd_migrate() -> int:
    pool = await connect()
    try:
        await store.apply_schema(pool)
    finally:
        await pool.close()
    _p({"migrate": "ok", "schemas": ["report_daily", "report_weekly"]})
    return 0


async def _cmd_show() -> int:
    pool = await connect()
    try:
        d = await pool.fetchrow(
            "SELECT report_date, status, kpi, kpi_deltas, "
            "coverage->>'coverage_pct' AS coverage_pct FROM report_daily.report WHERE id = 1"
        )
        w = await pool.fetch(
            "SELECT iso_week, status, days_covered, total_jobs_avg, new_jobs_sum, "
            "removed_jobs_sum, salary_median_usd "
            "FROM report_weekly.report ORDER BY week_start DESC LIMIT 8"
        )
    finally:
        await pool.close()
    _p({"daily": dict(d) if d else None, "weekly": [dict(r) for r in w]})
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        prog="report", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("crawl")
    c.add_argument("--shard", type=int, required=True)
    c.add_argument("--shards", type=int, required=True)
    c.add_argument("--out", type=Path, required=True)
    b = sub.add_parser("build")
    b.add_argument("--in", dest="in_dir", type=Path, required=True)
    b.add_argument("--shards", type=int, required=True)
    b.add_argument(
        "--date", type=date.fromisoformat, default=None, help="report date (UTC). Default: today"
    )
    b.add_argument("--dry-run", action="store_true")
    b.add_argument("--out", type=Path, default=None)
    sub.add_parser("migrate")
    sub.add_parser("show")
    args = ap.parse_args()

    if args.cmd == "crawl":
        rc = asyncio.run(_cmd_crawl(args.shard, args.shards, args.out))
    elif args.cmd == "build":
        day = args.date or datetime.now(UTC).date()
        rc = asyncio.run(_cmd_build(args.in_dir, args.shards, day, args.dry_run, args.out))
    elif args.cmd == "migrate":
        rc = asyncio.run(_cmd_migrate())
    else:
        rc = asyncio.run(_cmd_show())
    sys.exit(rc)


if __name__ == "__main__":
    main()
