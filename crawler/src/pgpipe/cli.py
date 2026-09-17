"""`jobs` CLI — Postgres-only, Redis-free crawl pipeline.

    jobs migrate                 apply the compact schema + jobs_analytics() (idempotent)
    jobs purge                   TRUNCATE job_posting + reset crawl_queue (clean re-scrape)
    jobs sync                    data/*.csv -> <schema>.company / job_board
    jobs run [--minutes N]       one bounded crawl pass (default 20): reclaim ->
                                 enqueue -> crawl (tech-filter + seniority-tag) ->
                                 close-stale -> hard-delete (1-day age cap) -> prune
    jobs close-stale [--days N]  status='closed' for postings unseen N days (def 1)
    jobs prune [--cap N]         keep newest N postings per company (def 400)
    jobs hard-delete [--days N]  DELETE postings older than N days by first_seen (def 1)
    jobs snapshot-market         compute crawler.compute_market_snapshot() and
                                  upsert it into public.jobs_market_snapshot_daily
                                  for CURRENT_DATE (run BEFORE hard-delete)
    jobs stats                   row counts

Clean re-scrape:  jobs migrate && jobs purge && jobs sync && jobs run --minutes 20

Entry point: `jobs = "src.pgpipe.cli:main"` in pyproject.toml.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

from src.pgpipe import cleanup, csv_sync
from src.pgpipe.config import settings
from src.pgpipe.db import apply_schema, connect
from src.pgpipe.runner import run as run_pass


def _p(obj: object) -> None:
    print(json.dumps(obj, indent=2, default=str))


async def _cmd_migrate() -> int:
    pool = await connect()
    try:
        await apply_schema(pool)
        _p({"migrate": "ok", "schema": settings.validated_schema()})
    finally:
        await pool.close()
    return 0


async def _cmd_purge() -> int:
    """Wipe all postings + queue for a clean re-scrape. Boards/companies stay."""
    schema = settings.validated_schema()
    pool = await connect()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                f'TRUNCATE {schema}.job_posting RESTART IDENTITY;'
                f'TRUNCATE {schema}.crawl_queue RESTART IDENTITY;'
            )
        _p({"purge": "ok", "truncated": ["job_posting", "crawl_queue"]})
    finally:
        await pool.close()
    return 0


async def _cmd_sync() -> int:
    pool = await connect()
    try:
        await apply_schema(pool)  # safe: idempotent, keeps `sync` usable standalone
        res = await csv_sync.sync(pool)
        _p({"sync": res})
    finally:
        await pool.close()
    return 0


async def _cmd_run(minutes: float) -> int:
    pool = await connect()
    try:
        summary = await run_pass(pool, time_budget_seconds=minutes * 60.0)
    finally:
        await pool.close()
    _p(summary)
    # fail loudly if the whole pass produced nothing but errors
    if summary["boards_attempted"] and summary["boards_ok"] == 0:
        print("ERROR: every board failed this pass", file=sys.stderr)
        return 1
    return 0


async def _cmd_close_stale(days: int | None) -> int:
    pool = await connect()
    try:
        n = await cleanup.close_stale(pool, days)
    finally:
        await pool.close()
    _p({"closed": n, "days": days or settings.close_after_days})
    return 0


async def _cmd_prune(cap: int | None) -> int:
    pool = await connect()
    try:
        n = await cleanup.prune_per_company(pool, cap)
    finally:
        await pool.close()
    _p({"pruned": n, "cap": cap or settings.per_company_cap})
    return 0


async def _cmd_hard_delete(days: int | None) -> int:
    pool = await connect()
    try:
        n = await cleanup.hard_delete_stale(pool, days)
    finally:
        await pool.close()
    _p({"hard_deleted": n, "days": days or settings.max_age_days})
    return 0


async def _cmd_snapshot_market() -> int:
    """Run crawler.compute_market_snapshot() and upsert the result into
    public.jobs_market_snapshot_daily for CURRENT_DATE. Meant to run BEFORE
    hard-delete in the daily cleanup workflow: at 1-day retention this table
    is the only surviving record of a day's activity once the source rows
    churn out."""
    pool = await connect()
    try:
        raw = await pool.fetchval("SELECT compute_market_snapshot()")
        data = raw if isinstance(raw, dict) else json.loads(raw)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO public.jobs_market_snapshot_daily
                    (snapshot_date, total_open, new_postings, closed_postings,
                     by_seniority, by_work_type, by_country, by_company_type,
                     top_companies)
                VALUES (CURRENT_DATE, $1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb,
                        $7::jsonb, $8::jsonb)
                ON CONFLICT (snapshot_date) DO UPDATE SET
                    total_open      = EXCLUDED.total_open,
                    new_postings    = EXCLUDED.new_postings,
                    closed_postings = EXCLUDED.closed_postings,
                    by_seniority    = EXCLUDED.by_seniority,
                    by_work_type    = EXCLUDED.by_work_type,
                    by_country      = EXCLUDED.by_country,
                    by_company_type = EXCLUDED.by_company_type,
                    top_companies   = EXCLUDED.top_companies
                """,
                data["total_open"],
                data["new_postings"],
                data["closed_postings"],
                json.dumps(data["by_seniority"]),
                json.dumps(data["by_work_type"]),
                json.dumps(data["by_country"]),
                json.dumps(data["by_company_type"]),
                json.dumps(data["top_companies"]),
            )
    finally:
        await pool.close()
    _p({"snapshot_market": "ok", "snapshot_date": "CURRENT_DATE", **data})
    return 0


async def _cmd_stats() -> int:
    pool = await connect()
    try:
        rows = {
            "schema": settings.validated_schema(),
            "companies": await pool.fetchval("SELECT count(*) FROM company"),
            "boards_enabled": await pool.fetchval(
                "SELECT count(*) FROM job_board WHERE is_enabled"),
            "boards_disabled": await pool.fetchval(
                "SELECT count(*) FROM job_board WHERE NOT is_enabled"),
            "queue_pending": await pool.fetchval(
                "SELECT count(*) FROM crawl_queue WHERE status='pending'"),
            "queue_error": await pool.fetchval(
                "SELECT count(*) FROM crawl_queue WHERE status='error'"),
            "postings_open": await pool.fetchval(
                "SELECT count(*) FROM job_posting WHERE status='open'"),
            "postings_closed": await pool.fetchval(
                "SELECT count(*) FROM job_posting WHERE status='closed'"),
            "postings_total": await pool.fetchval("SELECT count(*) FROM job_posting"),
            "companies_with_jobs": await pool.fetchval(
                "SELECT count(DISTINCT company_slug) FROM job_posting"),
        }
    finally:
        await pool.close()
    _p(rows)
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(prog="jobs", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate")
    sub.add_parser("purge")
    sub.add_parser("sync")
    rp = sub.add_parser("run")
    rp.add_argument("--minutes", type=float, default=20.0)
    cp = sub.add_parser("close-stale")
    cp.add_argument("--days", type=int, default=None)
    pp = sub.add_parser("prune")
    pp.add_argument("--cap", type=int, default=None)
    hp = sub.add_parser("hard-delete")
    hp.add_argument("--days", type=int, default=None)
    sub.add_parser("snapshot-market")
    sub.add_parser("stats")

    args = ap.parse_args()
    if args.cmd == "migrate":
        rc = asyncio.run(_cmd_migrate())
    elif args.cmd == "purge":
        rc = asyncio.run(_cmd_purge())
    elif args.cmd == "sync":
        rc = asyncio.run(_cmd_sync())
    elif args.cmd == "run":
        rc = asyncio.run(_cmd_run(args.minutes))
    elif args.cmd == "close-stale":
        rc = asyncio.run(_cmd_close_stale(args.days))
    elif args.cmd == "prune":
        rc = asyncio.run(_cmd_prune(args.cap))
    elif args.cmd == "hard-delete":
        rc = asyncio.run(_cmd_hard_delete(args.days))
    elif args.cmd == "snapshot-market":
        rc = asyncio.run(_cmd_snapshot_market())
    elif args.cmd == "stats":
        rc = asyncio.run(_cmd_stats())
    else:  # pragma: no cover
        ap.error(f"unknown command {args.cmd!r}")
        rc = 2
    sys.exit(rc)


if __name__ == "__main__":
    main()
