"""`jobs` CLI — Postgres-only, Redis-free crawl pipeline.

    jobs migrate                 apply the compact schema (idempotent)
    jobs sync                    data/*.csv -> <schema>.company / job_board
    jobs run [--minutes N]       one bounded crawl pass (default 20)
    jobs close-stale [--days N]  status='closed' for postings unseen N days (def 3)
    jobs prune [--cap N]         keep newest N postings per company (def 400)
    jobs hard-delete [--days N]  DELETE postings unseen N days (def 5)
    jobs stats                   row counts

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
    _p({"hard_deleted": n, "days": days or settings.delete_after_days})
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
    sub.add_parser("sync")
    rp = sub.add_parser("run")
    rp.add_argument("--minutes", type=float, default=20.0)
    cp = sub.add_parser("close-stale")
    cp.add_argument("--days", type=int, default=None)
    pp = sub.add_parser("prune")
    pp.add_argument("--cap", type=int, default=None)
    hp = sub.add_parser("hard-delete")
    hp.add_argument("--days", type=int, default=None)
    sub.add_parser("stats")

    args = ap.parse_args()
    if args.cmd == "migrate":
        rc = asyncio.run(_cmd_migrate())
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
    elif args.cmd == "stats":
        rc = asyncio.run(_cmd_stats())
    else:  # pragma: no cover
        ap.error(f"unknown command {args.cmd!r}")
        rc = 2
    sys.exit(rc)


if __name__ == "__main__":
    main()
