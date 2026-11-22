"""`jobs` CLI — Postgres-only, Redis-free crawl pipeline.

    jobs migrate                 apply the compact schema + jobs_analytics() (idempotent)
    jobs purge                   TRUNCATE job_posting + reset crawl_queue (clean re-scrape)
    jobs sync                    data/*.csv -> <schema>.company / job_board
    jobs run [--minutes N]       one bounded crawl pass (default 20): reclaim ->
                 [--shard I/N]   enqueue -> crawl (country gate IN/US, seniority-tag) ->
                 [--no-cleanup]  close-stale -> hard-delete (RETAIN_DAYS / MAX_POSTING_AGE_DAYS) -> prune.
                                 --shard crawls only 1/N of the boards (matrix jobs);
                                 --no-cleanup skips the last three (use `jobs finalize`)
    jobs finalize                close-stale + hard-delete + prune, once, after all shards
    jobs import-boards --from-sqlite PATH [--json DIR] [--statuses ok] [--dry-run]
                                 map another crawler's boards into job_board (origin='autoapply')
    jobs linkedin [--max-pages N] [--limit-queries N] [--minutes M]
                                 LinkedIn guest search (signed-out) -> source='linkedin' rows
    jobs db-size                 pg_database_size + per-table sizes (MAX_DB_MB guard)
    jobs close-stale [--days N]  status='closed' for postings unseen N days (def 1)
    jobs prune [--cap N]         keep newest N postings per company (def 400)
    jobs hard-delete [--days N]  DELETE postings older than N days by first_seen (def 1)
    jobs reclassify [--all] [--enforce] [--dry-run]   role_family backfill + apply pool rules
    jobs snapshot-market         compute crawler.compute_market_snapshot() and
                                  upsert it into public.jobs_market_snapshot_daily
                                  for CURRENT_DATE (run BEFORE hard-delete)
    jobs backfill-posted-at      one-time: parse date_posted -> posted_at for
                                  open rows that predate the column
    jobs stats                   row counts

Clean re-scrape:  jobs migrate && jobs purge && jobs sync && jobs run --minutes 20

Entry point: `jobs = "src.pgpipe.cli:main"` in pyproject.toml.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

from datetime import datetime, timezone

from src.pgpipe import board_import, cleanup, csv_sync, linkedin_source, queue as pg_queue
from src.pgpipe.config import settings
from src.pgpipe.db import apply_schema, connect
from src.pgpipe.posted_at import parse_posted_at
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


async def _cmd_run(minutes: float, shard_raw: str | None, cleanup_after: bool) -> int:
    shard = pg_queue.parse_shard(shard_raw)
    if shard and settings.shard_stagger_seconds > 0:
        await asyncio.sleep(shard[0] * settings.shard_stagger_seconds)
    pool = await connect()
    try:
        summary = await run_pass(
            pool, time_budget_seconds=minutes * 60.0, shard=shard, cleanup_after=cleanup_after
        )
    finally:
        await pool.close()
    _p(summary)
    # fail loudly if the whole pass produced nothing but errors
    if summary["boards_attempted"] and summary["boards_ok"] == 0:
        print("ERROR: every board failed this pass", file=sys.stderr)
        return 1
    return 0


async def _cmd_finalize() -> int:
    """Retention + cap, run once after every crawl shard has finished."""
    pool = await connect()
    try:
        closed = await cleanup.close_stale(pool)
        deleted = await cleanup.hard_delete_stale(pool)
        pruned = await cleanup.prune_per_company(pool)
        reclassified = await cleanup.reclassify(pool)          # legacy rows without role_family
        mix = await cleanup.enforce_mix(pool)                  # role / country drop + 90/10 caps
        purged = await pg_queue.purge_finished(pool)
    finally:
        await pool.close()
    _p({"finalize": "ok", "closed": closed, "deleted_old": deleted, "pruned": pruned,
        "reclassified": reclassified, "mix": mix, "queue_purged": purged})
    return 0


async def _cmd_import_boards(sqlite_path: str | None, json_dir: str | None,
                             statuses: str, dry_run: bool) -> int:
    sources: list[dict] = []
    if sqlite_path:
        sources += board_import.load_sqlite(
            sqlite_path, tuple(x.strip() for x in statuses.split(",") if x.strip())
        )
    if json_dir:
        sources += board_import.load_json_dir(json_dir)
    mapped, skipped = board_import.map_all(sources)
    pool = await connect()
    try:
        await apply_schema(pool)
        res = await board_import.import_boards(pool, mapped, dry_run=dry_run)
    finally:
        await pool.close()
    _p({"source_rows": len(sources), "skipped_in_mapping": dict(skipped), **res})
    return 0


async def _cmd_linkedin(max_pages: int, limit_queries: int | None, minutes: float | None,
                        window_hours: float) -> int:
    queries = linkedin_source.QUERIES[:limit_queries] if limit_queries else None
    locations = ("India",) if limit_queries else None
    pool = await connect()
    try:
        summary = await linkedin_source.run_linkedin(
            pool, queries=queries, locations=locations, max_pages=max_pages, window_hours=window_hours,
            time_budget_s=(minutes * 60.0) if minutes else None,
        )
    finally:
        await pool.close()
    _p(summary)
    return 0


async def _cmd_reclassify(all_rows: bool, enforce: bool, dry_run: bool) -> int:
    """Backfill role_family/role_category (and optionally apply the pool rules
    now: drop other-role / non-IN-US rows, trim product and US to their caps)."""
    pool = await connect()
    try:
        await apply_schema(pool)
        out: dict = {"before": await cleanup.mix_counts(pool)}
        if not dry_run:
            out["reclassified"] = await cleanup.reclassify(pool, only_null=not all_rows)
        if enforce:
            out["enforce"] = await cleanup.enforce_mix(pool, dry_run=dry_run)
        out["after"] = await cleanup.mix_counts(pool)
    finally:
        await pool.close()
    _p(out)
    return 0


async def _cmd_db_size() -> int:
    pool = await connect()
    try:
        schema = settings.validated_schema()
        size = await pool.fetchval("SELECT pg_database_size(current_database())")
        tables = {}
        for t in ("job_posting", "crawl_queue", "job_board", "company"):
            tables[t] = {
                "bytes": await pool.fetchval(f"SELECT pg_total_relation_size('{schema}.{t}')"),
                "rows": await pool.fetchval(f"SELECT count(*) FROM {schema}.{t}"),
            }
    finally:
        await pool.close()
    _p({"db_mb": round(size / 1048576, 1), "max_db_mb": settings.max_db_mb, "tables": tables})
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
    _p({"hard_deleted": n, "days": days or settings.retain_days})
    return 0


_BREAKDOWN_SQL = r"""
INSERT INTO public.jobs_market_snapshot_breakdown_daily
    (snapshot_date, country, role_bucket, seniority, company_type, open_count, new_today)
SELECT CURRENT_DATE,
       coalesce(jp.country, 'other'),
       coalesce(jp.role_category, 'unclassified'),
       coalesce(jp.seniority, 'unknown'),
       coalesce(co.company_type, 'unknown'),
       count(*) FILTER (WHERE jp.status = 'open'),
       count(*) FILTER (WHERE jp.posted_at >= CURRENT_DATE)
FROM {{SCHEMA}}.job_posting jp
LEFT JOIN {{SCHEMA}}.company co ON co.slug = jp.company_slug
WHERE jp.status = 'open'
GROUP BY 1, 2, 3, 4, 5
"""


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
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "DELETE FROM public.jobs_market_snapshot_breakdown_daily "
                "WHERE snapshot_date = CURRENT_DATE"
            )
            await conn.execute(_BREAKDOWN_SQL.replace("{{SCHEMA}}", settings.validated_schema()))
    finally:
        await pool.close()
    _p({"snapshot_market": "ok", "snapshot_date": "CURRENT_DATE", **data})
    return 0


async def _cmd_backfill_posted_at() -> int:
    """One-time fix for rows that predate the posted_at column: parse the raw
    date_posted text (dateutil, same rules as ingest.py) for every open row
    that doesn't have a posted_at yet. Unlike country, this genuinely needs a
    backfill — date_posted text for currently-open postings goes back to
    2019, so 1-day churn alone won't correct it. Falls back to first_seen_at
    for anything unparseable, same as a fresh insert would."""
    pool = await connect()
    updated = fell_back = 0
    try:
        rows = await pool.fetch(
            "SELECT id, date_posted, first_seen_at FROM job_posting "
            "WHERE status = 'open' AND posted_at IS NULL"
        )
        batch = []
        for r in rows:
            parsed = parse_posted_at(r["date_posted"])
            if parsed is None:
                parsed = r["first_seen_at"] or datetime.now(timezone.utc)
                fell_back += 1
            batch.append((parsed, r["id"]))
        if batch:
            async with pool.acquire() as conn:
                await conn.executemany(
                    "UPDATE job_posting SET posted_at = $1 WHERE id = $2", batch
                )
            updated = len(batch)
    finally:
        await pool.close()
    _p({"backfill_posted_at": "ok", "rows_updated": updated, "fell_back_to_first_seen": fell_back})
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
    rp.add_argument("--shard", default=None, help="I/N: crawl only shard I of N (0-based)")
    rp.add_argument("--no-cleanup", action="store_true",
                    help="skip close-stale / hard-delete / prune (use `jobs finalize`)")
    sub.add_parser("finalize")
    ip = sub.add_parser("import-boards")
    ip.add_argument("--from-sqlite", default=None)
    ip.add_argument("--json", default=None, help="directory holding companies/top_companies/watchlist.json")
    ip.add_argument("--statuses", default="ok")
    ip.add_argument("--dry-run", action="store_true")
    lp = sub.add_parser("linkedin")
    lp.add_argument("--max-pages", type=int, default=6)
    lp.add_argument("--limit-queries", type=int, default=None)
    lp.add_argument("--minutes", type=float, default=None)
    lp.add_argument("--window-hours", type=float, default=24.0)
    sub.add_parser("db-size")
    cp = sub.add_parser("close-stale")
    cp.add_argument("--days", type=int, default=None)
    pp = sub.add_parser("prune")
    pp.add_argument("--cap", type=int, default=None)
    hp = sub.add_parser("hard-delete")
    hp.add_argument("--days", type=int, default=None)
    rc_p = sub.add_parser("reclassify")
    rc_p.add_argument("--all", action="store_true", help="re-classify every row, not only NULL role_family")
    rc_p.add_argument("--enforce", action="store_true", help="also drop other-role / non IN-US rows and trim caps")
    rc_p.add_argument("--dry-run", action="store_true", help="with --enforce: count only, change nothing")
    sub.add_parser("snapshot-market")
    sub.add_parser("backfill-posted-at")
    sub.add_parser("stats")

    args = ap.parse_args()
    if args.cmd == "migrate":
        rc = asyncio.run(_cmd_migrate())
    elif args.cmd == "purge":
        rc = asyncio.run(_cmd_purge())
    elif args.cmd == "sync":
        rc = asyncio.run(_cmd_sync())
    elif args.cmd == "run":
        rc = asyncio.run(_cmd_run(args.minutes, args.shard, not args.no_cleanup))
    elif args.cmd == "finalize":
        rc = asyncio.run(_cmd_finalize())
    elif args.cmd == "import-boards":
        rc = asyncio.run(_cmd_import_boards(args.from_sqlite, args.json, args.statuses, args.dry_run))
    elif args.cmd == "linkedin":
        rc = asyncio.run(_cmd_linkedin(args.max_pages, args.limit_queries, args.minutes, args.window_hours))
    elif args.cmd == "db-size":
        rc = asyncio.run(_cmd_db_size())
    elif args.cmd == "close-stale":
        rc = asyncio.run(_cmd_close_stale(args.days))
    elif args.cmd == "prune":
        rc = asyncio.run(_cmd_prune(args.cap))
    elif args.cmd == "hard-delete":
        rc = asyncio.run(_cmd_hard_delete(args.days))
    elif args.cmd == "reclassify":
        rc = asyncio.run(_cmd_reclassify(args.all, args.enforce, args.dry_run))
    elif args.cmd == "snapshot-market":
        rc = asyncio.run(_cmd_snapshot_market())
    elif args.cmd == "backfill-posted-at":
        rc = asyncio.run(_cmd_backfill_posted_at())
    elif args.cmd == "stats":
        rc = asyncio.run(_cmd_stats())
    else:  # pragma: no cover
        ap.error(f"unknown command {args.cmd!r}")
        rc = 2
    sys.exit(rc)


if __name__ == "__main__":
    main()
