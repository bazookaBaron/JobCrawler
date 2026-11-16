"""pgpipe runtime config — plain env vars, no pydantic, no Redis.

Loads crawler/.env.local then crawler/.env (same files the old CLI used),
without importing src.config (which still carries jobseek's full settings
surface). Everything here is overridable from the environment / CI secrets.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# crawler/  (this file is crawler/src/pgpipe/config.py)
CRAWLER_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = CRAWLER_ROOT / "data"
SCHEMA_SQL = Path(__file__).resolve().parent / "schema.sql"

load_dotenv(CRAWLER_ROOT / ".env.local")
load_dotenv(CRAWLER_ROOT / ".env")

_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default


def _csv_lower(name: str, default: str) -> tuple[str, ...]:
    raw = os.environ.get(name, "").strip() or default
    return tuple(part.strip().lower() for part in raw.split(",") if part.strip())


@dataclass(frozen=True)
class Settings:
    # Supabase Postgres — the ONLY external dependency. No REDIS_URL anymore.
    # LOCAL_DATABASE_URL = admin/owner URI (needed by `jobs migrate`).
    # CRAWLER_DATABASE_URL = least-privilege `crawler_writer` login used by the
    # scheduled workflows; preferred for everything except `migrate`.
    database_url: str = os.environ.get("LOCAL_DATABASE_URL", "").strip()
    writer_database_url: str = os.environ.get("CRAWLER_DATABASE_URL", "").strip()

    # Dedicated Postgres schema so nothing collides with the shared webapp DB.
    schema: str = (os.environ.get("PGPIPE_SCHEMA", "crawler").strip() or "crawler")

    # Concurrency: how many boards are fetched in parallel (per host still
    # serialised by the politeness gate below). Single process, cron-friendly.
    concurrency: int = _int("PGPIPE_CONCURRENCY", 12)

    # Politeness — min seconds between two requests to the SAME host.
    # ATS API hosts (boards-api.greenhouse.io, api.ashbyhq.com, ...) are shared
    # by many boards, so this throttles the whole provider, not just one board.
    delay_ats_seconds: float = _float("PGPIPE_DELAY_ATS", 0.6)
    delay_other_seconds: float = _float("PGPIPE_DELAY_OTHER", 2.0)
    delay_jitter_seconds: float = _float("PGPIPE_DELAY_JITTER", 0.25)

    # Per-board fetch timeout and attempt cap.
    board_timeout_seconds: float = _float("PGPIPE_BOARD_TIMEOUT", 90.0)
    max_attempts: int = _int("PGPIPE_MAX_ATTEMPTS", 3)

    # Reset 'claimed' queue rows older than this at the start of each run.
    reclaim_after_minutes: int = _int("PGPIPE_RECLAIM_AFTER_MIN", 30)

    # Crawl-budget weighting. Boards whose monitor_type is in
    # `priority_monitor_types` (the big ATS providers) are claimed first for
    # `priority_weight` of the claims in each pass; everything else shares the
    # rest. Both groups keep draining — if one empties, the other takes 100%
    # of the remaining budget, so nothing starves. Set weight to 0 or 1 (or
    # clear the list) to fall back to plain FIFO.
    priority_monitor_types: tuple[str, ...] = _csv_lower(
        "PGPIPE_PRIORITY_MONITORS", "greenhouse,workday"
    )
    priority_weight: float = _float("PGPIPE_PRIORITY_WEIGHT", 0.7)

    # Retention (see src/pgpipe/cleanup.py). job_posting only keeps the last
    # `keep_days` (2) days of rows, open OR closed (by eff_at); expired rows are
    # deleted AFTER crawler.seen_posting remembers them, and are not re-inserted.
    # A job is closed when its board was crawled successfully this run and it was
    # missing, or when unseen for `close_after_hours` (boards crawl every 12h ->
    # 36) / `close_after_hours_linkedin` (LinkedIn every 2h -> 30). Closed rows
    # are deleted after `max_age_days` or the keep window, whichever is sooner.
    # `max_rows` is a hard safety cap on job_posting (oldest eff_at deleted);
    # seen_posting entries unseen for `seen_ttl_days` are pruned.
    keep_days: int = _int("PGPIPE_KEEP_DAYS", 2)
    max_rows: int = _int("PGPIPE_MAX_ROWS", 60000)
    seen_ttl_days: int = _int("PGPIPE_SEEN_TTL_DAYS", 45)
    per_company_cap: int = _int("PGPIPE_PER_COMPANY_CAP", 400)
    close_after_hours: int = (
        _int("PGPIPE_CLOSE_AFTER_DAYS", 0) * 24 or _int("PGPIPE_CLOSE_AFTER_HOURS", 36)
    )
    close_after_hours_linkedin: int = _int("PGPIPE_CLOSE_AFTER_HOURS_LINKEDIN", 30)
    max_age_days: int = _int("PGPIPE_MAX_AGE_DAYS", 7)  # days a CLOSED row is kept (max)

    worker_id: str = os.environ.get("PGPIPE_WORKER_ID", "").strip() or f"gha-{os.getpid()}"

    def validated_schema(self) -> str:
        if not _IDENT_RE.match(self.schema):
            raise ValueError(
                f"PGPIPE_SCHEMA {self.schema!r} must be a simple lowercase identifier"
            )
        return self.schema

    def require_db(self, admin: bool = False) -> str:
        """DSN to use. `admin=True` (jobs migrate) prefers LOCAL_DATABASE_URL;
        otherwise the least-privilege CRAWLER_DATABASE_URL wins when set."""
        order = (
            (self.database_url, self.writer_database_url)
            if admin
            else (self.writer_database_url, self.database_url)
        )
        for dsn in order:
            if dsn:
                return dsn
        raise SystemExit(
            "CRAWLER_DATABASE_URL / LOCAL_DATABASE_URL is not set. Put the Supabase "
            "URI in crawler/.env.local (see .env.local.example) or the CI secret."
        )


settings = Settings()
