"""Report runtime config — plain env vars, same style as src/pgpipe/config.py."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from src.pgpipe.config import CRAWLER_ROOT

REPORT_DATA_DIR = CRAWLER_ROOT / "data" / "report"
SCHEMA_SQL = Path(__file__).resolve().parent / "schema.sql"


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


@dataclass(frozen=True)
class ReportSettings:
    # Boards fetched in parallel per shard (per-host politeness still applies
    # via src.pgpipe.throttle, which reads PGPIPE_CONCURRENCY / PGPIPE_DELAY_*).
    concurrency: int = _int("PGPIPE_CONCURRENCY", 12)
    # Descriptions are fetched, so big boards take longer than in `jobs run`.
    board_timeout_seconds: float = _float("REPORT_BOARD_TIMEOUT", 180.0)
    board_attempts: int = _int("REPORT_BOARD_ATTEMPTS", 2)
    # A day is "partial" when fewer boards than this succeed.
    partial_below_pct: float = _float("REPORT_PARTIAL_BELOW_PCT", 90.0)
    # Optional cap for test runs (0 = all boards in the shard).
    board_limit: int = _int("REPORT_BOARD_LIMIT", 0)


report_settings = ReportSettings()
