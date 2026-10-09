"""`report crawl` — fetch one shard of the report boards, derive per-job
attributes, write them to a local parquet (+ a board-status JSON).

Shards are disjoint by crc32(board_slug) % shards, so N GitHub Actions jobs
can crawl in parallel from different runners. Nothing touches Postgres here.
"""

from __future__ import annotations

import asyncio
import csv
import json
import time
import zlib
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import structlog

from src.core.monitors import BoardGoneError
from src.pgpipe.http import make_client
from src.pgpipe.ingest import _discover, _rows_from_result
from src.pgpipe.throttle import Politeness
from src.report import derive
from src.report.config import REPORT_DATA_DIR, report_settings

log = structlog.get_logger()

SCHEMA = {
    "job_key": pl.Int64,
    "company_slug": pl.Utf8,
    "board_slug": pl.Utf8,
    "source": pl.Utf8,
    "title_clean": pl.Utf8,
    "profile": pl.Utf8,
    "job_family": pl.Utf8,
    "seniority": pl.Utf8,
    "employment_type": pl.Utf8,
    "work_mode": pl.Utf8,
    "country": pl.Utf8,
    "region": pl.Utf8,
    "city": pl.Utf8,
    "n_locations": pl.Int32,
    "n_countries": pl.Int32,
    "posted_day": pl.Int32,
    "sal_min": pl.Float64,
    "sal_max": pl.Float64,
    "sal_currency": pl.Utf8,
    "sal_unit": pl.Utf8,
    "sal_source": pl.Utf8,
    "yoe_min": pl.Float64,
    "technologies": pl.List(pl.Utf8),
    "education": pl.Utf8,
    "visa": pl.Utf8,
    "equity": pl.Boolean,
    "clearance": pl.Boolean,
    "language": pl.Utf8,
    "desc_len": pl.Int32,
}


def board_shard(board_slug: str, shards: int) -> int:
    return zlib.crc32(board_slug.encode()) % max(1, shards)


def load_boards() -> list[dict]:
    with open(REPORT_DATA_DIR / "boards.csv", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        try:
            r["monitor_config"] = json.loads(r.get("monitor_config") or "{}")
        except ValueError:
            r["monitor_config"] = {}
    return rows


async def _crawl_board(board: dict, http, pol: Politeness) -> list[dict]:
    board_dict = {
        "board_url": board["board_url"],
        "metadata": board["monitor_config"] or {},
        "board_slug": board["board_slug"],
        "company_slug": board["company_slug"],
    }
    async with pol.slot(board["board_url"]):
        result = await asyncio.wait_for(
            _discover(board["monitor_type"], board_dict, http),
            timeout=report_settings.board_timeout_seconds,
        )
    out: list[dict] = []
    for job in _rows_from_result(result):
        try:
            r = derive.row(board["company_slug"], board["board_slug"], board["monitor_type"], job)
        except Exception as exc:  # noqa: BLE001 - one odd posting must not sink the board
            log.warning("report.derive_failed", board=board["board_slug"], error=str(exc))
            continue
        if r is not None:
            out.append(r)
    return out


async def crawl_shard(shard: int, shards: int, out_dir: Path) -> dict:
    boards = [b for b in load_boards() if board_shard(b["board_slug"], shards) == shard]
    if report_settings.board_limit:
        boards = boards[: report_settings.board_limit]
    started = time.monotonic()
    rows: list[dict] = []
    status: list[dict] = []
    queue: asyncio.Queue[dict] = asyncio.Queue()
    for b in boards:
        queue.put_nowait(b)

    pol = Politeness()
    http = make_client()

    async def worker() -> None:
        while True:
            try:
                b = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            entry = {
                "board_slug": b["board_slug"],
                "company_slug": b["company_slug"],
                "monitor_type": b["monitor_type"],
                "status": "failed",
                "jobs": 0,
                "error": None,
            }
            for attempt in range(1, report_settings.board_attempts + 1):
                try:
                    got = await _crawl_board(b, http, pol)
                    rows.extend(got)
                    entry.update(status="ok", jobs=len(got), error=None)
                    break
                except BoardGoneError as gone:
                    entry.update(status="gone", error=f"gone: {gone}"[:300])
                    break
                except Exception as exc:  # noqa: BLE001 - recorded per board
                    entry["error"] = f"{type(exc).__name__}: {exc}"[:300]
                    if attempt < report_settings.board_attempts:
                        await asyncio.sleep(3 * attempt)
            status.append(entry)

    try:
        await asyncio.gather(*[worker() for _ in range(max(1, report_settings.concurrency))])
    finally:
        await http.aclose()

    out_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows, schema=SCHEMA, orient="row") if rows else pl.DataFrame(schema=SCHEMA)
    df.write_parquet(out_dir / f"jobs-{shard:02d}.parquet", compression="zstd")
    summary = {
        "shard": shard,
        "shards": shards,
        "finished_at": datetime.now(UTC).isoformat(),
        "elapsed_s": round(time.monotonic() - started, 1),
        "boards": status,
    }
    (out_dir / f"boards-{shard:02d}.json").write_text(json.dumps(summary), encoding="utf-8")
    ok = sum(1 for s in status if s["status"] == "ok")
    return {
        "shard": shard,
        "shards": shards,
        "boards": len(boards),
        "boards_ok": ok,
        "boards_failed": len(status) - ok,
        "jobs": len(rows),
        "elapsed_s": summary["elapsed_s"],
    }
