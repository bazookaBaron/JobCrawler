"""data/companies.csv + data/boards.csv  ->  <schema>.company / <schema>.job_board.

CSVs are the source of truth. Rows removed from a CSV have their board disabled
(is_enabled = false) rather than deleted, so posting history survives.
"""
from __future__ import annotations

import csv
import json
from urllib.parse import urlparse

import asyncpg

from src.pgpipe.config import DATA_DIR


def _read_companies() -> list[dict]:
    with open(DATA_DIR / "companies.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _read_boards() -> list[dict]:
    with open(DATA_DIR / "boards.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _parse_config(raw: str | None) -> str:
    raw = (raw or "").strip()
    if not raw:
        return "{}"
    try:
        return json.dumps(json.loads(raw))
    except Exception:
        return "{}"


def domain_of(website: str | None) -> str | None:
    """'https://www.Example.com/path' -> 'example.com' (None when not derivable)."""
    raw = (website or "").strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = "https://" + raw
    try:
        host = (urlparse(raw).hostname or "").lower().strip(".")
    except ValueError:
        return None
    if host.startswith("www."):
        host = host[4:]
    return host if "." in host else None


def _opt(v: str | None) -> str | None:
    v = (v or "").strip()
    return v or None


_COMPANY_TYPES = {"product", "startup", "service", "unknown"}


def _company_type(raw: str | None) -> str:
    v = (raw or "").strip().lower()
    return v if v in _COMPANY_TYPES else "unknown"


async def sync(pool: asyncpg.Pool) -> dict[str, int]:
    companies = _read_companies()
    boards = _read_boards()

    async with pool.acquire() as conn, conn.transaction():
        await conn.executemany(
            """
            INSERT INTO company (slug, name, website, industry, company_type,
                                 logo_url, icon_url, domain, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
            ON CONFLICT (slug) DO UPDATE SET
                name = EXCLUDED.name,
                website = EXCLUDED.website,
                industry = EXCLUDED.industry,
                company_type = EXCLUDED.company_type,
                logo_url = EXCLUDED.logo_url,
                icon_url = EXCLUDED.icon_url,
                domain = COALESCE(EXCLUDED.domain, company.domain),
                updated_at = now()
            """,
            [
                (
                    c["slug"],
                    c.get("name") or c["slug"],
                    (c.get("website") or None),
                    int(c["industry"]) if (c.get("industry") or "").strip().isdigit() else None,
                    _company_type(c.get("company_type")),
                    _opt(c.get("logo_url")),
                    _opt(c.get("icon_url")),
                    domain_of(c.get("website")),
                )
                for c in companies
            ],
        )

        await conn.executemany(
            """
            INSERT INTO job_board
                (board_slug, company_slug, board_url, monitor_type, monitor_config,
                 is_enabled, updated_at)
            VALUES ($1, $2, $3, $4, $5::jsonb, true, now())
            ON CONFLICT (board_slug) DO UPDATE SET
                company_slug = EXCLUDED.company_slug,
                board_url = EXCLUDED.board_url,
                monitor_type = EXCLUDED.monitor_type,
                monitor_config = EXCLUDED.monitor_config,
                origin = 'csv',
                is_enabled = true,
                updated_at = now()
            """,
            [
                (
                    b["board_slug"],
                    b["company_slug"],
                    b["board_url"],
                    b["monitor_type"],
                    _parse_config(b.get("monitor_config")),
                )
                for b in boards
            ],
        )

        present = [b["board_slug"] for b in boards]
        await conn.execute(
            """
            UPDATE job_board SET is_enabled = false, updated_at = now()
            WHERE is_enabled = true AND origin = 'csv' AND board_slug <> ALL($1::text[])
            """,
            present,
        )
        disabled = await conn.fetchval(
            "SELECT count(*) FROM job_board WHERE is_enabled = false"
        )

    return {
        "companies": len(companies),
        "boards": len(boards),
        "boards_disabled_total": int(disabled or 0),
    }
