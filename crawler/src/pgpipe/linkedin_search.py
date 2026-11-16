"""Logged-out LinkedIn guest-API search crawler.

    python -m src.pgpipe.linkedin_search --window-hours 3

Pulls the newest tech postings from LinkedIn's public (no login, no cookies)
``seeMoreJobPostings/search`` endpoint, filters them with the same
tech/country/seniority rules as the ATS pipeline, upserts them into
``crawler.job_posting`` (source='linkedin') and finally runs
``SELECT crawler.build_feed()``.

NEVER use a logged-in LinkedIn session from this module. It is designed to be
polite: one connection, 2-4 s jittered delay between requests, exponential
backoff on 429/999/5xx, abort after 5 consecutive blocks.

Exit codes: 0 ok, 2 configuration / schema problem, 3 aborted after repeated blocks.
"""
from __future__ import annotations

import argparse
import asyncio
import html as _html
import json
import logging
import os
import random
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import parse_qs, unquote, urlparse

import httpx
from selectolax.lexbor import LexborHTMLParser

from src.pgpipe.country import classify_country
from src.pgpipe.seniority import seniority_of
from src.pgpipe.tech_filter import TECH_ONLY, is_tech_role

log = logging.getLogger("linkedin_search")

SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
DETAIL_URL = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
JOB_URL = "https://www.linkedin.com/jobs/view/{job_id}/"
DEFAULT_QUERIES_FILE = Path(__file__).resolve().parents[2] / "data" / "linkedin_queries.json"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

PAGE_SIZE = 10
BLOCK_STATUSES = {401, 403, 429, 999}
MAX_RETRIES = 4
MAX_CONSECUTIVE_BLOCKS = 5
EXIT_OK, EXIT_CONFIG, EXIT_BLOCKED = 0, 2, 3

REQUIRED_COLUMNS = (
    "linkedin_job_id", "apply_url", "company_name", "reposted_at", "repost_count",
)

_JOB_URN_RE = re.compile(r"urn:li:jobPosting:(\d+)")
_REL_RE = re.compile(
    r"(\d+)\s*(minute|min|hour|hr|day|week|month)s?\s+ago", re.I
)
_UNIT_SECONDS = {
    "minute": 60, "min": 60, "hour": 3600, "hr": 3600,
    "day": 86400, "week": 604800, "month": 2592000,
}
LI_APPLY_RE = re.compile(r'<code id="applyUrl"[^>]*>\s*<!--\s*"?(.*?)"?\s*-->', re.S)
_REMOTE_RE = re.compile(r"\bremote\b", re.I)
_SLUG_RE = re.compile(r"[^a-z0-9]+")
_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


class BlockedError(RuntimeError):
    """Too many consecutive blocked responses; the whole run must stop."""


class BudgetExhausted(RuntimeError):
    """The request budget for this run is used up."""


# --------------------------------------------------------------------- parsing
@dataclass
class Card:
    job_id: str
    title: str | None
    company_name: str | None
    company_link: str | None
    location: str | None
    posted_at: datetime | None
    reposted_at: datetime | None
    is_repost: bool
    time_text: str | None
    url: str = ""
    apply_url: str | None = None
    # filled by the pipeline
    country: str | None = None
    seniority: str = "unknown"
    location_type: str | None = None

    def __post_init__(self) -> None:
        self.url = JOB_URL.format(job_id=self.job_id)


def _text(node) -> str | None:
    if node is None:
        return None
    value = " ".join(node.text(strip=True).split())
    return value or None


def parse_relative(text: str | None, now: datetime) -> tuple[datetime | None, int]:
    """'3 hours ago' / 'Reposted 2 days ago' -> (timestamp, granularity seconds)."""
    if not text:
        return None, 0
    low = text.lower()
    m = _REL_RE.search(low)
    if m:
        unit = _UNIT_SECONDS[m.group(2).lower()]
        return now - timedelta(seconds=int(m.group(1)) * unit), unit
    if "just now" in low or "moments ago" in low or "today" in low:
        return now, 60
    return None, 0


def _parse_dt_attr(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_cards(html: str, now: datetime | None = None) -> list[Card]:
    """Parse one guest search page. Tolerant: a malformed card is skipped, not fatal."""
    now = now or datetime.now(timezone.utc)
    tree = LexborHTMLParser(html or "")
    cards: list[Card] = []
    seen: set[str] = set()
    for node in tree.css(".base-search-card, .job-search-card"):
        urn = node.attributes.get("data-entity-urn") or ""
        m = _JOB_URN_RE.search(urn)
        if not m:
            link = node.css_first("a.base-card__full-link")
            m = re.search(r"/jobs/view/(?:[^/?#]*-)?(\d{6,})", (link.attributes.get("href") or "") if link else "")
        if not m or m.group(1) in seen:
            continue
        job_id = m.group(1)
        seen.add(job_id)

        title = _text(node.css_first(".base-search-card__title"))
        company_node = node.css_first(".base-search-card__subtitle")
        company_a = node.css_first('.base-search-card__subtitle a[href*="/company/"]')
        company_name = _text(company_a) or _text(company_node)
        company_link = None
        if company_a is not None:
            company_link = (company_a.attributes.get("href") or "").split("?")[0] or None
        location = _text(node.css_first(".job-search-card__location"))

        time_node = node.css_first("time") or node.css_first('[class*="listdate"]')
        time_text = _text(time_node)
        dt_attr = _parse_dt_attr(time_node.attributes.get("datetime") if time_node is not None else None)
        is_repost = bool(time_text and "repost" in time_text.lower())
        rel_ts, _ = parse_relative(time_text, now)

        posted_at: datetime | None
        reposted_at: datetime | None = None
        if is_repost:
            reposted_at = rel_ts or dt_attr or now
            posted_at = dt_attr if dt_attr and dt_attr <= reposted_at else reposted_at
        else:
            # a relative hour/minute stamp is more precise than the date-only attr
            posted_at = rel_ts if (rel_ts and (dt_attr is None or now - rel_ts < timedelta(days=1))) else dt_attr
            posted_at = posted_at or rel_ts
        if posted_at and posted_at > now + timedelta(hours=1):
            posted_at = now
        cards.append(Card(job_id, title, company_name, company_link, location,
                          posted_at, reposted_at, is_repost, time_text))
    return cards


def parse_apply_url(page: str) -> str | None:
    """External company apply URL from a jobPosting detail page (None for Easy Apply)."""
    m = LI_APPLY_RE.search(page or "")
    if not m:
        return None
    raw = _html.unescape(m.group(1)).strip()
    if not raw:
        return None
    q = parse_qs(urlparse(raw).query)
    url = unquote(q["url"][0]) if "url" in q else raw
    return url if urlparse(url).scheme in ("http", "https") else None


def slugify(name: str) -> str:
    return _SLUG_RE.sub("-", name.lower()).strip("-")


# --------------------------------------------------------------------- filtering
def classify_card(
    card: Card,
    *,
    tech_only: bool = TECH_ONLY,
    keep_other_countries: bool = False,
    title_exclude: Iterable[str] = (),
) -> str | None:
    """Fill derived fields; return a drop reason ('no_title', 'not_tech', ...) or None to keep."""
    if not card.title or not card.company_name:
        return "incomplete"
    low = card.title.lower()
    for bad in title_exclude:
        if bad and bad.lower() in low:
            return "title_excluded"
    if tech_only and not is_tech_role(card.title):
        return "not_tech"
    card.seniority = seniority_of(card.title)
    card.country = classify_country(card.location)
    if card.location and _REMOTE_RE.search(card.location):
        card.location_type = "remote"
    if not keep_other_countries and card.country == "other" and card.location_type != "remote":
        return "other_country"
    return None


# --------------------------------------------------------------------- HTTP
class Fetcher:
    """Single-connection polite fetcher with backoff, block counting and a budget."""

    def __init__(
        self,
        client: httpx.Client,
        *,
        budget: int = 700,
        delay: tuple[float, float] = (2.0, 4.0),
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
        base_backoff: float = 8.0,
    ) -> None:
        self.client = client
        self.budget = budget
        self.delay = delay
        self.sleep = sleep
        self.rng = rng or random.Random()
        self.base_backoff = base_backoff
        self.requests = 0
        self.blocked = 0              # total blocked responses
        self.consecutive_blocks = 0
        self._first = True

    def _pace(self) -> None:
        if self._first:
            self._first = False
            return
        self.sleep(self.rng.uniform(*self.delay))

    def backoff_seconds(self, attempt: int) -> float:
        """Exponential backoff with full jitter: base*2^attempt * U(0.5, 1.5)."""
        return self.base_backoff * (2 ** attempt) * self.rng.uniform(0.5, 1.5)

    def get(self, url: str, params: dict | None = None) -> str | None:
        """Return body text, or None when the page is unavailable (retries exhausted / 4xx).

        Raises BlockedError after MAX_CONSECUTIVE_BLOCKS consecutive blocked
        responses and BudgetExhausted when the request budget is spent."""
        for attempt in range(MAX_RETRIES + 1):
            if self.requests >= self.budget:
                raise BudgetExhausted(f"request budget {self.budget} reached")
            self._pace()
            self.requests += 1
            try:
                resp = self.client.get(url, params=params)
                status = resp.status_code
            except httpx.HTTPError as exc:
                log.warning("network error on attempt %d: %s", attempt + 1, exc)
                status = 599
                resp = None
            if status == 200 and resp is not None:
                self.consecutive_blocks = 0
                return resp.text
            if status in BLOCK_STATUSES:
                self.blocked += 1
                self.consecutive_blocks += 1
                log.warning("blocked (HTTP %s), consecutive=%d", status, self.consecutive_blocks)
                if self.consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
                    raise BlockedError(
                        f"{self.consecutive_blocks} consecutive blocks (last HTTP {status}); aborting run"
                    )
            elif status >= 500:
                log.warning("server error HTTP %s", status)
            else:
                log.warning("unexpected HTTP %s; skipping", status)
                return None
            if attempt < MAX_RETRIES:
                self.sleep(self.backoff_seconds(attempt))
        return None


def build_params(keyword: str, location: str, window_hours: float, start: int) -> dict[str, str | int]:
    return {
        "keywords": keyword,
        "location": location,
        "f_TPR": f"r{max(60, int(window_hours * 3600))}",
        "sortBy": "DD",
        "start": start,
    }


# --------------------------------------------------------------------- crawl
@dataclass
class Stats:
    requests: int = 0
    cards: int = 0
    unique: int = 0
    kept: int = 0
    new: int = 0
    updated: int = 0
    reposted: int = 0
    filtered: dict[str, int] = field(default_factory=dict)
    blocked: int = 0
    details: int = 0
    aborted: bool = False
    skipped_queries: int = 0

    def line(self) -> str:
        filt = sum(self.filtered.values())
        return (
            f"requests={self.requests} cards={self.cards} unique={self.unique} kept={self.kept} "
            f"new={self.new} updated={self.updated} reposted={self.reposted} "
            f"filtered_out={filt} {self.filtered or ''} blocked={self.blocked} "
            f"details={self.details} aborted={self.aborted}"
        )


def load_queries(path: Path | str) -> dict:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    keywords = [k for k in data.get("keywords", []) if isinstance(k, str) and k.strip()]
    locations = [k for k in data.get("locations", []) if isinstance(k, str) and k.strip()]
    if not keywords or not locations:
        raise ValueError(f"{path}: needs non-empty 'keywords' and 'locations'")
    return {
        "keywords": keywords,
        "locations": locations,
        "pages_per_query": int(data.get("pages_per_query", 5)),
    }


def crawl(
    fetcher: Fetcher,
    keywords: list[str],
    locations: list[str],
    *,
    window_hours: float,
    pages_per_query: int,
    shuffle_seed: int | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    stats: Stats | None = None,
) -> tuple[dict[str, Card], Stats]:
    """Run every keyword x location query. Returns {job_id: Card} (deduped) and stats.

    BlockedError is caught here: partial results are returned with stats.aborted=True."""
    stats = stats or Stats()
    plans = [(k, loc) for k in keywords for loc in locations]
    random.Random(shuffle_seed).shuffle(plans)   # budget cuts don't always starve the same queries
    found: dict[str, Card] = {}
    try:
        for idx, (kw, loc) in enumerate(plans):
            start = 0
            for _page in range(pages_per_query):
                body = fetcher.get(SEARCH_URL, build_params(kw, loc, window_hours, start))
                if body is None:
                    break
                cards = parse_cards(body, now())
                stats.cards += len(cards)
                for c in cards:
                    found.setdefault(c.job_id, c)
                if len(cards) < PAGE_SIZE:
                    break
                start += max(PAGE_SIZE, len(cards))
    except BudgetExhausted as exc:
        stats.skipped_queries = max(0, len(plans) - idx)
        log.warning("%s; %d queries not (fully) run", exc, stats.skipped_queries)
    except BlockedError as exc:
        stats.aborted = True
        log.error("ABORT: %s", exc)
    stats.requests = fetcher.requests
    stats.blocked = fetcher.blocked
    stats.unique = len(found)
    return found, stats


def filter_cards(cards: dict[str, Card], stats: Stats, **kw) -> list[Card]:
    kept: list[Card] = []
    for card in cards.values():
        reason = classify_card(card, **kw)
        if reason:
            stats.filtered[reason] = stats.filtered.get(reason, 0) + 1
        else:
            kept.append(card)
    stats.kept = len(kept)
    return kept


def fetch_apply_urls(fetcher: Fetcher, cards: list[Card], limit: int, stats: Stats) -> None:
    for card in cards[:limit]:
        try:
            body = fetcher.get(DETAIL_URL.format(job_id=card.job_id))
        except (BudgetExhausted, BlockedError) as exc:
            log.warning("details pass stopped: %s", exc)
            if isinstance(exc, BlockedError):
                stats.aborted = True
            break
        stats.details += 1
        if body:
            card.apply_url = parse_apply_url(body)
    stats.requests = fetcher.requests
    stats.blocked = fetcher.blocked


# --------------------------------------------------------------------- DB
def _schema() -> str:
    schema = (os.environ.get("PGPIPE_SCHEMA", "crawler").strip() or "crawler")
    if not _IDENT_RE.match(schema):
        raise ValueError(f"PGPIPE_SCHEMA {schema!r} must be a simple lowercase identifier")
    return schema


def get_dsn() -> str:
    dsn = (os.environ.get("CRAWLER_DATABASE_URL") or os.environ.get("LOCAL_DATABASE_URL") or "").strip()
    if not dsn:
        raise ValueError("CRAWLER_DATABASE_URL (or LOCAL_DATABASE_URL) is not set")
    return dsn


def company_sql(schema: str) -> str:
    return (
        f"INSERT INTO {schema}.company (slug, name, company_type) VALUES ($1, $2, 'unknown') "
        "ON CONFLICT (slug) DO NOTHING"
    )


def posting_sql(schema: str) -> str:
    t = f"{schema}.job_posting"
    return (
        f"INSERT INTO {t} (company_slug, company_name, title, url, linkedin_job_id, location, "
        "location_type, seniority, country, apply_url, posted_at, reposted_at, source, status, "
        "first_seen_at, last_seen_at) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,COALESCE($11, now()),$12,'linkedin','open',now(),now()) "
        "ON CONFLICT (linkedin_job_id) WHERE linkedin_job_id IS NOT NULL DO UPDATE SET "
        "last_seen_at = now(), status = 'open', "
        f"reposted_at = COALESCE(EXCLUDED.reposted_at, {t}.reposted_at), "
        f"repost_count = {t}.repost_count + "
        f"(EXCLUDED.reposted_at IS NOT NULL AND EXCLUDED.reposted_at IS DISTINCT FROM {t}.reposted_at)::int "
        "RETURNING (xmax = 0) AS inserted"
    )


def stable_reposted_at(card: Card, existing: datetime | None) -> datetime | None:
    """Relative 'Reposted N hours ago' text yields a slightly different timestamp each run.
    If the stored reposted_at is within tolerance of the new one, reuse the stored value so
    the ON CONFLICT clause doesn't count the same repost twice."""
    new = card.reposted_at
    if new is None or existing is None:
        return new
    _, gran = parse_relative(card.time_text, new)
    tol = timedelta(hours=36) if gran >= 86400 else timedelta(hours=8)
    return existing if abs(new - existing) <= tol else new


def posting_args(card: Card, reposted_at: datetime | None) -> tuple:
    return (
        slugify(card.company_name or ""), card.company_name, card.title, card.url, card.job_id,
        card.location, card.location_type, card.seniority, card.country, card.apply_url,
        card.posted_at, reposted_at,
    )


async def check_schema(conn, schema: str) -> None:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = 'job_posting'", schema)
    have = {r["column_name"] for r in rows}
    missing = [c for c in REQUIRED_COLUMNS if c not in have]
    if missing:
        raise RuntimeError(
            f"{schema}.job_posting is missing columns {missing}. "
            "The LinkedIn migration is not applied: run `jobs migrate` (from the crawler/ dir) first."
        )
    fn = await conn.fetchval(
        "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = $1 AND p.proname = 'build_feed'", schema)
    if not fn:
        raise RuntimeError(f"{schema}.build_feed() not found: run `jobs migrate` first.")


async def preflight(dsn: str, schema: str) -> None:
    import asyncpg
    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    try:
        await check_schema(conn, schema)
    finally:
        await conn.close()


async def existing_state(conn, schema: str, ids: list[str]) -> dict[str, datetime | None]:
    rows = await conn.fetch(
        f"SELECT linkedin_job_id, reposted_at FROM {schema}.job_posting "
        "WHERE linkedin_job_id = ANY($1::text[])", ids)
    return {r["linkedin_job_id"]: r["reposted_at"] for r in rows}


async def persist(
    dsn: str,
    schema: str,
    cards: list[Card],
    stats: Stats,
    *,
    details: Callable[[list[Card]], None] | None = None,
) -> int:
    """Upsert companies + postings in one transaction, then build_feed(). Returns feed size."""
    import asyncpg
    conn = await asyncpg.connect(dsn, statement_cache_size=0, command_timeout=120)
    try:
        await check_schema(conn, schema)
        existing = await existing_state(conn, schema, [c.job_id for c in cards])
        if details:
            details([c for c in cards if c.job_id not in existing])
        company_rows = {slugify(c.company_name or ""): c.company_name for c in cards}
        company_rows.pop("", None)
        async with conn.transaction():
            await conn.executemany(company_sql(schema), list(company_rows.items()))
            for c in cards:
                slug = slugify(c.company_name or "")
                if not slug:
                    continue
                old = existing.get(c.job_id)
                rep = stable_reposted_at(c, old)
                inserted = await conn.fetchval(posting_sql(schema), *posting_args(c, rep))
                if inserted:
                    stats.new += 1
                else:
                    stats.updated += 1
                    if rep is not None and rep != old:
                        stats.reposted += 1
        return await conn.fetchval(f"SELECT {schema}.build_feed()")
    finally:
        await conn.close()


# --------------------------------------------------------------------- CLI
def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m src.pgpipe.linkedin_search", description=__doc__.split("\n\n")[0])
    p.add_argument("--window-hours", type=float, default=3.0, help="only jobs posted in the last N hours (default 3)")
    p.add_argument("--max-requests", type=int, default=700, help="total HTTP request budget (default 700)")
    p.add_argument("--queries-file", default=str(DEFAULT_QUERIES_FILE))
    p.add_argument("--pages-per-query", type=int, default=None, help="override the file's pages_per_query")
    p.add_argument("--dry-run", action="store_true", help="print JSON, do not touch the DB")
    p.add_argument("--details", action="store_true", help="fetch external apply URLs for NEW jobs only (off by default)")
    p.add_argument("--max-details", type=int, default=100)
    p.add_argument("--keep-other-countries", action="store_true", help="do not drop non US/IN locations")
    p.add_argument("--include-non-tech", action="store_true", help="disable the tech-role title filter")
    p.add_argument("--title-exclude", default="", help="comma list of title substrings to drop")
    p.add_argument("--min-delay", type=float, default=2.0)
    p.add_argument("--max-delay", type=float, default=4.0)
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        plan = load_queries(args.queries_file)
    except (OSError, ValueError) as exc:
        log.error("bad queries file: %s", exc)
        return EXIT_CONFIG
    pages = args.pages_per_query or plan["pages_per_query"]

    dsn = schema = None
    if not args.dry_run:
        try:
            dsn, schema = get_dsn(), _schema()
            asyncio.run(preflight(dsn, schema))
        except (ValueError, RuntimeError) as exc:
            log.error("%s", exc)
            return EXIT_CONFIG
        except Exception as exc:  # connection failures etc.
            log.error("database preflight failed: %s", exc)
            return EXIT_CONFIG

    stats = Stats()
    limits = httpx.Limits(max_connections=1, max_keepalive_connections=1)
    with httpx.Client(headers=HEADERS, timeout=httpx.Timeout(20.0), limits=limits,
                      follow_redirects=True) as client:
        fetcher = Fetcher(client, budget=args.max_requests, delay=(args.min_delay, args.max_delay))
        found, stats = crawl(fetcher, plan["keywords"], plan["locations"],
                             window_hours=args.window_hours, pages_per_query=pages, stats=stats)
        kept = filter_cards(
            found, stats,
            tech_only=not args.include_non_tech,
            keep_other_countries=args.keep_other_countries,
            title_exclude=[s.strip() for s in args.title_exclude.split(",") if s.strip()],
        )

        def run_details(new_cards: list[Card]) -> None:
            if args.details and not stats.aborted:
                fetch_apply_urls(fetcher, new_cards, args.max_details, stats)

        if args.dry_run:
            run_details(kept)
            print(json.dumps([_card_json(c) for c in kept], indent=2, default=str))
            log.info("SUMMARY (dry-run) %s", stats.line())
        else:
            try:
                size = asyncio.run(persist(dsn, schema, kept, stats, details=run_details))
                log.info("feed rebuilt: %s rows", size)
            except Exception as exc:
                log.error("database write failed: %s", exc)
                log.info("SUMMARY %s", stats.line())
                return EXIT_CONFIG
            log.info("SUMMARY %s", stats.line())

    if stats.aborted:
        log.error("run aborted after repeated blocks from LinkedIn (exit 3); partial results were saved")
        return EXIT_BLOCKED
    return EXIT_OK


def _card_json(c: Card) -> dict:
    d = asdict(c)
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in d.items()}


if __name__ == "__main__":
    sys.exit(main())
