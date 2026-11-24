"""LinkedIn guest job search -> crawler.job_posting (source='linkedin').

Uses ONLY LinkedIn's public, signed-out guest endpoint

    https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search
        ?keywords=<q>&location=<India|United States>&f_TPR=r86400&start=<0,10,..>

Never signs in, never solves captchas. Polite: one request at a time with a
randomised 2-5 s gap; HTTP 429 / 999 are counted and after 3 CONSECUTIVE
refusals LinkedIn is abandoned for the rest of the run.

Stored lean: title, company, location, canonical URL
(https://www.linkedin.com/jobs/view/<id>), external_id = LinkedIn job id.
No descriptions. company_slug is the normalised company name (same slug form
as the board-imported companies, so rows merge per company).
"""
from __future__ import annotations

import asyncio
import html as html_mod
import json
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import quote, urlencode

import asyncpg
import httpx

from src.pgpipe.board_import import slugify
from src.pgpipe.config import settings
from src.pgpipe.country import classify_country
from src.pgpipe.cleanup import MixGate
from src.pgpipe.dbguard import SizeGuard
from src.pgpipe.role_classify import classify_role
from src.pgpipe.posted_at import parse_posted_at
from src.pgpipe.seniority import seniority_of

SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?"
PAGE_SIZE = 10
LOCATIONS = ("India", "United States")

# Role-family queries: tech families + product-associated roles only (the pool
# mix is ~90% tech / ~10% product; corporate roles are not searched).
TECH_QUERIES: tuple[str, ...] = (
    "software engineer", "backend developer", "frontend developer", "full stack developer",
    "java developer", "python developer", "react developer", "node js developer", ".net developer",
    "android developer", "ios developer", "mobile developer",
    "data engineer", "data scientist", "machine learning engineer", "ai engineer", "data analyst",
    "devops engineer", "site reliability engineer", "cloud engineer", "platform engineer", "kubernetes",
    "security engineer", "cybersecurity analyst", "information security",
    "qa engineer", "sdet", "test automation engineer",
    "embedded engineer", "firmware engineer", "network engineer", "solutions architect",
    "engineering manager", "technical lead", "software engineer intern", "associate software engineer",
    "senior software engineer", "staff engineer", "principal engineer",
)
PRODUCT_QUERIES: tuple[str, ...] = (
    "product manager", "product owner", "technical program manager", "project manager it",
    "ux designer", "product designer", "business analyst", "product analyst",
    "technical writer", "solutions engineer", "sales engineer", "customer success engineer",
)
QUERIES: tuple[str, ...] = TECH_QUERIES + PRODUCT_QUERIES

# India-heavy plan: (location, queries, max_pages). India gets every query with
# deep paging; the US only a short core list (US rows are capped at ~10% anyway).
US_QUERIES: tuple[str, ...] = (
    "software engineer", "data engineer", "machine learning engineer", "devops engineer",
    "security engineer", "product manager",
)
PLAN: tuple[tuple[str, tuple[str, ...], int], ...] = (
    ("India", QUERIES, 8),
    ("United States", US_QUERIES, 2),
)

_TAG_RE = re.compile(r"<[^>]+>")


def _text(s: str | None) -> str:
    return re.sub(r"\s+", " ", html_mod.unescape(_TAG_RE.sub(" ", s or ""))).strip()


def canonical_url(job_id: str) -> str:
    return f"https://www.linkedin.com/jobs/view/{job_id}"


def company_slug_of(name: str) -> str:
    return slugify(name, 80)


def parse_cards(page: str) -> list[dict]:
    """Guest search page -> [{id,title,company,location,posted}] (posted = the
    card's <time datetime>, YYYY-MM-DD, or '')."""
    out: list[dict] = []
    for li in page.split("<li>")[1:]:
        m = re.search(r"urn:li:jobPosting:(\d+)", li)
        if not m:
            continue
        title = re.search(r'base-search-card__title">(.*?)</h3>', li, re.S)
        comp = re.search(r'base-search-card__subtitle">(.*?)</h4>', li, re.S)
        loc = re.search(r'job-search-card__location">(.*?)</span>', li, re.S)
        tm = re.search(r'<time[^>]*datetime="([^"]+)"', li)
        out.append({
            "id": m.group(1),
            "title": _text(title.group(1)) if title else "",
            "company": _text(comp.group(1)) if comp else "",
            "location": _text(loc.group(1)) if loc else "",
            "posted": tm.group(1) if tm else "",
        })
    return out


def card_to_row(card: dict) -> dict | None:
    """Card -> row dict ready for upsert, or None if it must be dropped
    (no title/company, or location not in settings.countries)."""
    if not (card.get("id") and card.get("title") and card.get("company")):
        return None
    slug = company_slug_of(card["company"])
    if not slug:
        return None
    country = classify_country(card["location"], [card["location"]] if card["location"] else None)
    if country not in settings.countries:
        return None
    family, category = classify_role(card["title"])
    if settings.role_filter and family not in ("tech", "product"):
        return None
    posted = parse_posted_at(card.get("posted") or None)
    return {
        "role_family": family,
        "role_category": category,
        "company_slug": slug,
        "company_name": card["company"],
        "external_id": card["id"],
        "url": canonical_url(card["id"]),
        "title": card["title"],
        "location": card["location"] or None,
        "date_posted": card.get("posted") or None,
        "seniority": seniority_of(card["title"]),
        "country": country,
        "posted_at": posted,
    }


# --- throttle ------------------------------------------------------------------

@dataclass
class LinkedInState:
    max_consecutive_refusals: int = 3
    consecutive_refusals: int = 0
    refusals_total: int = 0
    requests: int = 0
    dead: bool = False
    log: list[str] = field(default_factory=list)

    def ok(self) -> None:
        self.consecutive_refusals = 0

    def refused(self, code: int) -> None:
        self.refusals_total += 1
        self.consecutive_refusals += 1
        if self.consecutive_refusals >= self.max_consecutive_refusals:
            self.dead = True
            msg = (f"LinkedIn answered HTTP {code} {self.consecutive_refusals} times in a row: "
                   "stopping LinkedIn for this run")
            self.log.append(msg)
            print("!!! " + msg, file=sys.stderr, flush=True)


async def fetch_page(client: httpx.AsyncClient, state: LinkedInState, params: dict,
                     sleep=asyncio.sleep) -> str | None:
    """One guest-search request. Returns page HTML, '' for end-of-results /
    non-refusal error, None when LinkedIn is (now) dead for this run."""
    if state.dead:
        return None
    state.requests += 1
    try:
        resp = await client.get(SEARCH_URL + urlencode(params))
    except httpx.HTTPError:
        return ""
    if resp.status_code in (429, 999):
        state.refused(resp.status_code)
        if state.dead:
            return None
        await sleep(15 * state.consecutive_refusals)  # short backoff, then caller moves on
        return ""
    if resp.status_code != 200:
        return ""
    state.ok()
    return resp.text


# --- storage -------------------------------------------------------------------

# Same no-op-rewrite guard as ingest._STALE_SEEN.
_STALE_SEEN = (
    f"job_posting.status <> 'open' OR "
    f"job_posting.last_seen_at < now() - interval '{int(settings.touch_min_hours)} hours'"
)

_UPSERT_LI = f"""
WITH incoming AS (
    SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
                         $6::text[], $7::text[], $8::text[], $9::text[], $10::timestamptz[],
                         $11::text[], $12::text[])
         AS t(company_slug, external_id, url, title, location, date_posted, seniority,
              country, titles_json, posted_at, role_family, role_category)
),
new_co AS (
    INSERT INTO company (slug, name, updated_at)
    SELECT DISTINCT ON (i.company_slug) i.company_slug, i.company_slug, now()
    FROM incoming i
    ON CONFLICT (slug) DO NOTHING
    RETURNING 1
),
ins AS (
    INSERT INTO job_posting
        (company_slug, board_slug, source, external_id, url, title, titles, location,
         locations, date_posted, seniority, country, posted_at, role_family,
         role_category, first_seen_at, last_seen_at, status)
    SELECT i.company_slug, NULL, 'linkedin', i.external_id, i.url, i.title,
           i.titles_json::jsonb,
           i.location,
           CASE WHEN i.location IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(i.location) END,
           i.date_posted, i.seniority, i.country, COALESCE(i.posted_at, now()), i.role_family, i.role_category, now(), now(), 'open'
    FROM incoming i
    WHERE NOT EXISTS (   -- same role already crawled from the company's own board
        SELECT 1 FROM job_posting p
        WHERE p.company_slug = i.company_slug AND p.source <> 'linkedin'
          AND lower(p.title) = lower(i.title) AND p.country IS NOT DISTINCT FROM i.country
    )
    ON CONFLICT (company_slug, url) DO UPDATE SET last_seen_at = now(), status = 'open'
        WHERE {_STALE_SEEN}   -- skip no-op rewrites (see config.touch_min_hours)
    RETURNING (xmax = 0) AS inserted
)
SELECT count(*) FILTER (WHERE inserted) AS inserted,
       count(*) FILTER (WHERE NOT inserted) AS updated,
       (SELECT count(*) FROM new_co) AS new_companies
FROM ins
"""

_TOUCH_LI = (
    "UPDATE job_posting SET last_seen_at = now(), status = 'open' "
    f"WHERE company_slug = $1 AND url = $2 AND ({_STALE_SEEN})"
)


async def store_rows(pool: asyncpg.Pool, rows: list[dict], guard: SizeGuard | None = None) -> dict[str, int]:
    """Upsert one batch. Over MAX_DB_MB: only refresh rows we already hold."""
    uniq = {(r["company_slug"], r["url"]): r for r in rows}
    rows = list(uniq.values())
    if not rows:
        return {"inserted": 0, "updated": 0, "suppressed": 0, "new_companies": 0}
    if guard and await guard.refresh():
        async with pool.acquire() as conn:
            await conn.executemany(_TOUCH_LI, [(r["company_slug"], r["url"]) for r in rows])
        return {"inserted": 0, "updated": 0, "suppressed": len(rows), "new_companies": 0}
    now = datetime.now(timezone.utc)
    cols = list(zip(*[
        (r["company_slug"], r["external_id"], r["url"], r["title"], r["location"],
         r["date_posted"], r["seniority"], r["country"], json.dumps([r["title"]]),
         r["posted_at"] or now, r["role_family"], r["role_category"])
        for r in rows
    ]))
    row = await pool.fetchrow(_UPSERT_LI, *[list(c) for c in cols])
    # company names: the CTE seeds name=slug; set the real display name for new ones
    names = {r["company_slug"]: r["company_name"] for r in rows}
    await pool.execute(
        "UPDATE company SET name = v.n FROM unnest($1::text[], $2::text[]) AS v(s, n) "
        "WHERE company.slug = v.s AND company.name = company.slug",
        list(names), list(names.values()),
    )
    return {"inserted": int(row["inserted"]), "updated": int(row["updated"]),
            "suppressed": 0, "new_companies": int(row["new_companies"])}


# --- driver --------------------------------------------------------------------

def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(25.0, connect=15.0),
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
    )


async def run_linkedin(
    pool: asyncpg.Pool,
    *,
    plan: tuple[tuple[str, tuple[str, ...], int], ...] | None = None,
    queries: tuple[str, ...] | None = None,
    locations: tuple[str, ...] | None = None,
    max_pages: int = 6,
    window_hours: float = 24.0,
    gap: tuple[float, float] = (2.0, 5.0),
    time_budget_s: float | None = None,
    client: httpx.AsyncClient | None = None,
    sleep=asyncio.sleep,
    guard: SizeGuard | None = None,
) -> dict:
    if plan is None:
        if queries is None and locations is None:
            plan = PLAN                      # India-heavy tech + product plan
        else:  # explicit override (tests / CLI --limit-queries)
            plan = tuple((loc, queries or QUERIES, max_pages) for loc in (locations or LOCATIONS))
    state = LinkedInState()
    guard = guard or SizeGuard(pool)
    mix = MixGate(pool)
    own = client is None
    client = client or make_client()
    tpr = f"r{int(round(window_hours * 3600))}" if window_hours else ""
    started = time.monotonic()
    totals = {"inserted": 0, "updated": 0, "suppressed": 0, "new_companies": 0}
    cards_seen = kept = pages = 0
    seen_ids: set[str] = set()
    try:
        for loc, loc_queries, loc_pages in plan:
            for q in loc_queries:
                if state.dead or (time_budget_s and time.monotonic() - started > time_budget_s):
                    break
                for p in range(loc_pages):
                    params = {"keywords": q, "location": loc, "start": p * PAGE_SIZE}
                    if tpr:
                        params["f_TPR"] = tpr
                    html = await fetch_page(client, state, params, sleep)
                    if html is None:
                        break
                    pages += 1
                    cards = parse_cards(html) if html else []
                    await mix.refresh()
                    batch = []
                    for c in cards:
                        if c["id"] in seen_ids:
                            continue
                        seen_ids.add(c["id"])
                        cards_seen += 1
                        r = card_to_row(c)
                        if r and mix.allow(r["role_family"], r["country"], r["posted_at"]):
                            batch.append(r)
                    kept += len(batch)
                    if batch:
                        res = await store_rows(pool, batch, guard)
                        for k in totals:
                            totals[k] += res[k]
                    await sleep(random.uniform(*gap))
                    if len(cards) < PAGE_SIZE:
                        break  # short/empty page = end of results for this query
    finally:
        if own:
            await client.aclose()
    return {
        "linkedin": "stopped_by_refusals" if state.dead else "ok",
        "requests": state.requests,
        "pages_read": pages,
        "refusals_429_999": state.refusals_total,
        "cards_seen_unique": cards_seen,
        "kept_in_or_us": kept,
        **totals,
        "elapsed_s": round(time.monotonic() - started, 1),
        "queries": sum(len(qs) for _, qs, _ in plan),
        "locations": [loc for loc, _, _ in plan],
        "notes": state.log,
    }


def query_url(q: str, loc: str, start: int = 0, tpr: str = "r86400") -> str:
    return SEARCH_URL + f"keywords={quote(q)}&location={quote(loc)}&f_TPR={tpr}&start={start}"
