"""Compact, *rich* Workday monitor — NOT jobseek's URL-only src/core/monitors/workday.py.

Hits the Workday CXS search API directly and returns full DiscoveredJob rows,
so no scraper / description fetch is needed.

board monitor_config (same keys upstream jobseek uses):
    {"company": "<tenant>", "wd_instance": "<dc>", "site": "<site>"}
e.g. {"company": "nvidia", "wd_instance": "wd5", "site": "NVIDIAExternalCareerSite"}

POST https://{tenant}.{dc}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs
     {"limit": 20, "offset": N, "searchText": "", "appliedFacets": {}}
paginate by 20 to `total` (hard cap 2000 postings / 100 pages).
"""
from __future__ import annotations

import asyncio
import re

import httpx
import structlog

from src.core.monitors import DiscoveredJob
from src.pgpipe.config import settings

log = structlog.get_logger()

_PAGE = 20
_MAX_POSTINGS = 2000
_MAX_PAGES = _MAX_POSTINGS // _PAGE
_AGE_RE = re.compile(r"(\d+)\+?\s*days?\s+ago", re.I)  # "Posted 3 Days Ago", "Posted 30+ Days Ago"


def _age_days(posted_on: str | None) -> int | None:
    """Whole days since posting from Workday's relative text (None if unknown)."""
    t = (posted_on or "").strip().lower()
    if not t:
        return None
    if "today" in t or "just" in t:
        return 0
    if "yesterday" in t:
        return 1
    m = _AGE_RE.search(t)
    return int(m.group(1)) if m else None

# https://{tenant}.{dc}.myworkdayjobs.com/[locale/]{site}[/...]
_URL_RE = re.compile(
    r"https?://(?P<tenant>[^./]+)\.(?P<dc>wd\d+)\.myworkdayjobs\.com/"
    r"(?:(?P<locale>[a-z]{2}-[A-Za-z]{2})/)?(?P<site>[^/?#]+)",
    re.I,
)


class _Gate:
    """Global min-gap between ANY two Workday requests from this process.
    Workday rate-limits per client IP across all tenants (the per-host gate in
    throttle.py is not enough: every tenant is a different host), and the board
    crawl issues many sequential page requests, so gate each request."""

    def __init__(self) -> None:
        self._lock: asyncio.Lock | None = None
        self._next = 0.0

    async def wait(self, gap: float) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            loop = asyncio.get_running_loop()
            delay = self._next - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            self._next = loop.time() + gap


    def cool_off(self, seconds: float) -> None:
        try:
            self._next = max(self._next, asyncio.get_running_loop().time() + seconds)
        except RuntimeError:  # pragma: no cover
            pass


_gate = _Gate()
_RETRY_WAITS = (8.0, 20.0, 45.0)


async def _post(client: httpx.AsyncClient, url: str, body: dict, headers: dict) -> httpx.Response:
    """POST with the global gate + patient retry on 429 / 503 (Retry-After honoured)."""
    for attempt in range(len(_RETRY_WAITS) + 1):
        await _gate.wait(settings.workday_gap_seconds)
        resp = await client.post(url, json=body, headers=headers)
        if resp.status_code not in (429, 503):
            return resp
        if attempt == len(_RETRY_WAITS):
            # still refused after patient retries: cool the whole Workday lane for a minute
            _gate.cool_off(60.0)
            return resp
        try:
            wait = float(resp.headers.get("Retry-After", ""))
        except ValueError:
            wait = _RETRY_WAITS[attempt]
        await asyncio.sleep(min(max(wait, 1.0), 60.0))
    return resp  # pragma: no cover


def _cfg(board: dict) -> tuple[str, str, str]:
    """Resolve (tenant, dc, site). Upstream boards.csv uses several key names
    (company/tenant, wd_instance/dc, site/board/site_id/job_board/board_id) and
    some leave monitor_config empty — so fall back to parsing the board_url."""
    md = board.get("metadata") or {}
    tenant = (md.get("company") or md.get("tenant") or "").strip()
    dc = (md.get("wd_instance") or md.get("dc") or "").strip()
    site = (
        md.get("site") or md.get("board") or md.get("site_id")
        or md.get("job_board") or md.get("board_id") or ""
    ).strip()

    m = _URL_RE.match(board.get("board_url") or "")
    if m:
        tenant = tenant or m.group("tenant")
        dc = dc or m.group("dc")
        site = site or m.group("site")

    if not (tenant and dc and site):
        raise ValueError(
            f"workday board {board.get('board_slug')!r}: cannot resolve tenant/dc/site "
            f"from metadata {md!r} or url {board.get('board_url')!r}"
        )
    return tenant, dc, site


def _base(tenant: str, dc: str) -> str:
    return f"https://{tenant}.{dc}.myworkdayjobs.com"


def _parse_posting(posting: dict, tenant: str, dc: str, site: str) -> DiscoveredJob | None:
    ext = posting.get("externalPath") or ""
    if not ext:
        return None
    url = f"{_base(tenant, dc)}/{site}{ext}"
    title = (posting.get("title") or "").strip() or None
    loc = (posting.get("locationsText") or "").strip() or None
    bullets = [b for b in (posting.get("bulletFields") or []) if isinstance(b, str) and b.strip()]
    md: dict = {}
    if bullets:
        md["bulletFields"] = bullets
        # bulletFields[0] is almost always the requisition id (JR..., R-...)
        md["requisition_id"] = bullets[0]
    return DiscoveredJob(
        url=url,
        title=title,
        locations=[loc] if loc else None,
        date_posted=(posting.get("postedOn") or None),
        metadata=md or None,
    )


async def discover_workday(board: dict, client: httpx.AsyncClient) -> list[DiscoveredJob]:
    tenant, dc, site = _cfg(board)
    api = f"{_base(tenant, dc)}/wday/cxs/{tenant}/{site}/jobs"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    out: list[DiscoveredJob] = []
    seen: set[str] = set()
    total = None
    max_pages = max(1, min(_MAX_PAGES, settings.workday_max_pages))
    for page in range(max_pages):
        offset = page * _PAGE
        if total is not None and offset >= total:
            break
        resp = await _post(
            client, api,
            {"limit": _PAGE, "offset": offset, "searchText": "", "appliedFacets": {}},
            headers,
        )
        resp.raise_for_status()
        data = resp.json()
        if total is None:
            total = int(data.get("total") or 0)
        postings = data.get("jobPostings") or []
        if not postings:
            break
        for p in postings:
            job = _parse_posting(p, tenant, dc, site)
            if job and job.url not in seen:
                seen.add(job.url)
                out.append(job)
        # Results come newest-first: once a whole page is at/over the posting
        # window (MAX_POSTING_AGE_DAYS, default 15) the rest is outside it too
        # (ingest drops those anyway), so stop paging.
        window = int(settings.max_posting_age_days)
        ages = [_age_days(p.get("postedOn")) for p in postings]
        if all(a is not None and a >= window for a in ages):
            break
        if len(postings) < _PAGE:
            break
        await asyncio.sleep(0)  # cooperative; politeness gate handles real spacing
    log.info("workday.discovered", board=board.get("board_slug"), total=total, kept=len(out))
    return out
