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

log = structlog.get_logger()

_PAGE = 20
_MAX_POSTINGS = 2000
_MAX_PAGES = _MAX_POSTINGS // _PAGE

# https://{tenant}.{dc}.myworkdayjobs.com/[locale/]{site}[/...]
_URL_RE = re.compile(
    r"https?://(?P<tenant>[^./]+)\.(?P<dc>wd\d+)\.myworkdayjobs\.com/"
    r"(?:(?P<locale>[a-z]{2}-[A-Za-z]{2})/)?(?P<site>[^/?#]+)",
    re.I,
)


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
    for page in range(_MAX_PAGES):
        offset = page * _PAGE
        if total is not None and offset >= total:
            break
        resp = await client.post(
            api,
            json={"limit": _PAGE, "offset": offset, "searchText": "", "appliedFacets": {}},
            headers=headers,
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
        if len(postings) < _PAGE:
            break
        await asyncio.sleep(0)  # cooperative; politeness gate handles real spacing
    log.info("workday.discovered", board=board.get("board_slug"), total=total, kept=len(out))
    return out
