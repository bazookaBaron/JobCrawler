"""Compact, *rich* iCIMS listing monitor (title + location + posted date).

jobseek's src/core/monitors/icims.py is URL-only (it leaves detail extraction
to a scraper we don't run), so it would yield no rows here. This one reads the
public server-rendered listing  https://{host}/jobs/search?ss=1&in_iframe=1&pr=N
(no login, no JS, no captcha) and takes everything from the job cards:

    <li class="iCIMS_JobCardItem"> ... Job Locations ... Posted Date ...
        <a href=".../jobs/485240/regional-account-executive/job"><h3>Title</h3>

board monitor_config: {"host": "<tenant>.icims.com"} (falls back to board_url).
Older table-style tenants (iCIMS_JobListingRow) are parsed with the same
label-based extraction; where a card carries no location/date they stay None.
"""
from __future__ import annotations

import html as html_mod
import re
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse

import httpx
import structlog
from dateutil import parser as _dparser

from src.core.monitors import BoardGoneError, DiscoveredJob
from src.pgpipe.config import settings
from src.pgpipe.posted_at import parse_posted_at

log = structlog.get_logger()

_HOST_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.icims\.com$", re.I)
_RESERVED = {"api.icims.com", "app.icims.com", "help.icims.com", "support.icims.com", "www.icims.com"}
_LINK_RE = re.compile(r'href="(https?://[^"\s]+?/jobs/(\d+)/([^/"?\s]*)/job)[^"]*"', re.I)
_PAGE_OF_RE = re.compile(r"Page\s+(\d+)\s+of\s+(\d+)", re.I)
_TAG_RE = re.compile(r"<[^>]+>")
_STATE_COUNTRY = {"US": "United States", "IN": "India"}
_LOC_CODE_RE = re.compile(r"^([A-Z]{2})-(?:([A-Za-z0-9]{2,3})-)?(.+)$")


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", html_mod.unescape(_TAG_RE.sub(" ", s))).strip()


def normalize_location(raw: str) -> list[str]:
    """iCIMS writes 'US-TX-Austin | IN-KA-Bangalore'. Rewrite US-/IN- prefixed
    parts into 'Austin, TX, United States' / 'Bangalore, India' so the offline
    country classifier sees them; leave other formats untouched."""
    out: list[str] = []
    for part in re.split(r"\s*\|\s*|\s*;\s*", raw or ""):
        part = part.strip()
        if not part:
            continue
        m = _LOC_CODE_RE.match(part)
        if m and m.group(1) in _STATE_COUNTRY:
            cc, state, city = m.group(1), m.group(2), m.group(3).strip()
            bits = [city] + ([state] if state and cc == "US" else []) + [_STATE_COUNTRY[cc]]
            out.append(", ".join(bits))
        else:
            out.append(part)
    return out


def host_of(board: dict) -> str | None:
    md = board.get("metadata") or {}
    for cand in (md.get("host"), urlparse(board.get("board_url") or "").hostname):
        h = (cand or "").strip().lower()
        if _HOST_RE.match(h) and h not in _RESERVED:
            return h
    return None


def _after_label(chunk: str, label: str) -> str | None:
    m = re.search(rf"{label}[^<]*</span>\s*<span([^>]*)>(.*?)</span>", chunk, re.I | re.S)
    if not m:
        return None
    return m.group(1) + "\x00" + m.group(2)


def _posted(chunk: str) -> str | None:
    raw = _after_label(chunk, r"Posted\s*Date")
    if not raw:
        return None
    attrs, inner = raw.split("\x00", 1)
    rel = parse_posted_at(_clean(inner.split("<span", 1)[0]))
    if rel is not None:
        return rel.date().isoformat()
    t = re.search(r'title="([^"]+)"', attrs)
    if t:
        txt = t.group(1)
        try:
            dt = _dparser.parse(txt, dayfirst="." in txt.split(" ")[0])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.date().isoformat()
        except (ValueError, OverflowError):
            return None
    return None


def parse_listing(page: str, host: str) -> list[DiscoveredJob]:
    """Parse one listing page into DiscoveredJobs (deduped by job id)."""
    marker = "iCIMS_JobCardItem" if "iCIMS_JobCardItem" in page else "iCIMS_JobListingRow"
    chunks = page.split(marker)[1:] if marker in page else [page]
    jobs: dict[str, DiscoveredJob] = {}
    for ch in chunks:
        m = _LINK_RE.search(ch)
        if not m:
            continue
        url, jid, slug = m.group(1), m.group(2), m.group(3)
        if jid in jobs:
            continue
        h3 = re.search(r"<h3[^>]*>(.*?)</h3>", ch, re.S)
        title = _clean(h3.group(1)) if h3 else unquote(slug).replace("-", " ").strip()
        loc_raw = _after_label(ch, r"Job\s*Locations?")
        locs = normalize_location(_clean(loc_raw.split("\x00", 1)[1])) if loc_raw else []
        jobs[jid] = DiscoveredJob(
            url=url,
            title=title or None,
            locations=locs or None,
            date_posted=_posted(ch),
            metadata={"requisition_id": jid},
        )
    return list(jobs.values())


def _has_next(page: str, pr: int) -> bool:
    m = _PAGE_OF_RE.search(page)
    if m:
        return int(m.group(1)) < int(m.group(2))
    return f"pr={pr + 1}" in page


async def discover_icims(board: dict, client: httpx.AsyncClient) -> list[DiscoveredJob]:
    host = host_of(board)
    if not host:
        raise ValueError(f"icims board {board.get('board_slug')!r}: no valid *.icims.com host")
    oldest = datetime.now(timezone.utc).timestamp() - settings.max_posting_age_days * 86400
    out: dict[str, DiscoveredJob] = {}
    for pr in range(max(1, settings.icims_max_pages)):
        resp = await client.get(
            f"https://{host}/jobs/search", params={"ss": 1, "in_iframe": 1, "pr": pr}
        )
        if resp.status_code in (404, 410):
            raise BoardGoneError(f"iCIMS host {host} returned {resp.status_code}")
        resp.raise_for_status()
        page = resp.text
        jobs = parse_listing(page, host)
        if not jobs:
            break
        for j in jobs:
            out.setdefault(j.url, j)
        dated = [j for j in jobs if j.date_posted]
        # newest-first listing: a full page of too-old postings ends the crawl
        if dated and len(dated) == len(jobs) and all(
            parse_posted_at(j.date_posted).timestamp() < oldest for j in dated  # type: ignore[union-attr]
        ):
            break
        if not _has_next(page, pr):
            break
    log.info("icims.discovered", host=host, jobs=len(out))
    return list(out.values())
