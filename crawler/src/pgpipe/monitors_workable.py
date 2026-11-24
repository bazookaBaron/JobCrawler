"""Compact, *rich* Workable monitor — jobseek's src/core/monitors/workable.py is URL-only
(registered rich=False), so in this pipeline it would store no rows.

One public call per board returns every published job with title, locations, dates,
department and description:
    GET https://apply.workable.com/api/v1/widget/accounts/{slug}?details=true
(apply.workable.com/robots.txt allows all paths.)

board monitor_config: {"token": "<account slug>"}  (falls back to the board URL
https://apply.workable.com/<slug>/).  Job URLs match jobseek's workable monitor:
https://apply.workable.com/{slug}/j/{shortcode}/
"""
from __future__ import annotations

import re

import httpx

from src.core.monitors import BoardGoneError, DiscoveredJob

_URL_RE = re.compile(r"apply\.workable\.com/(?!api/|j/)([\w-]+)", re.I)
_MAX_JOBS = 5000
_TENANT_OK = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


def workable_slug(board_url: str, metadata: dict | None) -> str | None:
    tok = (metadata or {}).get("token") or (metadata or {}).get("slug")
    if isinstance(tok, str) and tok.strip():
        return tok.strip()
    m = _URL_RE.search(board_url or "")
    return m.group(1) if m else None


def _loc(d: dict) -> str | None:
    parts = [d.get("city"), d.get("region") or d.get("state"), d.get("country")]
    seen, out = set(), []
    for p in parts:
        p = (p or "").strip()
        if p and p.lower() not in seen:
            seen.add(p.lower())
            out.append(p)
    return ", ".join(out) or None


async def discover_workable(board: dict, client: httpx.AsyncClient) -> list[DiscoveredJob]:
    slug = workable_slug(board.get("board_url", ""), board.get("metadata"))
    if not slug:
        raise ValueError(f"Cannot derive Workable account from {board.get('board_url')!r}")
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}?details=true"
    resp = await client.get(url, headers={"accept": "application/json"})
    if resp.status_code in (404, 410):
        raise BoardGoneError(f"Workable account {slug} returned {resp.status_code}", url=url,
                             status_code=resp.status_code)
    resp.raise_for_status()
    data = resp.json()
    jobs = data.get("jobs") or []
    out: list[DiscoveredJob] = []
    for j in jobs[:_MAX_JOBS]:
        code = j.get("shortcode")
        if not code:
            continue
        locs = [x for x in (_loc(l) for l in (j.get("locations") or [])) if x]
        if not locs:
            one = _loc({"city": j.get("city"), "region": j.get("state"), "country": j.get("country")})
            locs = [one] if one else []
        meta = {"department": j.get("department") or None, "industry": j.get("industry") or None,
                "function": j.get("function") or None, "company_name": data.get("name")}
        out.append(DiscoveredJob(
            url=f"https://apply.workable.com/{slug}/j/{code}/",
            title=(j.get("title") or "").strip() or None,
            description=j.get("description") or None,
            locations=locs or None,
            employment_type=j.get("employment_type") or None,
            job_location_type="remote" if j.get("telecommuting") else None,
            date_posted=j.get("published_on") or j.get("created_at") or None,
            metadata={k: v for k, v in meta.items() if v},
            source_identity=f"workable:{slug.lower()}:{code}" if _TENANT_OK.match(slug.lower()) else None,
        ))
    return out
