"""Import another crawler's board list (AutoApply's discovery.db + JSON career-
site lists) into <schema>.job_board so the daily crawl covers the union.

Public career-site identifiers only (ATS tenant slugs / hosts / feed URLs).
Each source row is mapped to the (monitor_type, board_url, monitor_config)
triple the existing monitors already understand:

    greenhouse/ashby/lever   board_id = token          -> {"token": id}
    workday                  board_id = host/site      -> {"company","wd_instance","site"}
    icims                    board_id = <tenant>.icims.com -> {"host": host}
    successfactors           board_id = .../googlefeed.xml -> rss preset successfactors

Existing rows are never touched: inserts use ON CONFLICT DO NOTHING on the
primary key (board_slug), and a board whose ATS identity (e.g. greenhouse token)
is already present under any slug is skipped, so no board is crawled twice.
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import asyncpg

ORIGIN = "autoapply"
SUPPORTED = ("greenhouse", "ashby", "lever", "workday", "icims", "successfactors")

_WD_HOST_RE = re.compile(r"^(?P<tenant>[^./]+)\.(?P<dc>wd\d+)\.myworkdayjobs\.com$", re.I)
_SF_GENERIC_SUBS = {
    "www", "careers", "career", "jobs", "job", "apply", "apply-now", "applynow", "talent",
    "join", "work", "recruiting", "recruit", "hiring", "employment", "external", "emea",
}
_SF_HOSTING = ("jobs2web.com", "successfactors.com", "successfactors.eu", "sapsf.com", "sapsf.eu")
_ICIMS_PREFIX_RE = re.compile(
    r"^(?:[a-z]{0,10}careers?|jobs?|external|internal|employees?|apply|talent|join|work|global|us|uk|ca|can|in)-"
)
_ICIMS_RESERVED = {"www.icims.com", "api.icims.com", "app.icims.com", "help.icims.com", "support.icims.com"}


def slugify(text: str, maxlen: int = 60) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return s[:maxlen].strip("-")


def _titleize(slug: str) -> str:
    return " ".join(w.capitalize() for w in slug.split("-") if w) or slug


def _sf_feed(board_id: str) -> tuple[str, str] | None:
    raw = (board_id or "").strip()
    if not raw:
        return None
    if not re.match(r"^https?://", raw, re.I):
        raw = f"https://{raw.strip('/')}/googlefeed.xml"
    u = urlparse(raw)
    host = (u.hostname or "").lower()
    if not host:
        return None
    return host, f"{u.scheme}://{u.netloc}{u.path or '/googlefeed.xml'}"


def _company_from_host(host: str, ats: str) -> str:
    host = host.lower()
    if ats == "icims":
        stripped = host.removesuffix(".icims.com")
        while True:
            nxt = _ICIMS_PREFIX_RE.sub("", stripped, count=1)
            if nxt == stripped or len(nxt) < 3:
                break
            stripped = nxt
        return slugify(stripped)
    labels = host.split(".")
    if any(host.endswith(h) for h in _SF_HOSTING) or len(labels) < 2:
        first = labels[0]
        return slugify(f"sf-{first}" if first.isdigit() else first)
    core = [lab for lab in labels[:-1] if lab not in _SF_GENERIC_SUBS]
    return slugify(core[-1] if core else labels[0])


def map_board(ats: str, board_id: str, company: str | None = None) -> dict | None:
    """AutoApply (ats, board_id[, company]) -> job_board row dict, or None when
    the ATS is unsupported / the id is unusable. Keys: identity, board_slug,
    company_slug, company_name, board_url, monitor_type, monitor_config."""
    ats = (ats or "").strip().lower()
    bid = (board_id or "").strip()
    if ats not in SUPPORTED or not bid:
        return None

    if ats in ("greenhouse", "ashby", "lever"):
        token = bid.strip("/")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", token):
            return None
        url = {
            "greenhouse": f"https://job-boards.greenhouse.io/{token}",
            "ashby": f"https://jobs.ashbyhq.com/{token}",
            "lever": f"https://jobs.lever.co/{token}",
        }[ats]
        cfg = {"token": token}
        identity = f"{ats}:{token.lower()}"
        derived = slugify(token)
    elif ats == "workday":
        host, _, site = bid.partition("/")
        host = host.strip().lower()
        site = site.strip("/").split("/")[0]
        m = _WD_HOST_RE.match(host)
        if not (m and site):
            return None
        tenant, dc = m.group("tenant"), m.group("dc")
        url = f"https://{host}/{site}"
        cfg = {"company": tenant, "wd_instance": dc, "site": site}
        identity = f"workday:{tenant}.{dc}/{site.lower()}"
        derived = slugify(tenant)
    elif ats == "icims":
        host = bid.lower().removeprefix("https://").removeprefix("http://").split("/")[0]
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]*\.icims\.com", host) or host in _ICIMS_RESERVED:
            return None
        url = f"https://{host}/jobs/search?ss=1&in_iframe=1"
        cfg = {"host": host}
        identity = f"icims:{host}"
        derived = _company_from_host(host, "icims")
    else:  # successfactors
        feed = _sf_feed(bid)
        if not feed:
            return None
        host, feed_url = feed
        url = f"https://{host}/"
        cfg = {"preset": "successfactors", "feed_url": feed_url}
        identity = f"successfactors:{host}"
        derived = _company_from_host(host, "successfactors")

    company_slug = slugify(company or "") or derived
    if not company_slug:
        return None
    monitor_type = "rss" if ats == "successfactors" else ats
    board_slug = "aa-" + slugify(re.sub(r"[:/.]", "-", identity), 90)
    return {
        "identity": identity,
        "board_slug": board_slug,
        "company_slug": company_slug,
        "company_name": (company or "").strip() or _titleize(company_slug),
        "board_url": url,
        "monitor_type": monitor_type,
        "monitor_config": cfg,
    }


def identity_of_existing(monitor_type: str, board_url: str, cfg: dict | None) -> str | None:
    """Same identity key for rows already in job_board (so duplicates are skipped)."""
    cfg = cfg or {}
    url = board_url or ""
    if monitor_type in ("greenhouse", "ashby", "lever"):
        token = (cfg.get("token") or "").strip()
        if not token:
            parts = [p for p in urlparse(url).path.split("/") if p]
            token = parts[0] if parts else ""
        return f"{monitor_type}:{token.lower()}" if token else None
    if monitor_type == "workday":
        from src.pgpipe.monitors_workday import _cfg

        try:
            tenant, dc, site = _cfg({"metadata": cfg, "board_url": url})
        except ValueError:
            return None
        return f"workday:{tenant.lower()}.{dc.lower()}/{site.lower()}"
    if monitor_type == "icims":
        h = (cfg.get("host") or urlparse(url).hostname or "").lower()
        return f"icims:{h}" if h else None
    if monitor_type == "rss" and cfg.get("preset") == "successfactors":
        feed = _sf_feed(cfg.get("feed_url") or "")
        return f"successfactors:{feed[0]}" if feed else None
    return None


# --- sources -----------------------------------------------------------------

def load_sqlite(path: str | Path, statuses: tuple[str, ...] = ("ok",)) -> list[dict]:
    con = sqlite3.connect(f"file:{Path(path).as_posix()}?mode=ro", uri=True)
    try:
        q = ",".join("?" for _ in statuses)
        rows = con.execute(
            f"SELECT ats, board_id, company FROM boards WHERE status IN ({q})", statuses
        ).fetchall()
    finally:
        con.close()
    return [{"ats": a, "board_id": b, "company": c} for a, b, c in rows]


def load_json_dir(directory: str | Path) -> list[dict]:
    """companies.json / top_companies.json / watchlist.json: lists keyed by ATS
    (greenhouse|lever|ashby: tokens; workday: {name,host,site}; icims: hosts;
    successfactors: feed URLs). Other keys (eightfold, smartrecruiters, ...)
    are not supported by the crawler and are ignored."""
    out: list[dict] = []
    for name in ("companies.json", "top_companies.json", "watchlist.json"):
        p = Path(directory) / name
        if not p.exists():
            continue
        data = json.loads(p.read_text(encoding="utf-8"))
        for ats in SUPPORTED:
            for item in data.get(ats) or []:
                if isinstance(item, str):
                    out.append({"ats": ats, "board_id": item, "company": None})
                elif isinstance(item, dict) and ats == "workday" and item.get("host") and item.get("site"):
                    out.append({"ats": ats, "board_id": f"{item['host']}/{item['site']}", "company": None})
    return out


def map_all(sources: list[dict]) -> tuple[list[dict], Counter]:
    """Map + dedupe (first occurrence wins; put richer sources first)."""
    seen: set[str] = set()
    slugs: set[str] = set()
    out: list[dict] = []
    skipped: Counter = Counter()
    for s in sources:
        m = map_board(s["ats"], s["board_id"], s.get("company"))
        if m is None:
            skipped[f"unmappable:{s['ats']}"] += 1
            continue
        if m["identity"] in seen or m["board_slug"] in slugs:
            skipped["dup_in_input"] += 1
            continue
        seen.add(m["identity"])
        slugs.add(m["board_slug"])
        out.append(m)
    return out, skipped


# --- DB ----------------------------------------------------------------------

async def _counts(conn: asyncpg.Connection) -> dict[str, dict[str, int]]:
    rows = await conn.fetch(
        "SELECT monitor_type, count(*) AS total, count(*) FILTER (WHERE is_enabled) AS enabled "
        "FROM job_board GROUP BY 1 ORDER BY 2 DESC"
    )
    return {r["monitor_type"]: {"total": r["total"], "enabled": r["enabled"]} for r in rows}


async def import_boards(pool: asyncpg.Pool, mapped: list[dict], dry_run: bool = False) -> dict:
    async with pool.acquire() as conn:
        before = await _counts(conn)
        existing = await conn.fetch("SELECT monitor_type, board_url, monitor_config FROM job_board")
        have: set[str] = set()
        for r in existing:
            cfg = r["monitor_config"]
            if isinstance(cfg, str):
                cfg = json.loads(cfg or "{}")
            ident = identity_of_existing(r["monitor_type"], r["board_url"], cfg)
            if ident:
                have.add(ident)
        todo = [m for m in mapped if m["identity"] not in have]
        skipped_existing = len(mapped) - len(todo)
        if not dry_run and todo:
            async with conn.transaction():
                await conn.executemany(
                    "INSERT INTO company (slug, name, updated_at) VALUES ($1, $2, now()) "
                    "ON CONFLICT (slug) DO NOTHING",
                    list({m["company_slug"]: (m["company_slug"], m["company_name"]) for m in todo}.values()),
                )
                await conn.executemany(
                    "INSERT INTO job_board (board_slug, company_slug, board_url, monitor_type, "
                    "monitor_config, is_enabled, origin, updated_at) "
                    "VALUES ($1, $2, $3, $4, $5::jsonb, true, $6, now()) "
                    "ON CONFLICT (board_slug) DO NOTHING",
                    [
                        (m["board_slug"], m["company_slug"], m["board_url"], m["monitor_type"],
                         json.dumps(m["monitor_config"]), ORIGIN)
                        for m in todo
                    ],
                )
        after = await _counts(conn)
    return {
        "dry_run": dry_run,
        "mapped": len(mapped),
        "skipped_already_present": skipped_existing,
        "to_insert": len(todo),
        "to_insert_by_monitor_type": dict(Counter(m["monitor_type"] for m in todo)),
        "before": before,
        "after": after,
    }
