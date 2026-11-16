"""Merge AutoJobApplier's cloud_hunter/companies.json into
crawler/data/boards.csv + crawler/data/companies.csv.

    python tools/import_aja_boards.py --aja /path/to/AutoJobApplier/cloud_hunter/companies.json
    python tools/import_aja_boards.py --aja ... --dry-run          # counts only
    python tools/import_aja_boards.py --aja ... --only greenhouse,lever

Idempotent: boards are de-duplicated by (ats, token) against what is already in
boards.csv (and against each other), so re-running adds nothing new. Existing
rows are never modified or reordered; new rows are appended.

Supported (rich HTTP monitors the pgpipe pipeline can store): greenhouse,
ashby, lever, workday. Skipped, with counts reported:
  * icims          - the registered monitor is URL-only (no title/location), so
                     pgpipe would store nothing useful;
  * successfactors - no monitor registered in this fork (only a legacy stub).

NOTE: tools/trim_csvs.py regenerates both CSVs from an upstream checkout and
would drop these additions; re-run this script afterwards.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "crawler" / "data"

SUPPORTED = ("greenhouse", "ashby", "lever", "workday")
UNSUPPORTED = ("icims", "successfactors")

URL_TEMPLATES = {
    "greenhouse": "https://job-boards.greenhouse.io/{token}",
    "ashby": "https://jobs.ashbyhq.com/{token}",
    "lever": "https://jobs.lever.co/{token}",
}
_TOKEN_FROM_URL = {
    "greenhouse": re.compile(r"greenhouse\.io/(?:embed/job_board\?for=)?([\w.-]+)", re.I),
    "ashby": re.compile(r"ashbyhq\.com/([\w.%-]+)", re.I),
    "lever": re.compile(r"lever\.co/([\w.-]+)", re.I),
}
_WD_RE = re.compile(
    r"https?://(?P<tenant>[^./]+)\.(?P<dc>wd\d+)\.myworkdayjobs\.com/"
    r"(?:(?P<locale>[a-z]{2}-[A-Za-z]{2})/)?(?P<site>[^/?#]+)", re.I)

BOARD_FIELDS = ["company_slug", "board_slug", "board_url", "monitor_type",
                "monitor_config", "scraper_type", "scraper_config"]
COMPANY_FIELDS = ["slug", "name", "website", "logo_url", "icon_url", "logo_type", "industry",
                  "employee_count_range", "founded_year", "extras", "company_type"]


def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def pretty_name(token: str) -> str:
    words = re.sub(r"[-_.]+", " ", token).strip()
    return words.title() if words else token


def read_csv(path: Path) -> tuple[list[str], list[dict]]:
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        return list(rd.fieldnames or []), list(rd)


def existing_keys(boards: list[dict]) -> set[tuple]:
    keys: set[tuple] = set()
    for b in boards:
        mt = (b.get("monitor_type") or "").lower()
        try:
            cfg = json.loads(b.get("monitor_config") or "{}")
        except json.JSONDecodeError:
            cfg = {}
        url = b.get("board_url") or ""
        if mt in _TOKEN_FROM_URL:
            tok = cfg.get("token") or cfg.get("slug")
            if not tok:
                m = _TOKEN_FROM_URL[mt].search(url)
                tok = m.group(1) if m else None
            if tok:
                keys.add((mt, tok.lower()))
        elif mt == "workday":
            keys.add(("workday",) + _wd_key(cfg, url))
    return keys


def _wd_key(cfg: dict, url: str) -> tuple:
    tenant = (cfg.get("company") or cfg.get("tenant") or "").lower()
    dc = (cfg.get("wd_instance") or cfg.get("dc") or "").lower()
    site = (cfg.get("site") or cfg.get("board") or cfg.get("site_id") or "").lower()
    m = _WD_RE.match(url or "")
    if m:
        tenant = tenant or m.group("tenant").lower()
        dc = dc or m.group("dc").lower()
        site = site or m.group("site").lower()
    return (tenant, dc, site)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--aja", required=True, type=Path, help="path to cloud_hunter/companies.json")
    ap.add_argument("--data", type=Path, default=DATA, help="dir holding boards.csv + companies.csv")
    ap.add_argument("--only", default=",".join(SUPPORTED),
                    help="comma list of ATS to import (default: all supported)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    only = {s.strip() for s in args.only.split(",") if s.strip()}
    bad = only - set(SUPPORTED)
    if bad:
        ap.error(f"unsupported ATS requested: {sorted(bad)} (supported: {SUPPORTED})")

    src = json.loads(args.aja.read_text(encoding="utf-8"))
    board_fields, boards = read_csv(args.data / "boards.csv")
    company_fields, companies = read_csv(args.data / "companies.csv")
    board_fields = board_fields or BOARD_FIELDS
    company_fields = company_fields or COMPANY_FIELDS

    seen = existing_keys(boards)
    company_slugs = {c["slug"] for c in companies}
    board_slugs = {b["board_slug"] for b in boards}
    new_boards: list[dict] = []
    new_companies: list[dict] = []
    stats: dict[str, dict[str, int]] = {}

    def add(ats: str, key: tuple, slug: str, name: str, url: str, cfg: dict) -> None:
        st = stats.setdefault(ats, {"in_source": 0, "added": 0, "already_present": 0})
        if key in seen:
            st["already_present"] += 1
            return
        seen.add(key)
        if slug not in company_slugs:
            company_slugs.add(slug)
            row = {f: "" for f in company_fields}
            row.update(slug=slug, name=name, company_type="unknown")
            new_companies.append(row)
        bslug = f"{slug}-{ats}"
        n = 2
        while bslug in board_slugs:
            bslug = f"{slug}-{ats}-{n}"
            n += 1
        board_slugs.add(bslug)
        row = {f: "" for f in board_fields}
        row.update(company_slug=slug, board_slug=bslug, board_url=url, monitor_type=ats,
                   monitor_config=json.dumps(cfg), scraper_type="skip")
        new_boards.append(row)
        st["added"] += 1

    for ats in ("greenhouse", "ashby", "lever"):
        if ats not in only:
            continue
        for tok in src.get(ats, []):
            tok = str(tok).strip()
            if not tok:
                continue
            stats.setdefault(ats, {"in_source": 0, "added": 0, "already_present": 0})["in_source"] += 1
            slug = slugify(tok)
            if not slug:
                continue
            add(ats, (ats, tok.lower()), slug, pretty_name(tok),
                URL_TEMPLATES[ats].format(token=tok), {"token": tok})

    if "workday" in only:
        for t in src.get("workday", []):
            host, site, name = t.get("host", ""), t.get("site", ""), t.get("name", "")
            m = re.match(r"^([^.]+)\.(wd\d+)\.myworkdayjobs\.com$", host or "", re.I)
            stats.setdefault("workday", {"in_source": 0, "added": 0, "already_present": 0})["in_source"] += 1
            if not (m and site and name):
                stats["workday"].setdefault("malformed", 0)
                stats["workday"]["malformed"] += 1
                continue
            tenant, dc = m.group(1), m.group(2)
            add("workday", ("workday", tenant.lower(), dc.lower(), site.lower()),
                slugify(name) or slugify(tenant), pretty_name(name),
                f"https://{host}/{site}", {"company": tenant, "wd_instance": dc, "site": site})

    skipped = {k: len(src.get(k, [])) for k in UNSUPPORTED}
    report = {"dry_run": args.dry_run, "per_ats": stats, "skipped_unsupported": skipped,
              "new_boards": len(new_boards), "new_companies": len(new_companies),
              "boards_total_after": len(boards) + len(new_boards),
              "companies_total_after": len(companies) + len(new_companies)}

    if not args.dry_run:
        for path, fields, rows in (
            (args.data / "boards.csv", board_fields, new_boards),
            (args.data / "companies.csv", company_fields, new_companies),
        ):
            if not rows:
                continue
            raw = path.read_bytes()
            eol = "\r\n" if b"\r\n" in raw else "\n"
            with open(path, "a", newline="", encoding="utf-8") as f:
                if raw and not raw.endswith(b"\n"):
                    f.write(eol)
                w = csv.DictWriter(f, fieldnames=fields, lineterminator=eol, extrasaction="ignore")
                w.writerows(rows)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
