"""DiscoveredJob -> one flat row of report attributes (in memory only).

The description is read here and thrown away: only the derived signals
(salary, years of experience, technologies, education, visa, ...) survive
into the shard parquet. No posting text or URL leaves the runner.
"""

from __future__ import annotations

import hashlib
import html
import re
from datetime import UTC, date

from src.core.enum_normalize import _EMPLOYMENT_TYPE_MAP, _JOB_LOCATION_TYPE_MAP
from src.core.experience_extract import extract_experience
from src.core.monitors import DiscoveredJob
from src.core.technology_resolve import match_technologies
from src.pgpipe.posted_at import parse_posted_at
from src.report import classify, geo, salary

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_EPOCH = date(1970, 1, 1)

# Columns of the shard parquet, in order. Keep in sync with row().
COLUMNS = (
    "job_key",
    "company_slug",
    "board_slug",
    "source",
    "title_clean",
    "profile",
    "job_family",
    "seniority",
    "employment_type",
    "work_mode",
    "country",
    "region",
    "city",
    "n_locations",
    "n_countries",
    "posted_day",
    "sal_min",
    "sal_max",
    "sal_currency",
    "sal_unit",
    "sal_source",
    "yoe_min",
    "technologies",
    "education",
    "visa",
    "equity",
    "clearance",
    "language",
    "desc_len",
)


def job_key(company_slug: str, job: DiscoveredJob) -> int:
    """Stable signed int64 identity: company + provider id (or URL)."""
    ident = job.source_identity or (job.metadata or {}).get("requisition_id") or job.url
    digest = hashlib.blake2b(f"{company_slug}|{ident}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)


def _text(description: str | None) -> str:
    if not description:
        return ""
    return _WS_RE.sub(" ", html.unescape(_TAG_RE.sub(" ", description))).strip()


def _employment(raw: str | None) -> str:
    if not raw:
        return "unspecified"
    return _EMPLOYMENT_TYPE_MAP.get(raw.strip().lower(), "other")


def _language(job: DiscoveredJob, title: str, text: str) -> str:
    if job.language:
        return job.language.lower()[:2]
    sample = f"{title}. {text[:400]}".strip()
    if len(sample) < 20:
        return "unknown"
    try:
        from fast_langdetect import detect

        res = detect(sample.replace("\n", " "), model="lite")
        if isinstance(res, list):
            res = res[0] if res else {}
        return str((res or {}).get("lang") or "unknown")
    except Exception:  # noqa: BLE001 - language is a nice-to-have
        return "unknown"


def _work_mode(job: DiscoveredJob, places: list[geo.Place]) -> str:
    raw = (job.job_location_type or "").strip().lower()
    mapped = _JOB_LOCATION_TYPE_MAP.get(raw) or _JOB_LOCATION_TYPE_MAP.get(raw.split(" (", 1)[0])
    if mapped in ("remote", "hybrid", "onsite"):
        return mapped
    if any(p.hybrid for p in places):
        return "hybrid"
    if any(p.remote for p in places):
        return "remote"
    if any(p.onsite for p in places):
        return "onsite"
    return "unspecified"


def _posted_day(raw: str | None) -> int | None:
    dt = parse_posted_at(raw)
    if dt is None:
        return None
    return (dt.astimezone(UTC).date() - _EPOCH).days


def row(company_slug: str, board_slug: str, source: str, job: DiscoveredJob) -> dict | None:
    if not job.url:
        return None
    title = (job.title or "").strip()
    text = _text(job.description)
    places = [geo.resolve(loc) for loc in (job.locations or [])[:50]]
    primary = next((p for p in places if p.country), places[0] if places else None)
    countries = {p.country for p in places if p.country}

    profile, family = classify.classify_profile(title)
    sal_min, sal_max, sal_cur, sal_unit, sal_src = salary.normalize(
        job.base_salary if isinstance(job.base_salary, dict) else None, job.description
    )
    exp = extract_experience(job.description) if job.description else None
    techs = match_technologies(f"{title}\n{job.description or ''}")

    return {
        "job_key": job_key(company_slug, job),
        "company_slug": company_slug,
        "board_slug": board_slug,
        "source": source,
        "title_clean": classify.clean_title(title),
        "profile": profile,
        "job_family": family,
        "seniority": classify.classify_seniority(title),
        "employment_type": _employment(job.employment_type),
        "work_mode": _work_mode(job, places),
        "country": primary.country if primary else None,
        "region": primary.region if primary else None,
        "city": primary.city if primary else None,
        "n_locations": len(places),
        "n_countries": len(countries),
        "posted_day": _posted_day(job.date_posted),
        "sal_min": sal_min,
        "sal_max": sal_max,
        "sal_currency": sal_cur,
        "sal_unit": sal_unit,
        "sal_source": sal_src,
        "yoe_min": float(exp.min_years) if exp else None,
        "technologies": sorted(set(techs)),
        "education": classify.classify_education(text) if text else "none_stated",
        "visa": classify.classify_visa(text) if text else "not_mentioned",
        "equity": classify.has_equity(text) if text else False,
        "clearance": classify.needs_clearance(text) if text else False,
        "language": _language(job, title, text),
        "desc_len": len(text),
    }
