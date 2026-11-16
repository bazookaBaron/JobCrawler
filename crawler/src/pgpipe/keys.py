"""Pure-Python reference of the SQL key functions in schema.sql
(`{schema}.norm_company_key`, `norm_title`, `norm_city`, `compute_dedupe_key`).
The DB trigger is authoritative; this mirror exists so the rules are unit-tested
and so the LinkedIn crawler can compute the same keys client-side.
"""
from __future__ import annotations

import hashlib
import re

_SUFFIX_RE = re.compile(r"\b(inc|llc|ltd|pvt|private|limited|technologies|corp)\b")
_NON_ALNUM = re.compile(r"[^a-z0-9]")
_NON_ALNUM_RUN = re.compile(r"[^a-z0-9]+")


def norm_company_key(name: str | None) -> str:
    if not name:
        return ""
    low = name.lower()
    stripped = _NON_ALNUM.sub("", _SUFFIX_RE.sub("", low))
    return stripped or _NON_ALNUM.sub("", low)


def norm_title(title: str | None) -> str:
    return _NON_ALNUM_RUN.sub(" ", (title or "").lower()).strip()


def norm_city(location: str | None) -> str:
    return _NON_ALNUM.sub("", (location or "").split(",", 1)[0].lower())


def dedupe_key(company_key: str, title: str | None, location: str | None) -> str:
    raw = f"{company_key}|{norm_title(title)}|{norm_city(location)}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()
