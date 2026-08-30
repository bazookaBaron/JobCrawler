"""seniority_of(title) -> one of intern|junior|mid|senior|staff|principal|unknown.

Derived from the job title at ingest and stored in job_posting.seniority.

Rules, checked most-specific first (first hit wins):
  intern    : intern, internship, co-op, apprentice
  principal : principal, distinguished, fellow, architect, "senior staff", L7/L8
  staff     : staff, L6
  senior    : senior, sr, " III ", L5, "SDE/SWE 3", "SDE/SWE III"
  mid       : " II ", "level 2", L4, "SDE/SWE 2", "SDE/SWE II", "mid-level"
  junior    : junior, jr, new grad, early career, associate, entry-level,
              graduate, " I ", "level 1", L3, "SDE/SWE 1", "SDE/SWE I"

Then, when IC_DEFAULT_MID is True (see below), a title that matched NONE of the
buckets above but reads as an individual-contributor engineering/dev role
(and is NOT a manager/director/head/lead title) defaults to `mid` instead of
`unknown` -- the common convention for an unqualified "Software Engineer" is
~2-4 YoE. Genuinely ambiguous non-IC titles still fall to `unknown`.
"""
from __future__ import annotations

import re

from src.pgpipe.tech_filter import is_tech_role

BUCKETS = ("intern", "junior", "mid", "senior", "staff", "principal", "unknown")

# Set to False to revert to strict "unqualified IC title -> unknown".
IC_DEFAULT_MID = True

# order matters: first hit wins
_RULES: list[tuple[str, list[str]]] = [
    ("intern", [r"\bintern(ship)?\b", r"\bco-?op\b", r"\bapprentice\b"]),
    ("principal", [
        r"\bprincipal\b", r"\bdistinguished\b", r"\bfellow\b",
        r"\bl[78]\b", r"senior staff", r"\barchitect\b",
    ]),
    ("staff", [r"\bstaff\b", r"\bl6\b"]),
    ("senior", [
        r"\bsenior\b", r"\bsr\.?\b", r" iii ", r"\bl5\b",
        r"\bsde\s*(3|iii)\b", r"\bswe\s*(3|iii)\b",
    ]),
    ("mid", [
        r" ii ", r"level 2", r"\bl4\b", r"\bsde\s*(2|ii)\b",
        r"\bswe\s*(2|ii)\b", r"mid[- ]level",
    ]),
    ("junior", [
        r"\bjunior\b", r"\bjr\.?\b", r"new grad", r"early career",
        r"\bassociate\b", r"entry[- ]level", r"\bgraduate\b",
        r" i ", r"level 1", r"\bl3\b",
        r"\bsde\s*(1|i)\b", r"\bswe\s*(1|i)\b",
    ]),
]

_COMPILED = [(b, [re.compile(p, re.I) for p in ps]) for b, ps in _RULES]

# Management / lead titles are excluded from the IC->mid default (stay unknown).
_MGMT_RE = re.compile(r"\b(manager|director|head|lead|vp|chief|officer)\b", re.I)
# IC engineering / dev / science signal (in addition to the tech-filter ALLOW set).
_IC_RE = re.compile(
    r"\b(engineer|engineering|developer|programmer|sde|swe|scientist)\b", re.I
)


def _looks_ic_engineering(title: str) -> bool:
    return not _MGMT_RE.search(title) and (is_tech_role(title) or bool(_IC_RE.search(title)))


def seniority_of(title: str | None) -> str:
    if not title:
        return "unknown"
    # pad so " i "/" ii "/" iii " match trailing roman numerals: "Engineer II"
    t = f" {title} "
    for bucket, pats in _COMPILED:
        if any(p.search(t) for p in pats):
            return bucket
    if IC_DEFAULT_MID and _looks_ic_engineering(title):
        return "mid"
    return "unknown"
