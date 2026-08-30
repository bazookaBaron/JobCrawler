"""seniority_of(title) -> one of intern|junior|mid|senior|staff|principal|unknown.

Derived from the job title at ingest and stored in job_posting.seniority.
Checked most-specific first so "Senior Staff Engineer" -> principal and
"New Grad SWE Intern" -> intern.
"""
from __future__ import annotations

import re

BUCKETS = ("intern", "junior", "mid", "senior", "staff", "principal", "unknown")

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


def seniority_of(title: str | None) -> str:
    if not title:
        return "unknown"
    # pad so " i "/" ii "/" iii " match trailing roman numerals: "Engineer II"
    t = f" {title} "
    for bucket, pats in _COMPILED:
        if any(p.search(t) for p in pats):
            return bucket
    return "unknown"
