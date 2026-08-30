"""is_tech_role(title, department) — keep only engineering / applied-science /
quant-dev roles. Applied in ingest.py before upsert when PGPIPE_TECH_ONLY=1
(the default).

BLOCK wins: a title that matches both an ALLOW and a BLOCK pattern is dropped.
"""
from __future__ import annotations

import os
import re

TECH_ONLY = (os.environ.get("PGPIPE_TECH_ONLY", "1").strip() not in ("0", "false", "no", ""))

# --- ALLOW: role is technical if any of these hit (case-insensitive) --------
_ALLOW_SRC = [
    r"software engineer", r"software developer", r"\bsde\b", r"\bswe\b",
    r"\bback[- ]?end\b", r"\bfront[- ]?end\b", r"\bfull[- ]?stack\b",
    r"staff engineer", r"principal engineer", r"senior engineer",
    r"systems? engineer", r"platform engineer", r"infrastructure engineer",
    r"\bdevops\b", r"\bsre\b", r"site reliability", r"reliability engineer",
    r"qa engineer", r"\bsdet\b", r"test engineer", r"automation engineer",
    r"data engineer", r"\bml engineer\b", r"machine learning engineer",
    r"\bai engineer\b", r"applied scientist", r"research engineer",
    r"research scientist", r"data scientist", r"security engineer",
    r"application security", r"mobile engineer", r"ios engineer",
    r"android engineer", r"embedded engineer", r"firmware engineer",
    r"cloud engineer", r"solutions? architect", r"software architect",
    r"engineering manager", r"technical lead", r"tech lead",
    r"developer advocate", r"computer vision", r"nlp engineer",
    r"quantitative developer", r"quant developer", r"quantitative researcher",
    r"quantitative research", r"performance engineer", r"compiler engineer",
    r"kernel engineer", r"database engineer", r"network engineer",
    r"blockchain engineer", r"game engineer", r"graphics engineer",
]

# --- BLOCK: drop even if an ALLOW matched ---------------------------------
_BLOCK_SRC = [
    r"sales engineer", r"solutions engineer", r"customer engineer",
    r"field engineer", r"support engineer", r"implementation engineer",
    r"\baccount\b", r"recruit", r"marketing", r"financial", r"collections",
    r"relationship manager", r"\bbranch\b",
]

_ALLOW = [re.compile(p, re.I) for p in _ALLOW_SRC]
_BLOCK = [re.compile(p, re.I) for p in _BLOCK_SRC]


def is_tech_role(title: str | None, department: str | None = None) -> bool:
    """True if the posting is an engineering / applied-science / quant-dev role."""
    text = f"{title or ''}  {department or ''}"
    if not text.strip():
        return False
    if any(p.search(text) for p in _BLOCK):
        return False
    return any(p.search(text) for p in _ALLOW)
