"""is_tech_role(title, department) — keep engineering, ML / data science,
applied & security research, and quant roles. Applied in ingest.py before
upsert when PGPIPE_TECH_ONLY=1 (the default).

BLOCK wins: a title that matches both an ALLOW and a BLOCK pattern is dropped.
Exception: the *soft* blocks (marketing, "account") are overridden by a strong
tech term, so "Marketing Technology Engineer" / "Account Data Engineer" survive
while "Marketing Manager" / "Account Executive" do not.
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
    r"developer advocate", r"performance engineer", r"compiler engineer",
    r"kernel engineer", r"database engineer", r"network engineer",
    r"blockchain engineer", r"game engineer", r"graphics engineer",
    r"robotics engineer", r"controls engineer",
    # --- ML / AI / data science ---
    r"machine learning", r"\bml\b", r"\bmlops\b", r"\bml ?ops\b",
    r"ml platform", r"ml infrastructure", r"deep learning",
    r"\bnlp\b", r"natural language processing", r"computer vision",
    r"\bllm\b", r"large language model", r"generative ai", r"\bgen ?ai\b",
    r"\bai/ ?ml\b", r"ml scientist", r"ml researcher",
    r"machine learning scientist", r"machine learning researcher",
    r"ai researcher", r"ai scientist", r"research scientist",
    r"data scien", r"decision scientist", r"applied research",
    # --- Quant ---
    r"quant engineer", r"quantitative engineer", r"quant developer",
    r"quantitative developer", r"quant researcher", r"quantitative research",
    r"quant analyst", r"quantitative analyst", r"quantitative strategist",
    # --- Security / cyber ---
    r"cyber ?security", r"information security", r"\binfosec\b",
    r"security analyst", r"security researcher", r"security architect",
    r"application security", r"product security", r"cloud security",
    r"offensive security", r"penetration test", r"\bpen[- ]?test",
    r"red team", r"blue team", r"detection engineer", r"security operations",
    r"\bsoc analyst\b", r"vulnerability", r"threat (intel|detection|research|hunt)",
    r"incident response", r"cryptography engineer", r"security software engineer",
]

# --- ALLOW (generic): matched against the TITLE only, never the department,
# so an "Engineering" department doesn't admit "Office Manager". -------------
_ALLOW_GENERIC_SRC = [
    r"\bdevelopers?\b", r"\bengineers?\b", r"\bengineering\b", r"\bprogrammers?\b",
    r"member of technical staff", r"\bmts\b", r"\bsde\b", r"\bswe\b",
    r"\barchitects?\b", r"\bdevops\b", r"\bsre\b",
    r"data (scientist|engineer|analyst)", r"\bml\b", r"\bai\b",
    r"analytics engineer", r"founding engineer", r"product engineer",
    r"\bqa\b", r"\bsdet\b", r"test engineer",
]

# --- BLOCK: drop even if an ALLOW matched ---------------------------------
_BLOCK_SRC = [
    r"sales engineer", r"solutions engineer", r"customer engineer",
    r"field engineer", r"support engineer", r"implementation engineer",
    r"recruit", r"collections",
    r"financial (advis|analyst|planner|consultant|controller|servic|reporting)",
    r"relationship manager", r"\bbranch\b",
    # non-software engineering disciplines now that bare "engineer" is allowed
    r"\b(civil|structural|chemical|petroleum|mining|stationary|operating) engineer",
]

# Soft blocks: dropped unless a STRONG tech term is also present.
_SOFT_BLOCK_SRC = [r"\baccount\b", r"marketing"]

# Terms strong enough to override a soft block. Deliberately specific: a bare
# "engineer" is NOT enough ("Marketing Engineer" is ambiguous); a qualified one
# ("technology engineer", "software engineer") is.
_STRONG_SRC = [
    r"\bsoftware\b", r"\bdevelopers?\b", r"\bprogrammers?\b", r"\bdevops\b",
    r"\bsre\b", r"\bsdet\b", r"\bsde\b", r"\bswe\b", r"\bmts\b",
    r"member of technical staff", r"data (engineer|scientist)", r"machine learning",
    r"\b(ml|ai) engineer",
    r"\b(technology|technical|platform|data|systems?|infrastructure|back[- ]?end|"
    r"front[- ]?end|full[- ]?stack|security|cloud|analytics|product) engineer",
    r"engineering manager", r"\barchitect\b",
]

_ALLOW = [re.compile(p, re.I) for p in _ALLOW_SRC]
_ALLOW_GENERIC = [re.compile(p, re.I) for p in _ALLOW_GENERIC_SRC]
_BLOCK = [re.compile(p, re.I) for p in _BLOCK_SRC]
_SOFT_BLOCK = [re.compile(p, re.I) for p in _SOFT_BLOCK_SRC]
_STRONG = [re.compile(p, re.I) for p in _STRONG_SRC]


def is_tech_role(title: str | None, department: str | None = None) -> bool:
    """True if the posting is an engineering / applied-science / quant-dev role."""
    text = f"{title or ''}  {department or ''}"
    if not text.strip():
        return False
    if any(p.search(text) for p in _BLOCK):
        return False
    if any(p.search(text) for p in _SOFT_BLOCK) and not any(p.search(text) for p in _STRONG):
        return False
    if any(p.search(text) for p in _ALLOW):
        return True
    return any(p.search(title or "") for p in _ALLOW_GENERIC)
