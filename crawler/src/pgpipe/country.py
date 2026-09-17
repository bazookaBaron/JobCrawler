"""Lightweight, offline location -> country classifier.

Maps a job posting's free-text location string(s) to 'US' | 'IN' | 'other'.
No external geocoding calls — pure string matching against curated signal
lists, O(1) per posting (all lookups are against small fixed tuples/sets).
Wired into ingest.py at the same point location_type is captured, so every
newly-ingested posting gets classified at insert time. Deliberately
conservative: an unmatched or ambiguous string falls back to 'other' rather
than guessing.
"""
from __future__ import annotations

import re

# --- US signals -------------------------------------------------------
_US_STATE_NAMES: dict[str, str] = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}
# 2-letter abbreviations. Only matched with a strict ", XX" boundary (see
# _has_state_abbr below) — several of these collide with common English
# words ("in", "or", "me", "hi", "pa", ...) so a bare word match would be
# too noisy.
_US_STATE_ABBRS = set(_US_STATE_NAMES.values())

_US_CITIES = (
    "new york city", "new york, ny", "san francisco", "los angeles",
    "chicago", "houston", "phoenix", "philadelphia", "san antonio",
    "san diego", "dallas", "austin", "san jose", "fort worth",
    "jacksonville", "columbus", "charlotte", "indianapolis", "seattle",
    "denver", "boston", "nashville", "portland", "las vegas", "detroit",
    "memphis", "atlanta", "miami", "minneapolis", "raleigh", "cincinnati",
    "pittsburgh", "sacramento", "orlando", "st. louis", "saint louis",
    "tampa", "salt lake city", "kansas city", "mountain view",
    "palo alto", "menlo park", "redwood city", "sunnyvale", "cupertino",
    "santa clara", "irvine", "santa monica", "brooklyn", "arlington, va",
    "bellevue, wa", "cambridge, ma", "washington, dc", "washington dc",
    "washington, d.c.",
)

# Punctuated abbreviations are distinctive enough to match as plain
# substrings (word-boundary regex is unreliable when a pattern ends in
# punctuation like a period). Everything else is word-bounded below to
# avoid collisions like "in"/"india" matching inside "Indiana".
_US_MARKERS_SUBSTR = ("u.s.a.", "u.s.a", "u.s.")
_US_MARKERS_WORD = (
    "united states", "usa",
    "remote - us", "remote (us)", "remote, us", "remote-us", "remote us",
    "remote - usa", "remote (usa)", "us remote", "usa remote",
)

# --- India signals ------------------------------------------------------
_INDIA_CITIES = (
    "bangalore", "bengaluru", "hyderabad", "pune", "mumbai", "new delhi",
    "delhi", "delhi ncr", "gurgaon", "gurugram", "noida", "chennai",
    "kolkata", "calcutta", "ahmedabad", "chandigarh", "jaipur", "kochi",
    "cochin", "coimbatore", "indore", "nagpur", "vadodara", "thane",
    "navi mumbai", "trivandrum", "thiruvananthapuram", "visakhapatnam",
    "vizag", "mysore", "mysuru", "surat", "lucknow", "bhopal",
)

_INDIA_MARKERS = (
    "india", "remote - in", "remote (in)", "remote, india", "remote - india",
    "remote (india)", "india remote",
)
# All word-bounded (see _US_MARKERS_SUBSTR/_WORD comment) — bare "in"/"india"
# would otherwise substring-match inside "Indiana" etc.

_STATE_ABBR_RE = re.compile(r",\s*([a-z]{2})\b")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def _has_word(haystack: str, needle: str) -> bool:
    return re.search(rf"\b{re.escape(needle)}\b", haystack) is not None


def _has_us_state_abbr(blob: str) -> bool:
    """Match a 2-letter state code only right after a comma (e.g. 'Austin,
    TX' or 'Remote, NY') to avoid false positives on words like 'in'/'or'."""
    for m in _STATE_ABBR_RE.finditer(blob):
        if m.group(1).upper() in _US_STATE_ABBRS:
            return True
    return False


def classify_country(location: str | None, locations: list[str] | None = None) -> str | None:
    """Classify a posting's location into 'US' | 'IN' | 'other'.

    Checks India signals first (city/country names that never collide with
    US state abbreviations), then US signals (country markers, major
    cities, full state names, and ", XX" state abbreviations). Returns None
    when there's nothing to classify (no location text at all) so callers
    can leave the column NULL rather than writing a meaningless 'other'.
    """
    candidates = [location] if location else []
    if locations:
        candidates.extend(loc for loc in locations if loc)
    if not candidates:
        return None

    blob = _norm(" | ".join(candidates))
    if not blob:
        return None

    for city in _INDIA_CITIES:
        if _has_word(blob, city):
            return "IN"
    for marker in _INDIA_MARKERS:
        if _has_word(blob, marker):
            return "IN"

    for marker in _US_MARKERS_SUBSTR:
        if marker in blob:
            return "US"
    for marker in _US_MARKERS_WORD:
        if _has_word(blob, marker):
            return "US"
    for city in _US_CITIES:
        if city in blob:
            return "US"
    for state in _US_STATE_NAMES:
        if _has_word(blob, state):
            return "US"
    if _has_us_state_abbr(blob):
        return "US"

    return "other"
