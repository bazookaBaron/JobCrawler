"""Parse each ATS's raw, inconsistently-formatted `date_posted` string into a
real timestamp. Sources mix ISO-8601-with-offset and RFC-2822 style strings
(confirmed live: e.g. "2019-01-09T03:26:23-05:00" next to
"Wed, 29 Jul 2026 17:50:41 +0200" in the same column) — `dateutil` handles
both. Returns None on anything unparseable or implausible (comfortably in
the future — allow a little clock-skew slack rather than reject on any
offset), so callers can fall back to "posted now" (first_seen_at) rather
than silently keep a bad value.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from dateutil import parser as _dateutil_parser

_FUTURE_SLACK = timedelta(hours=6)

# Relative phrasing used by Workday ("Posted 3 Days Ago", "Posted 30+ Days
# Ago", "Posted Today") and iCIMS ("2 days ago", "today").
_REL_RE = re.compile(
    r"(?:posted\s+)?(?:(?P<plus>\d+)\+\s*(?P<pu>day|week|month)s?|"
    r"(?P<n>\d+)\s*(?P<u>minute|hour|day|week|month)s?)\s+ago",
    re.I,
)
_UNIT = {"minute": timedelta(minutes=1), "hour": timedelta(hours=1), "day": timedelta(days=1),
         "week": timedelta(weeks=1), "month": timedelta(days=30)}


def _parse_relative(raw: str, now: datetime) -> datetime | None:
    t = raw.strip().lower()
    if re.fullmatch(r"(posted\s+)?(today|just now|just posted)", t):
        return now
    if re.fullmatch(r"(posted\s+)?yesterday", t):
        return now - timedelta(days=1)
    m = _REL_RE.fullmatch(t)
    if not m:
        return None
    if m.group("plus"):
        # "30+ days ago": at least that old; use one past so a 30-day window drops it
        return now - _UNIT[m.group("pu").lower()] * (int(m.group("plus")) + 1)
    return now - _UNIT[m.group("u").lower()] * int(m.group("n"))


def parse_posted_at(raw: str | None) -> datetime | None:
    if not raw or not raw.strip():
        return None
    rel = _parse_relative(raw, datetime.now(timezone.utc))
    if rel is not None:
        return rel
    try:
        dt = _dateutil_parser.parse(raw.strip())
    except (ValueError, OverflowError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if dt > datetime.now(timezone.utc) + _FUTURE_SLACK:
        return None
    return dt
