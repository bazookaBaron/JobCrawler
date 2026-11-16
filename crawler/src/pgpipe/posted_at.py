"""Parse each ATS's raw `date_posted` into a real timestamp.

Handles
  * ISO-8601 (with/without offset) and RFC-2822 strings (dateutil),
  * epoch seconds / milliseconds (as digits, e.g. Lever `createdAt`),
  * Workday relative text, resolved against the crawl time `now`:
        "Posted Today"        -> now
        "Posted Yesterday"    -> now - 1d
        "Posted N Days Ago"   -> now - Nd
        "Posted 30+ Days Ago" -> now - 30d  (lower bound; it is *at least* that old)
Returns None on anything unparseable or implausible (comfortably in the
future), so callers can fall back to first_seen_at rather than keep a bad
value. `is_relative_date()` tells ingest the result is an estimate that must
not overwrite an earlier, better value on later crawls.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from dateutil import parser as _dateutil_parser

_FUTURE_SLACK = timedelta(hours=6)

_REL_TODAY = re.compile(r"\b(posted\s+)?(today|just\s+(posted|now))\b", re.I)
_REL_YESTERDAY = re.compile(r"\b(posted\s+)?yesterday\b", re.I)
_REL_DAYS = re.compile(r"\b(?:posted\s+)?(\d+)\s*\+?\s*days?\s+ago\b", re.I)
_DIGITS = re.compile(r"^\d{9,14}$")


def is_relative_date(raw: str | None) -> bool:
    if not raw:
        return False
    s = raw.strip()
    return bool(_REL_TODAY.search(s) or _REL_YESTERDAY.search(s) or _REL_DAYS.search(s))


def _relative(s: str, now: datetime) -> datetime | None:
    if _REL_TODAY.search(s):
        return now
    if _REL_YESTERDAY.search(s):
        return now - timedelta(days=1)
    m = _REL_DAYS.search(s)
    if m:
        return now - timedelta(days=int(m.group(1)))
    return None


def _epoch(s: str) -> datetime | None:
    n = int(s)
    if n >= 10**11:  # milliseconds
        n //= 1000
    try:
        return datetime.fromtimestamp(n, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def parse_posted_at(raw: str | None, now: datetime | None = None) -> datetime | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    now = now or datetime.now(timezone.utc)

    dt: datetime | None
    if _DIGITS.match(s):
        dt = _epoch(s)
    else:
        dt = _relative(s, now)
        if dt is None:
            try:
                dt = _dateutil_parser.parse(s)
            except (ValueError, OverflowError, TypeError):
                return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if dt > now + _FUTURE_SLACK:
        return None
    return dt
