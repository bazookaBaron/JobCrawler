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

from datetime import datetime, timedelta, timezone

from dateutil import parser as _dateutil_parser

_FUTURE_SLACK = timedelta(hours=6)


def parse_posted_at(raw: str | None) -> datetime | None:
    if not raw or not raw.strip():
        return None
    try:
        dt = _dateutil_parser.parse(raw.strip())
    except (ValueError, OverflowError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if dt > datetime.now(timezone.utc) + _FUTURE_SLACK:
        return None
    return dt
