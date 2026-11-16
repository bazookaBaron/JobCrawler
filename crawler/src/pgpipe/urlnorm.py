"""Canonical posting-URL normalisation, applied at ingest so the
UNIQUE (company_slug, url) key is stable across crawls and across tracking
variants of the same link.

Rules: http/https only (anything else -> None), lowercase scheme + host,
drop the fragment, drop tracking query params (utm_*, gh_src, lever-source,
source, ref), KEEP functional ones (gh_jid, token, ...), strip a trailing
slash from the path. Remaining query params keep their original order.
"""
from __future__ import annotations

from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

_TRACKING_EXACT = frozenset({"gh_src", "lever-source", "source", "ref"})
_TRACKING_PREFIXES = ("utm_",)


def _is_tracking(key: str) -> bool:
    k = key.lower()
    return k in _TRACKING_EXACT or k.startswith(_TRACKING_PREFIXES)


def canonical_url(url: str | None) -> str | None:
    """Return the canonical form of `url`, or None if it is empty/invalid or
    not http(s)."""
    if not url:
        return None
    raw = url.strip()
    if not raw:
        return None
    try:
        parts = urlsplit(raw)
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        return None
    host = (parts.hostname or "").lower()
    if not host:
        return None
    netloc = host
    try:
        port = parts.port
    except ValueError:
        return None
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{host}:{port}"
    path = parts.path
    if len(path) > 1:
        path = path.rstrip("/") or "/"
    pairs = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if not _is_tracking(k)]
    query = urlencode(pairs, quote_via=quote, safe="/:,@")
    return urlunsplit((scheme, netloc, path, query, ""))
