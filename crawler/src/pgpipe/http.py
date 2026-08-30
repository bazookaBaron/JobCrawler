"""A plain httpx client for monitor discover() calls.

Deliberately does NOT use src.shared.http (which pulls in the proxy provider
and SSRF-guarded transport). Job boards are public HTTPS APIs; we just need a
polite client with a browser-ish UA and redirects on.
"""
from __future__ import annotations

import httpx

# Mirrors jobseek's default UA (src/shared/http.py) — a bare custom UA gets
# WAF-blocked by several ATS/CDN vendors.
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)


def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(30.0, connect=15.0),
        headers={
            "User-Agent": _UA,
            "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
    )
