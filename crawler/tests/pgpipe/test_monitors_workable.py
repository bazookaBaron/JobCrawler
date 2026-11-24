"""pgpipe rich Workable adapter: one widget-API call -> DiscoveredJob rows with location + date."""
from __future__ import annotations

import asyncio

import httpx
import pytest

from src.core.monitors import BoardGoneError
from src.pgpipe.monitors_workable import discover_workable, workable_slug

WIDGET = {
    "name": "Acme Consulting",
    "jobs": [
        {"title": "Java Developer", "shortcode": "ABC123", "published_on": "2026-11-20",
         "employment_type": "Full-time", "telecommuting": False, "industry": "Information Technology and Services",
         "locations": [{"country": "India", "countryCode": "IN", "city": "Pune", "region": "Maharashtra"}],
         "description": "<p>Work for our clients</p>"},
        {"title": "No code", "shortcode": None},
    ],
}


def _client(status: int, body: dict | None = None) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/widget/accounts/acme"
        return httpx.Response(status, json=body or {})
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def test_slug_from_config_or_url():
    assert workable_slug("https://apply.workable.com/acme/", {}) == "acme"
    assert workable_slug("https://apply.workable.com/x/", {"token": "acme"}) == "acme"
    assert workable_slug("https://apply.workable.com/j/ABC/", {}) is None


def test_discover_rich_rows():
    jobs = asyncio.run(discover_workable({"board_url": "https://apply.workable.com/acme/", "metadata": {}},
                                         _client(200, WIDGET)))
    assert len(jobs) == 1
    j = jobs[0]
    assert j.url == "https://apply.workable.com/acme/j/ABC123/"
    assert j.locations == ["Pune, Maharashtra, India"]
    assert j.date_posted == "2026-11-20"
    assert j.source_identity == "workable:acme:ABC123"
    assert j.metadata["industry"] == "Information Technology and Services"


def test_gone_on_404():
    with pytest.raises(BoardGoneError):
        asyncio.run(discover_workable({"board_url": "https://apply.workable.com/acme/", "metadata": {}},
                                      _client(404)))
