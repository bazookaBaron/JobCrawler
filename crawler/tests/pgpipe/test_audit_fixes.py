"""Audit-fix unit tests: relative-date truncation, http(s)-only ingest, logo/domain
derivation, workday window paging, retention SQL guards. No network, no DB."""

from __future__ import annotations

from datetime import datetime, timezone

from src.core.monitors import DiscoveredJob
from src.pgpipe import board_import as bi
from src.pgpipe import cleanup
from src.pgpipe.config import settings
from src.pgpipe.csv_sync import domain_of
from src.pgpipe.ingest import _is_http_url, _to_record
from src.pgpipe.monitors_workday import _age_days
from src.pgpipe.posted_at import parse_posted_at


def test_relative_dates_truncate_to_day():
    for raw in ("Posted Today", "Posted 3 Days Ago", "2 hours ago", "5 minutes ago", "Posted 30+ Days Ago"):
        d = parse_posted_at(raw)
        assert d is not None
        assert (d.hour, d.minute, d.second, d.microsecond) == (0, 0, 0, 0), raw
        assert d.tzinfo is not None
    # stable across calls within a day (the upsert no-op guard relies on this)
    assert parse_posted_at("Posted 3 Days Ago") == parse_posted_at("Posted 3 Days Ago")
    assert parse_posted_at("Posted Today") <= datetime.now(timezone.utc)


def test_absolute_dates_untouched():
    d = parse_posted_at("2026-07-29T17:50:41+02:00")
    assert (d.hour, d.minute) == (17, 50)


def test_ingest_rejects_non_http_urls():
    assert _is_http_url("https://x.com/jobs/1")
    assert _is_http_url("HTTP://x.com/1")
    for bad in ("javascript:alert(1)", "data:text/html,hi", "ftp://x.com/1", "//x.com/1", "/jobs/1", "", None):
        assert not _is_http_url(bad), bad
    ok = DiscoveredJob(url="https://x.com/j/1", title="Engineer")
    assert _to_record("c", "b", "greenhouse", ok) is not None
    assert _to_record("c", "b", "greenhouse", DiscoveredJob(url="javascript:alert(1)", title="x")) is None


def test_domain_of_website():
    assert domain_of("https://www.1password.com") == "1password.com"
    assert domain_of("http://Careers.Example.co.uk/path?q=1") == "careers.example.co.uk"
    assert domain_of("example.com") == "example.com"
    assert domain_of("") is None and domain_of(None) is None and domain_of("localhost") is None


def test_board_import_domain_only_for_custom_hosts():
    assert bi.custom_domain("https://job-boards.greenhouse.io/stripe", "greenhouse") is None
    assert bi.custom_domain("https://acme.wd5.myworkdayjobs.com/Site", "workday") is None
    assert bi.custom_domain("https://career5.successfactors.eu/", "successfactors") is None
    assert bi.custom_domain("https://careers.acme.com/", "successfactors") == "acme.com"
    m = bi.map_board("successfactors", "jobs.acme.co.uk")
    assert m["company_domain"] == "acme.co.uk"
    assert bi.map_board("greenhouse", "stripe")["company_domain"] is None


def test_workday_age_parse_and_window():
    assert _age_days("Posted Today") == 0
    assert _age_days("Posted Yesterday") == 1
    assert _age_days("Posted 14 Days Ago") == 14
    assert _age_days("Posted 30+ Days Ago") == 30
    assert _age_days(None) is None and _age_days("weird") is None
    assert settings.max_posting_age_days == 15 or settings.max_posting_age_days > 0


def test_workday_gets_larger_timeout():
    assert settings.board_timeout_for("workday") >= 600
    assert settings.board_timeout_for("greenhouse") == settings.board_timeout_seconds


def test_retention_sql_guards_board_health():
    # close/delete by "unseen" is gated on a successful, failure-free board crawl after last_seen_at
    assert "last_success_at > p.last_seen_at" in cleanup._BOARD_OK
    assert "consecutive_failures = 0" in cleanup._BOARD_OK
