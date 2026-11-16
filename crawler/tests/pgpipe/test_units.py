"""Pure unit tests for the pgpipe helpers (no network, no DB)."""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

import pytest

from src.core.monitors import DiscoveredJob
from src.core.monitors.lever import _created_at_iso
from src.pgpipe.country import classify_country
from src.pgpipe.feedref import cooldown_order
from src.pgpipe.ingest import _dedupe_records, _to_record
from src.pgpipe.keys import dedupe_key, norm_city, norm_company_key, norm_title, posting_key
from src.pgpipe.posted_at import is_relative_date, parse_posted_at
from src.pgpipe.tech_filter import is_tech_role
from src.pgpipe.urlnorm import canonical_url

NOW = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)


# --- canonical URL -------------------------------------------------------------
@pytest.mark.parametrize(
    "raw,want",
    [
        ("https://Boards.Greenhouse.io/Acme/jobs/123?gh_jid=123&gh_src=abc&utm_source=x#apply",
         "https://boards.greenhouse.io/Acme/jobs/123?gh_jid=123"),
        ("https://jobs.lever.co/acme/abc-def/?lever-source=LinkedIn&source=li&ref=x",
         "https://jobs.lever.co/acme/abc-def"),
        ("https://x.com/job?token=T&utm_campaign=c&b=2", "https://x.com/job?token=T&b=2"),
        ("HTTP://EXAMPLE.com:80/a/", "http://example.com/a"),
        ("https://example.com/", "https://example.com/"),
        ("https://example.com", "https://example.com"),
        ("  https://example.com/a  ", "https://example.com/a"),
    ],
)
def test_canonical_url(raw, want):
    assert canonical_url(raw) == want


@pytest.mark.parametrize(
    "raw",
    [None, "", "   ", "javascript:alert(1)", "ftp://x.com/a", "mailto:a@b.c",
     "data:text/html,hi", "//x.com/a", "/relative/path", "https://"],
)
def test_canonical_url_rejects(raw):
    assert canonical_url(raw) is None


def test_canonical_url_idempotent():
    u = "https://a.io/x/?utm_x=1&gh_jid=9"
    assert canonical_url(canonical_url(u)) == canonical_url(u)


def test_to_record_drops_bad_scheme_and_canonicalises():
    bad = DiscoveredJob(url="javascript:void(0)", title="Software Engineer")
    assert _to_record("c", "c-gh", "greenhouse", bad) is None
    ok = DiscoveredJob(url="https://B.com/j/1/?utm_source=z", title="Software Engineer")
    rec = _to_record("c", "c-gh", "greenhouse", ok, run_id=7, now=NOW)
    assert rec[4] == "https://b.com/j/1"
    assert rec[-1] == 7


def test_dedupe_records_same_url_or_external_id():
    def rec(url, ext):
        return _to_record(
            "c", "b", "greenhouse",
            DiscoveredJob(url=url, title="Software Engineer", source_identity=ext), 1, NOW)

    rows = [rec("https://a.com/1?utm_source=x", "greenhouse:c:1"), rec("https://a.com/1", "greenhouse:c:1"),
            rec("https://a.com/2", "greenhouse:c:1"), rec("https://a.com/3", None)]
    assert [r[4] for r in _dedupe_records(rows)] == ["https://a.com/1", "https://a.com/3"]


# --- dates ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw,delta",
    [
        ("Posted Today", timedelta(0)),
        ("posted today", timedelta(0)),
        ("Posted Yesterday", timedelta(days=1)),
        ("Posted 3 Days Ago", timedelta(days=3)),
        ("Posted 1 Day Ago", timedelta(days=1)),
        ("Posted 30+ Days Ago", timedelta(days=30)),
        ("Posted 30 + Days Ago", timedelta(days=30)),
    ],
)
def test_workday_relative_dates(raw, delta):
    assert parse_posted_at(raw, NOW) == NOW - delta
    assert is_relative_date(raw)


def test_absolute_dates():
    assert parse_posted_at("2026-11-10T09:30:00-05:00", NOW) == datetime(
        2026, 11, 10, 14, 30, tzinfo=timezone.utc)
    assert parse_posted_at("Wed, 11 Nov 2026 17:50:41 +0200", NOW) == datetime(
        2026, 11, 11, 15, 50, 41, tzinfo=timezone.utc)
    assert parse_posted_at("2026-11-10", NOW) == datetime(2026, 11, 10, tzinfo=timezone.utc)
    assert not is_relative_date("2026-11-10")


def test_epoch_dates_and_lever_created_at():
    ms = int(datetime(2026, 11, 1, tzinfo=timezone.utc).timestamp() * 1000)
    assert parse_posted_at(str(ms), NOW) == datetime(2026, 11, 1, tzinfo=timezone.utc)
    assert parse_posted_at(str(ms // 1000), NOW) == datetime(2026, 11, 1, tzinfo=timezone.utc)
    iso = _created_at_iso({"createdAt": ms})
    assert parse_posted_at(iso, NOW) == datetime(2026, 11, 1, tzinfo=timezone.utc)
    assert _created_at_iso({}) is None
    assert _created_at_iso({"createdAt": "nope"}) is None


@pytest.mark.parametrize("raw", [None, "", "  ", "garbage", "2099-01-01", "Posted Whenever"])
def test_unparseable_or_future_dates(raw):
    assert parse_posted_at(raw, NOW) is None


def test_undated_and_relative_rows_never_overwrite_posted_at():
    undated = _to_record(
        "c", "b", "pinpoint", DiscoveredJob(url="https://a.com/1", title="SWE"), 1, NOW)
    assert undated[15] is None and undated[16] is True
    rel = _to_record(
        "c", "b", "workday",
        DiscoveredJob(url="https://a.com/2", title="SWE", date_posted="Posted 3 Days Ago"), 1, NOW)
    assert rel[15] == NOW - timedelta(days=3) and rel[16] is True
    absolute = _to_record(
        "c", "b", "lever",
        DiscoveredJob(url="https://a.com/3", title="SWE",
                      date_posted="2026-11-01T00:00:00+00:00"), 1, NOW)
    assert absolute[16] is False


# --- tech filter -----------------------------------------------------------------------
@pytest.mark.parametrize(
    "title",
    [
        "Software Engineer", "Marketing Technology Engineer", "Account Data Engineer",
        "Developer Advocate", "Member of Technical Staff", "MTS, Inference", "SDE II", "SWE Intern",
        "Solution Architect", "DevOps Lead", "SRE", "Data Analyst", "Data Scientist",
        "ML Researcher", "AI Lead", "Analytics Engineer", "Founding Engineer", "Product Engineer",
        "QA Analyst", "SDET", "Test Engineer", "Programmer Analyst", "Engineering Manager",
        "Staff Backend Developer",
    ],
)
def test_tech_allowed(title):
    assert is_tech_role(title), title


@pytest.mark.parametrize(
    "title",
    [
        "Marketing Manager", "Account Executive", "Account Manager", "Sales Engineer",
        "Solutions Engineer", "Support Engineer", "Technical Recruiter", "Office Manager",
        "Civil Engineer", "Financial Analyst", "Content Marketing Lead", "", None,
    ],
)
def test_tech_blocked(title):
    assert not is_tech_role(title), title


def test_engineering_department_does_not_admit_non_tech_title():
    assert not is_tech_role("Office Manager", "Engineering")
    assert is_tech_role("Software Engineer", "Marketing")  # strong tech term beats soft block


# --- country ---------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "loc,locs,title,want",
    [
        ("Remote", None, None, "remote"),
        ("Worldwide", None, "Engineer", "remote"),
        ("Anywhere", None, None, "remote"),
        (None, None, "Backend Engineer (Remote)", "remote"),
        ("Remote - US", None, None, "US"),
        ("Remote, India", None, None, "IN"),
        ("Bengaluru, Karnataka", None, None, "IN"),
        ("Austin, TX", None, None, "US"),
        ("Berlin, Germany", None, None, "other"),
        ("Berlin", ["Berlin"], "Engineer", "other"),
        (None, None, None, None),
    ],
)
def test_country(loc, locs, title, want):
    assert classify_country(loc, locs, title) == want


# --- keys (mirror of SQL) ------------------------------------------------------------------------
def test_company_key():
    assert norm_company_key("Acme Technologies, Inc.") == "acme"
    assert norm_company_key("Foo Pvt. Ltd") == "foo"
    assert norm_company_key("Private Limited") == "privatelimited"  # never empty
    assert norm_company_key("Stripe") == "stripe"


def test_dedupe_key_stable_across_formatting():
    a = dedupe_key("acme", "Senior Software Engineer (Backend)", "Bengaluru, Karnataka, India")
    b = dedupe_key("acme", "senior software engineer - backend", "Bengaluru")
    assert norm_title("Senior  Software-Engineer") == "senior software engineer"
    assert norm_city("New York, NY") == "newyork"
    assert a == b


# --- greedy cooldown reference ---------------------------------------------------------------------
def test_cooldown_spreads_a_dominant_company():
    keys = ["A"] * 6 + list("BCDEFGHIJKL")  # newest-first; A has the 6 newest jobs
    order = cooldown_order(keys)
    assert sorted(order) == list(range(len(keys)))
    seq = [keys[i] for i in order]
    assert seq[:11] == ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "A"]


def test_cooldown_fallback_places_oldest_waiting():
    keys = ["A", "A", "B", "B", "B"]  # only 2 companies: the cooldown cannot hold
    # A(0); B(2) eligible; then both cooling -> oldest waiting = A, then B, B
    assert cooldown_order(keys) == [0, 2, 1, 3, 4]


def test_cooldown_is_a_permutation_and_respects_gap_when_possible():
    rng = random.Random(7)
    for _ in range(50):
        n_companies = rng.randint(12, 30)
        keys = [rng.randrange(n_companies) for _ in range(rng.randint(30, 120))]
        order = cooldown_order(keys)
        assert sorted(order) == list(range(len(keys)))
        seq = [keys[i] for i in order]
        for p, k in enumerate(seq):
            recent = seq[max(0, p - 9):p]
            if k in recent:  # repeat inside the window only if nothing else was eligible
                assert all(keys[i] in recent for i in order[p:]), (p, seq)


def test_cooldown_limit_is_prefix():
    keys = [i % 13 for i in range(60)]
    full = cooldown_order(keys)
    assert cooldown_order(keys, limit=20) == full[:20]
    assert cooldown_order([]) == []


def test_posting_key_identity_rules():
    import uuid
    a = posting_key("42", "linkedin", "acme", None, "https://www.linkedin.com/jobs/view/42/")
    assert isinstance(a, uuid.UUID)
    # linkedin id wins over everything else; ext id beats url; url is the fallback
    assert a == posting_key("42", "x", "y", "ext", "other")
    b = posting_key(None, "greenhouse", "acme", "gh:1", "https://a/1")
    assert b == posting_key(None, "greenhouse", "acme", "gh:1", "https://a/moved")
    assert b != posting_key(None, "lever", "acme", "gh:1", "https://a/1")
    c = posting_key(None, "greenhouse", "acme", None, "https://a/1")
    assert c != b and c == posting_key(None, "lever", "acme", "", "https://a/1")
