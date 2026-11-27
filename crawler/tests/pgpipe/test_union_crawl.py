"""Unit tests for the union-crawl pieces: board mapping, iCIMS listing parsing,
relative posted dates, country gate, LinkedIn guest-card parsing + throttle,
shard clause, DB size guard. No network, no DB."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src.pgpipe import board_import as bi
from src.pgpipe import linkedin_source as li
from src.pgpipe.dbguard import SizeGuard, over_limit
from src.pgpipe.ingest import keep_country
from src.pgpipe.monitors_icims import host_of, normalize_location, parse_listing
from src.pgpipe.posted_at import parse_posted_at
from src.pgpipe.queue import parse_shard, shard_clause

# --- board mapping -----------------------------------------------------------------


def test_map_token_boards():
    gh = bi.map_board("greenhouse", "Stripe", "Stripe Inc")
    assert gh["monitor_type"] == "greenhouse"
    assert gh["board_url"] == "https://job-boards.greenhouse.io/Stripe"
    assert gh["monitor_config"] == {"token": "Stripe"}
    assert gh["company_slug"] == "stripe-inc" and gh["company_name"] == "Stripe Inc"
    assert gh["identity"] == "greenhouse:stripe"
    assert bi.map_board("ashby", "0g")["board_url"] == "https://jobs.ashbyhq.com/0g"
    assert bi.map_board("lever", "100ms")["monitor_config"] == {"token": "100ms"}


def test_map_workday():
    m = bi.map_board("workday", "2020companies.wd1.myworkdayjobs.com/External_Careers", "2020companies")
    assert m["monitor_type"] == "workday"
    assert m["monitor_config"] == {"company": "2020companies", "wd_instance": "wd1", "site": "External_Careers"}
    assert m["board_url"] == "https://2020companies.wd1.myworkdayjobs.com/External_Careers"
    assert bi.map_board("workday", "not-a-workday-host/site") is None
    assert bi.map_board("workday", "x.wd5.myworkdayjobs.com") is None  # no site


def test_map_icims_and_successfactors():
    ic = bi.map_board("icims", "cancareers-yelp.icims.com")
    assert ic["monitor_type"] == "icims" and ic["monitor_config"] == {"host": "cancareers-yelp.icims.com"}
    assert ic["company_slug"] == "yelp"
    assert bi.map_board("icims", "www.icims.com") is None
    sf = bi.map_board("successfactors", "https://apply-now.serco.com/googlefeed.xml")
    assert sf["monitor_type"] == "rss"
    assert sf["monitor_config"] == {"preset": "successfactors", "feed_url": "https://apply-now.serco.com/googlefeed.xml"}
    assert sf["company_slug"] == "serco"
    assert bi.map_board("successfactors", "https://133723.jobs2web.com/googlefeed.xml")["company_slug"] == "sf-133723"
    assert bi.map_board("smartrecruiters", "Foo") is None  # unsupported -> skipped


def test_identity_of_existing_matches_mapping():
    m = bi.map_board("greenhouse", "agilityrobotics")
    assert bi.identity_of_existing("greenhouse", "https://job-boards.greenhouse.io/agilityrobotics",
                                   {"token": "agilityrobotics"}) == m["identity"]
    w = bi.map_board("workday", "tibco.wd5.myworkdayjobs.com/Cloud_Software_Group")
    assert bi.identity_of_existing(
        "workday", "https://tibco.wd5.myworkdayjobs.com/Cloud_Software_Group",
        {"site": "Cloud_Software_Group", "company": "tibco", "wd_instance": "wd5"}) == w["identity"]
    sf = bi.map_board("successfactors", "https://jobs.atos.net/googlefeed.xml")
    assert bi.identity_of_existing(
        "rss", "https://jobs.atos.net/viewalljobs/",
        {"preset": "successfactors", "feed_url": "https://jobs.atos.net/googlefeed.xml"}) == sf["identity"]
    assert bi.identity_of_existing("pinpoint", "x", {}) is None


def test_load_and_map_all_dedupes(tmp_path):
    db = tmp_path / "d.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE boards(ats TEXT, board_id TEXT, host TEXT, site TEXT, company TEXT, status TEXT)")
    con.executemany("INSERT INTO boards VALUES (?,?,?,?,?,?)", [
        ("greenhouse", "acme", "", "", "Acme", "ok"),
        ("greenhouse", "dead", "", "", "Dead", "dead"),
        ("icims", "careers-foo.icims.com", "", "", "", "ok"),
    ])
    con.commit()
    con.close()
    (tmp_path / "companies.json").write_text(json.dumps({
        "greenhouse": ["ACME", "other"], "workday": [{"name": "n", "host": "a.wd1.myworkdayjobs.com", "site": "S"}],
        "eightfold": [{"name": "x"}]}))
    src = bi.load_sqlite(db) + bi.load_json_dir(tmp_path)
    assert {s["board_id"] for s in src} == {"acme", "careers-foo.icims.com", "ACME", "other", "a.wd1.myworkdayjobs.com/S"}
    mapped, skipped = bi.map_all(src)
    assert len(mapped) == 4 and skipped["dup_in_input"] == 1  # ACME == acme (case-insensitive)
    assert mapped[0]["company_name"] == "Acme"  # first (sqlite) wins: it carries the name


# --- iCIMS -------------------------------------------------------------------------

ICIMS_PAGE = """
<h2>Search Results Page 1 of 3</h2>
<ul class="container-fluid iCIMS_JobsTable">
<li class="iCIMS_JobCardItem"><div class="row">
<div class="col-xs-6 header left"><span class="sr-only field-label">Job Locations</span>
<span >
US-TX-Austin | UK-London</span></div>
<div class="col-xs-6 header right"><span class="sr-only field-label">Requisition Post Information* : Posted Date</span>
<span title="20.11.2026 9:43 AM">
2 days ago<span class="sr-only">(20.11.2026 9:43 AM)</span></span></div>
<div class="col-xs-12 title"><a href="https://x.icims.com/jobs/485240/regional-account-executive/job?in_iframe=1" class="iCIMS_Anchor">
<span class="sr-only field-label">Job Title</span><h3 >
Regional Account Executive</h3></a></div></div></li>
<li class="iCIMS_JobCardItem"><div class="row">
<div class="col-xs-6 header left"><span class="sr-only field-label">Job Locations</span><span >IN-KA-Bangalore</span></div>
<div class="col-xs-12 title"><a href="https://x.icims.com/jobs/7/finance-analyst/job"><h3>Finance Analyst</h3></a></div></div></li>
</ul>
"""


def test_icims_parse_listing():
    jobs = parse_listing(ICIMS_PAGE, "x.icims.com")
    assert [j.title for j in jobs] == ["Regional Account Executive", "Finance Analyst"]
    j0, j1 = jobs
    assert j0.url == "https://x.icims.com/jobs/485240/regional-account-executive/job"
    assert j0.locations == ["Austin, TX, United States", "UK-London"]
    assert j0.metadata == {"requisition_id": "485240"}
    assert datetime.fromisoformat(j0.date_posted).date() == (datetime.now(timezone.utc) - timedelta(days=2)).date()
    assert j1.locations == ["Bangalore, India"] and j1.date_posted is None


def test_icims_location_and_host():
    assert normalize_location("US-NY-New York | IN-MH-Mumbai") == ["New York, NY, United States", "Mumbai, India"]
    assert normalize_location("UK-Reading") == ["UK-Reading"]
    assert host_of({"metadata": {"host": "Foo.icims.com"}, "board_url": ""}) == "foo.icims.com"
    assert host_of({"metadata": {}, "board_url": "https://bar.icims.com/jobs/search?ss=1"}) == "bar.icims.com"
    assert host_of({"metadata": {}, "board_url": "https://www.icims.com/"}) is None


# --- posted_at relative forms / country gate --------------------------------------------


@pytest.mark.parametrize("raw, days", [("Posted Today", 0), ("Posted Yesterday", 1), ("Posted 3 Days Ago", 3),
                                       ("2 days ago", 2), ("Posted 30+ Days Ago", 31)])
def test_relative_posted_at(raw, days):
    got = parse_posted_at(raw)
    assert got is not None
    # truncated to midnight UTC so repeated crawls produce the same value
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    assert got == today - timedelta(days=days)


def test_iso_posted_at_unchanged():
    assert parse_posted_at("2026-11-01T00:00:00Z") == datetime(2026, 11, 1, tzinfo=timezone.utc)
    assert parse_posted_at("garbage") is None


@pytest.mark.parametrize("loc, want", [
    ("Hagen, DE, 58099", "other"),              # Germany, not Delaware
    ("Mirabel, Quebec, CA, J7N 1H3", "other"),  # Canada, not California
    ("Camden, AR, US, 71701", "US"),
    ("San Jose, CA, 95112", "US"),
    ("Pune, IN, 411001", "IN"),                 # 6-digit PIN -> India
    ("Carmel, IN, 46032", "US"),                # 5-digit ZIP -> Indiana
    ("Berlin, Germany", "other"),
    ("Bengaluru, Karnataka, India", "IN"),
    ("Austin, TX", "US"),
    ("Remote", "other"),
])
def test_country_classifier_false_positive_guards(loc, want):
    from src.pgpipe.country import classify_country
    assert classify_country(loc) == want


def test_country_any_location_in_or_us_wins():
    from src.pgpipe.country import classify_country
    assert classify_country("Berlin, Germany", ["Berlin, Germany", "New York, NY"]) == "US"
    assert classify_country("London", ["London", "Pune, India"]) == "IN"
    assert classify_country(None, []) is None


def test_country_gate_only_in_us():
    assert keep_country("IN") and keep_country("US")
    assert not keep_country("other") and not keep_country(None)


# --- LinkedIn --------------------------------------------------------------------------

CARD = """<li><div class="base-card" data-entity-urn="urn:li:jobPosting:4012345678">
<a class="base-card__full-link" href="https://in.linkedin.com/jobs/view/x-4012345678?trk=a"></a>
<h3 class="base-search-card__title">
  Senior Product Manager </h3>
<h4 class="base-search-card__subtitle"><a>Acme &amp; Sons Pvt. Ltd.</a></h4>
<span class="job-search-card__location">Bengaluru, Karnataka, India</span>
<time class="job-search-card__listdate" datetime="2026-11-21">1 day ago</time></div></li>
<li><div data-entity-urn="urn:li:jobPosting:5">
<h3 class="base-search-card__title">Chef</h3><h4 class="base-search-card__subtitle">Cafe</h4>
<span class="job-search-card__location">London, England, United Kingdom</span></div></li>"""


def test_linkedin_parse_and_row():
    cards = li.parse_cards(CARD)
    assert [c["id"] for c in cards] == ["4012345678", "5"]
    c = cards[0]
    assert c["title"] == "Senior Product Manager"
    assert c["company"] == "Acme & Sons Pvt. Ltd."
    assert c["posted"] == "2026-11-21"
    row = li.card_to_row(c)
    assert row["url"] == "https://www.linkedin.com/jobs/view/4012345678"
    assert row["external_id"] == "4012345678"
    assert row["company_slug"] == "acme-sons-pvt-ltd"
    assert row["country"] == "IN" and row["seniority"] == "senior"
    assert row["posted_at"] == datetime(2026, 11, 21, tzinfo=timezone.utc)
    assert li.card_to_row(cards[1]) is None  # UK -> dropped by the IN/US gate


async def test_linkedin_stops_after_three_consecutive_refusals():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(999)

    async def no_sleep(_s):
        return None

    state = li.LinkedInState()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        results = [await li.fetch_page(client, state, {"keywords": "x"}, no_sleep) for _ in range(5)]
    assert results[:2] == ["", ""] and results[2:] == [None, None, None]
    assert state.dead and state.refusals_total == 3 and len(calls) == 3  # no request after dead


async def test_linkedin_success_resets_refusal_streak():
    seq = iter([429, 429, 200, 429, 429])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(next(seq), text="<li></li>")

    async def no_sleep(_s):
        return None

    state = li.LinkedInState()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        for _ in range(5):
            await li.fetch_page(client, state, {}, no_sleep)
    assert not state.dead and state.consecutive_refusals == 2 and state.refusals_total == 4


# --- shard clause / size guard ---------------------------------------------------------------


def test_shard_clause_and_parse():
    assert shard_clause(None) == ""
    c = shard_clause((2, 8))
    assert "hashtext(b.board_slug)" in c and c.endswith("= 2") and "8" in c
    assert parse_shard("3/8") == (3, 8) and parse_shard("") is None
    for bad in ("8/8", "-1/4", "0/0"):
        with pytest.raises(ValueError):
            parse_shard(bad)


def test_over_limit():
    assert over_limit(420.1, 420) and not over_limit(419.9, 420) and not over_limit(10_000, 0)


class _FakePool:
    def __init__(self, sizes):
        self.sizes = list(sizes)
        self.calls = 0

    async def fetchval(self, _sql):
        self.calls += 1
        return self.sizes.pop(0) * 1048576


async def test_size_guard_blocks_and_caches():
    pool = _FakePool([100, 430, 200])
    g = SizeGuard(pool, max_mb=420, ttl=0)  # ttl 0: re-read every call
    assert await g.refresh() is False
    assert await g.refresh() is True and g.blocked and round(g.size_mb) == 430
    assert await g.refresh() is False  # space freed -> unblocked again
    pool2 = _FakePool([500])
    g2 = SizeGuard(pool2, max_mb=420, ttl=3600)
    assert await g2.refresh() is True and await g2.refresh() is True
    assert pool2.calls == 1  # cached within ttl


# --- Workday: global gate, 429 retry, early stop on "30+ days" --------------------------------


def _wd_posting(i: int, posted: str) -> dict:
    return {"title": f"Role {i}", "externalPath": f"/job/Loc/Role-{i}_R{i}", "locationsText": "Pune, India",
            "postedOn": posted, "bulletFields": [f"R{i}"]}


async def test_workday_retries_429_and_stops_on_old_pages(monkeypatch):
    from src.pgpipe import monitors_workday as wd

    monkeypatch.setattr(wd, "_RETRY_WAITS", (0.0, 0.0, 0.0))
    from types import SimpleNamespace
    monkeypatch.setattr(wd, "settings", SimpleNamespace(workday_gap_seconds=0.0, workday_max_pages=50, max_posting_age_days=15))
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        offset = json.loads(request.content)["offset"]
        if offset == 0:
            return httpx.Response(200, json={"total": 100, "jobPostings": [_wd_posting(i, "Posted Today") for i in range(20)]})
        # second page is entirely past the 15-day window -> crawl must stop, never ask for offset 40
        return httpx.Response(200, json={"total": 100, "jobPostings": [_wd_posting(100 + i, "Posted 16 Days Ago") for i in range(20)]})

    board = {"board_url": "https://acme.wd5.myworkdayjobs.com/Careers", "metadata": {}, "board_slug": "x"}
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        jobs = await wd.discover_workday(board, client)
    assert calls["n"] == 3                      # 429 retry + page 0 + page 1, then stop
    assert len(jobs) == 40
    assert jobs[0].url == "https://acme.wd5.myworkdayjobs.com/Careers/job/Loc/Role-0_R0"
    assert parse_posted_at(jobs[-1].date_posted) < datetime.now(timezone.utc) - timedelta(days=15)
