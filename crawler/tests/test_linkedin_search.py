from __future__ import annotations

import asyncio
import json
import random
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from src.pgpipe import linkedin_search as ls

FX = Path(__file__).parent / "fixtures" / "linkedin"
NOW = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)


def fx(name: str) -> str:
    return (FX / name).read_text(encoding="utf-8")


def by_id(html_name="search_page_full.html"):
    return {c.job_id: c for c in ls.parse_cards(fx(html_name), NOW)}


# ------------------------------------------------------------------ parsing
def test_parse_full_page():
    cards = by_id()
    assert len(cards) == 10
    c = cards["4000000001"]
    assert c.title == "Senior Software Engineer"
    assert c.company_name == "Acme Technologies Pvt Ltd"
    assert c.company_link == "https://www.linkedin.com/company/acme-technologies-pvt-ltd"
    assert c.location == "Bengaluru, Karnataka, India"
    assert c.url == "https://www.linkedin.com/jobs/view/4000000001/"
    assert not c.is_repost and c.reposted_at is None


def test_relative_hours_beats_date_only_attr():
    c = by_id()
    assert c["4000000001"].posted_at == NOW - timedelta(hours=2)
    assert c["4000000002"].posted_at == NOW - timedelta(minutes=45)
    assert c["4000000007"].posted_at == NOW  # "Just now"


def test_days_and_weeks_fall_back_to_date_attr():
    c = by_id()
    assert c["4000000004"].posted_at == datetime(2026, 11, 13, tzinfo=timezone.utc)
    assert c["4000000009"].posted_at == datetime(2026, 11, 9, tzinfo=timezone.utc)


def test_reposted_card():
    c = by_id()["4000000003"]
    assert c.is_repost
    assert c.reposted_at == NOW - timedelta(hours=3)
    assert c.posted_at == datetime(2026, 11, 14, tzinfo=timezone.utc)  # original date kept


def test_parse_relative_units():
    assert ls.parse_relative("2 weeks ago", NOW)[0] == NOW - timedelta(weeks=2)
    assert ls.parse_relative("Reposted 1 day ago", NOW)[0] == NOW - timedelta(days=1)
    assert ls.parse_relative("garbage", NOW) == (None, 0)
    assert ls.parse_relative(None, NOW) == (None, 0)


def test_empty_and_garbage_pages():
    assert ls.parse_cards(fx("search_empty.html"), NOW) == []
    assert ls.parse_cards("", NOW) == []
    assert ls.parse_cards("<html><div class='base-search-card'>no urn</div></html>", NOW) == []


def test_apply_url_extraction():
    assert ls.parse_apply_url(fx("job_posting_external.html")) == "https://boards.greenhouse.io/acme/jobs/123?gh_src=li"
    assert ls.parse_apply_url(fx("job_posting_easy.html")) is None


# ------------------------------------------------------------------ filtering
def _classified():
    stats = ls.Stats()
    kept = ls.filter_cards(by_id(), stats, tech_only=True)
    return {c.job_id: c for c in kept}, stats


def test_filtering_drops_non_tech_and_foreign():
    kept, stats = _classified()
    assert "4000000005" not in kept          # Marketing Manager
    assert "4000000006" not in kept          # Berlin
    assert stats.filtered.get("not_tech", 0) >= 1
    assert stats.filtered.get("other_country") == 1
    assert kept["4000000001"].country == "IN"
    assert kept["4000000001"].seniority == "senior"
    assert kept["4000000002"].location_type == "remote"
    assert kept["4000000003"].country == "US"


def test_keep_other_countries_flag():
    kept = ls.filter_cards(by_id(), ls.Stats(), tech_only=True, keep_other_countries=True)
    assert "4000000006" in {c.job_id for c in kept}


def test_title_exclude():
    kept = ls.filter_cards(by_id(), ls.Stats(), tech_only=True, title_exclude=["intern"])
    assert "4000000008" not in {c.job_id for c in kept}


def test_slugify():
    assert ls.slugify("Acme Technologies Pvt. Ltd.") == "acme-technologies-pvt-ltd"
    assert ls.slugify("  ") == ""


# ------------------------------------------------------------------ fetcher / backoff
class Script:
    """Mock transport serving a scripted list of status codes (then 200 bodies)."""

    def __init__(self, statuses, body=""):
        self.statuses = list(statuses)
        self.body = body
        self.calls = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        status = self.statuses.pop(0) if self.statuses else 200
        return httpx.Response(status, text=self.body if status == 200 else "x")


def make_fetcher(script, **kw):
    sleeps: list[float] = []
    client = httpx.Client(transport=httpx.MockTransport(script))
    f = ls.Fetcher(client, sleep=sleeps.append, rng=random.Random(1), **kw)
    return f, sleeps


def test_backoff_on_429_then_success():
    script = Script([429, 429, 200], body="ok")
    f, sleeps = make_fetcher(script)
    assert f.get(ls.SEARCH_URL) == "ok"
    assert script.calls == 3
    assert f.consecutive_blocks == 0 and f.blocked == 2
    backoffs = [s for s in sleeps if s > 4.0]
    assert len(backoffs) == 2 and backoffs[1] > backoffs[0] * 0.9  # grows (with jitter)


def test_5xx_retried_max_four_then_gives_up():
    script = Script([503] * 10)
    f, _ = make_fetcher(script)
    assert f.get(ls.SEARCH_URL) is None
    assert script.calls == 1 + ls.MAX_RETRIES
    assert f.blocked == 0


def test_abort_after_five_consecutive_blocks():
    script = Script([999] * 50)
    f, _ = make_fetcher(script)
    with pytest.raises(ls.BlockedError):
        f.get(ls.SEARCH_URL)
    assert script.calls == ls.MAX_CONSECUTIVE_BLOCKS


def test_success_resets_block_counter():
    script = Script([429, 429, 429, 200, 429, 429, 429, 200], body="ok")
    f, _ = make_fetcher(script)
    assert f.get(ls.SEARCH_URL) == "ok"
    assert f.get(ls.SEARCH_URL) == "ok"


def test_budget_exhausted():
    script = Script([], body="ok")
    f, _ = make_fetcher(script, budget=2)
    f.get(ls.SEARCH_URL)
    f.get(ls.SEARCH_URL)
    with pytest.raises(ls.BudgetExhausted):
        f.get(ls.SEARCH_URL)


def test_404_skipped_without_retry():
    script = Script([404])
    f, _ = make_fetcher(script)
    assert f.get(ls.SEARCH_URL) is None and script.calls == 1


def test_delay_is_jittered_between_requests():
    script = Script([], body="ok")
    f, sleeps = make_fetcher(script)
    for _ in range(4):
        f.get(ls.SEARCH_URL)
    assert len(sleeps) == 3 and all(2.0 <= s <= 4.0 for s in sleeps)


# ------------------------------------------------------------------ crawl loop
def test_crawl_pages_until_short_page_and_dedupes():
    pages = [fx("search_page_full.html"), fx("search_page_full.html"), fx("search_page_short.html")]
    seen_params = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen_params.append(dict(req.url.params))
        return httpx.Response(200, text=pages[min(len(seen_params) - 1, 2)])

    f = ls.Fetcher(httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda s: None, rng=random.Random(0))
    found, stats = ls.crawl(f, ["software engineer"], ["India"], window_hours=3, pages_per_query=5, now=lambda: NOW)
    assert [p["start"] for p in seen_params] == ["0", "10", "20"]
    assert seen_params[0]["f_TPR"] == "r10800" and seen_params[0]["sortBy"] == "DD"
    assert seen_params[0]["keywords"] == "software engineer" and seen_params[0]["location"] == "India"
    assert len(found) == 10 and stats.requests == 3 and stats.unique == 10


def test_crawl_returns_partial_on_block():
    f, _ = make_fetcher(Script([429] * 20))
    found, stats = ls.crawl(f, ["a", "b"], ["India"], window_hours=3, pages_per_query=2, now=lambda: NOW)
    assert stats.aborted and stats.blocked == 5 and found == {}


def test_crawl_respects_budget():
    f, _ = make_fetcher(Script([], body=fx("search_empty.html")), budget=3)
    _, stats = ls.crawl(f, ["a", "b", "c", "d", "e"], ["India", "Pune"], window_hours=3, pages_per_query=1, now=lambda: NOW)
    assert stats.requests == 3 and stats.skipped_queries > 0 and not stats.aborted


def test_fetch_apply_urls_limit():
    f, _ = make_fetcher(Script([], body=fx("job_posting_external.html")))
    cards = list(ls.parse_cards(fx("search_page_full.html"), NOW))
    stats = ls.Stats()
    ls.fetch_apply_urls(f, cards, 2, stats)
    assert stats.details == 2
    assert cards[0].apply_url.startswith("https://boards.greenhouse.io") and cards[2].apply_url is None


# ------------------------------------------------------------------ queries file / SQL
def test_shipped_queries_file_valid():
    plan = ls.load_queries(ls.DEFAULT_QUERIES_FILE)
    assert len(plan["keywords"]) >= 30
    assert {"India", "United States", "Remote", "Bengaluru"} <= set(plan["locations"])
    assert plan["pages_per_query"] == 5


def test_load_queries_rejects_empty(tmp_path):
    p = tmp_path / "q.json"
    p.write_text(json.dumps({"keywords": [], "locations": ["India"]}))
    with pytest.raises(ValueError):
        ls.load_queries(p)


def test_posting_sql_matches_contract():
    sql = ls.posting_sql("crawler")
    assert "ON CONFLICT (linkedin_job_id) WHERE linkedin_job_id IS NOT NULL DO UPDATE SET" in sql
    assert "last_seen_at = now(), status = 'open'" in sql
    assert "COALESCE(EXCLUDED.reposted_at, crawler.job_posting.reposted_at)" in sql
    assert ("(EXCLUDED.reposted_at IS NOT NULL AND EXCLUDED.reposted_at "
            "IS DISTINCT FROM crawler.job_posting.reposted_at)::int") in sql
    assert "'linkedin','open'" in sql
    assert "ON CONFLICT (slug) DO NOTHING" in ls.company_sql("crawler")
    assert "'unknown'" in ls.company_sql("crawler")


def test_posting_args_arity_matches_placeholders():
    kept, _ = _classified()
    args = ls.posting_args(kept["4000000001"], None)
    n = max(int(x) for x in re.findall(r"\$(\d+)", ls.posting_sql("crawler")))
    assert len(args) == n == 12
    assert args[0] == "acme-technologies-pvt-ltd" and args[4] == "4000000001"
    assert args[3] == "https://www.linkedin.com/jobs/view/4000000001/"


def test_stable_reposted_at_avoids_double_count():
    kept, _ = _classified()
    card = kept["4000000003"]                       # "Reposted 3 hours ago"
    stored = card.reposted_at - timedelta(hours=2)  # same repost seen by an earlier run
    assert ls.stable_reposted_at(card, stored) == stored
    far = card.reposted_at - timedelta(days=3)      # genuinely new repost
    assert ls.stable_reposted_at(card, far) == card.reposted_at
    assert ls.stable_reposted_at(card, None) == card.reposted_at


def test_schema_name_validation(monkeypatch):
    monkeypatch.setenv("PGPIPE_SCHEMA", "crawler; drop")
    with pytest.raises(ValueError):
        ls._schema()


def test_dsn_fallback(monkeypatch):
    monkeypatch.delenv("CRAWLER_DATABASE_URL", raising=False)
    monkeypatch.setenv("LOCAL_DATABASE_URL", "postgresql://x")
    assert ls.get_dsn() == "postgresql://x"
    monkeypatch.delenv("LOCAL_DATABASE_URL")
    with pytest.raises(ValueError):
        ls.get_dsn()


def test_missing_columns_message():
    class Conn:
        async def fetch(self, *a):
            return [{"column_name": "id"}]

    with pytest.raises(RuntimeError, match="jobs migrate"):
        asyncio.run(ls.check_schema(Conn(), "crawler"))
