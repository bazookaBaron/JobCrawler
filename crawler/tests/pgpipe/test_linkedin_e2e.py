"""End-to-end: LinkedIn guest cards -> persist() -> crawler schema -> build_feed().

Exercises the contract between linkedin_search.py and schema.sql together
(embedded Postgres; skipped when pixeltable-pgserver is unavailable).
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("pixeltable_pgserver")
psycopg2 = pytest.importorskip("psycopg2")
pytest.importorskip("asyncpg")

from src.pgpipe import linkedin_search as li  # noqa: E402

from test_pg_integration import SCHEMA, _company, _reset, db, pg, q  # noqa: E402,F401

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "linkedin"


def _cards():
    cards = li.parse_cards((FIX / "search_page_full.html").read_text(encoding="utf-8"))
    assert cards, "fixture should contain cards"
    return cards


def _persist(uri, cards, keep_days=36500, stats=None):
    # fixture cards carry fixed old dates; the retention window is exercised separately
    return asyncio.run(li.persist(uri, SCHEMA, cards, stats or li.Stats(), keep_days=keep_days))


def test_linkedin_persist_build_feed_and_repost_stability(db):
    uri, conn = db
    _reset(conn)
    cards = _cards()

    n1 = _persist(uri, cards)
    rows = q(conn, f"SELECT linkedin_job_id, source, company_key, dedupe_key, eff_at, status, id "
                   f"FROM {SCHEMA}.job_posting WHERE source='linkedin' ORDER BY linkedin_job_id")
    assert len(rows) == len({c.job_id for c in cards})
    assert all(r[1] == "linkedin" and r[2] and r[3] and r[4] and r[5] == "open" for r in rows)
    ids_before = {r[0]: r[6] for r in rows}

    # companies auto-inserted for LinkedIn-only employers
    n_comp = q(conn, f"SELECT count(*) FROM {SCHEMA}.company")[0][0]
    assert n_comp >= 1

    # feed contains them, positions are 1..N unique
    feed = q(conn, f"SELECT feed_position FROM {SCHEMA}.job_feed ORDER BY feed_position")
    assert [f[0] for f in feed] == list(range(1, len(feed) + 1))
    assert n1 == len(feed)

    # second identical run: same ids, no repost_count inflation
    _persist(uri, cards)
    rows2 = q(conn, f"SELECT linkedin_job_id, id, repost_count FROM {SCHEMA}.job_posting "
                    f"WHERE source='linkedin'")
    assert {r[0]: r[1] for r in rows2} == ids_before
    assert max(r[2] for r in rows2) <= 1


def test_linkedin_duplicate_of_ats_job_is_folded(db):
    uri, conn = db
    _reset(conn)
    card = _cards()[0]
    slug = li.slugify(card.company_name)
    _company(conn, slug, card.company_name)
    q(conn, f"INSERT INTO {SCHEMA}.job_posting (company_slug, title, url, location, source, status, "
            f"posted_at, first_seen_at, last_seen_at) VALUES (%s,%s,%s,%s,'greenhouse','open',now(),now(),now())",
      (slug, card.title, "https://boards.greenhouse.io/x/jobs/1", card.location))

    _persist(uri, [card])
    dup = q(conn, f"SELECT duplicate_of FROM {SCHEMA}.job_posting WHERE source='linkedin'")[0][0]
    assert dup is not None
    alt = q(conn, f"SELECT alt_urls::text FROM {SCHEMA}.job_posting WHERE source='greenhouse'")[0][0]
    assert "linkedin.com/jobs/view" in alt
    feed_sources = [r[0] for r in q(conn, f"SELECT source FROM {SCHEMA}.job_feed")]
    assert feed_sources == ["greenhouse"]


def _card(job_id, posted_ago, repost_ago=None):
    c = _cards()[0]
    now = datetime.now(timezone.utc)
    return replace(c, job_id=job_id, posted_at=now - posted_ago,
                   reposted_at=(now - repost_ago) if repost_ago else None,
                   is_repost=repost_ago is not None)


def test_old_linkedin_card_skipped_but_repost_inside_window_kept(db):
    uri, conn = db
    _reset(conn)
    q(conn, f"TRUNCATE {SCHEMA}.seen_posting")
    old = _card("9000000001", timedelta(days=10))
    repost = _card("9000000002", timedelta(days=10), repost_ago=timedelta(hours=2))
    fresh = _card("9000000003", timedelta(hours=4))
    for c in (old, repost, fresh):
        c.url = li.JOB_URL.format(job_id=c.job_id)
    # the repost was first seen 5 days ago (e.g. earlier crawl, row since expired)
    q(conn, f"INSERT INTO {SCHEMA}.seen_posting (key, first_seen_at, posted_at, last_seen_at) "
            f"VALUES ({SCHEMA}.posting_key('9000000002','linkedin',%s,NULL,%s), "
            "now() - interval '5 days', now() - interval '10 days', now() - interval '1 day')",
      (li.slugify(repost.company_name), repost.url))
    stats = li.Stats()
    _persist(uri, [old, repost, fresh], keep_days=2, stats=stats)
    rows = {r[0]: r for r in q(conn, f"SELECT linkedin_job_id, first_seen_at, reposted_at, eff_at, repost_count "
                                     f"FROM {SCHEMA}.job_posting ORDER BY 1")}
    assert set(rows) == {"9000000002", "9000000003"}          # 10-day-old non-repost never inserted
    assert stats.skipped_old == 1 and stats.new == 2
    _, first_seen, reposted, eff, _ = rows["9000000002"]
    assert datetime.now(timezone.utc) - first_seen > timedelta(days=4, hours=23)  # original, not "now"
    assert reposted is not None and eff == reposted            # shows in the feed as reposted
    # the skipped card is still remembered for the TTL
    assert q(conn, f"SELECT count(*) FROM {SCHEMA}.seen_posting")[0][0] == 3
    # re-run: nothing changes, the skipped card stays skipped
    _persist(uri, [old, repost, fresh], keep_days=2)
    assert q(conn, f"SELECT count(*) FROM {SCHEMA}.job_posting")[0][0] == 2
