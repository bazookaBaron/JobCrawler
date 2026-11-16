"""Integration tests against a real (embedded) Postgres.

Uses `pixeltable-pgserver` (pip install pixeltable-pgserver psycopg2-binary) to
start a throw-away server; the whole module is skipped when it is unavailable.
Covers: schema idempotency, triggers, build_feed()/feed_page() vs the
pure-Python reference, and the no-churn retention behaviour of ingest.
"""
from __future__ import annotations

import random
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pgserver = pytest.importorskip("pixeltable_pgserver")
psycopg2 = pytest.importorskip("psycopg2")
asyncpg = pytest.importorskip("asyncpg")

from src.core.monitors import DiscoveredJob  # noqa: E402
from src.pgpipe import cleanup, ingest  # noqa: E402
from src.pgpipe.config import SCHEMA_SQL  # noqa: E402
from src.pgpipe.feedref import cooldown_order  # noqa: E402

SCHEMA = "crawler"


@pytest.fixture(scope="module")
def pg():
    d = tempfile.mkdtemp(prefix="pgpipe_test_")
    try:
        srv = pgserver.get_server(Path(d), cleanup_mode="stop")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"embedded postgres unavailable: {exc}")
    uri = srv.get_uri()
    conn = psycopg2.connect(uri)
    conn.autocommit = True
    cur = conn.cursor()
    for role in ("anon", "authenticated", "service_role"):
        cur.execute(f"CREATE ROLE {role} NOLOGIN")
    cur.execute("CREATE SCHEMA IF NOT EXISTS public")
    yield uri, conn
    conn.close()
    srv.cleanup()


def _apply(conn) -> None:
    sql = Path(SCHEMA_SQL).read_text(encoding="utf-8").replace("{{SCHEMA}}", SCHEMA)
    with conn.cursor() as cur:
        cur.execute(sql)


@pytest.fixture(scope="module")
def db(pg):
    uri, conn = pg
    _apply(conn)
    _apply(conn)  # idempotent: second run must be a clean no-op
    return uri, conn


def q(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if cur.description else None


def _reset(conn):
    q(conn, f"TRUNCATE {SCHEMA}.job_feed, {SCHEMA}.job_posting RESTART IDENTITY CASCADE")


def _company(conn, slug, name=None):
    q(conn, f"INSERT INTO {SCHEMA}.company (slug, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
      (slug, name or slug.title()))


def test_schema_objects_and_idempotent_migration(db):
    _, conn = db
    cols = {r[0] for r in q(conn, "SELECT column_name FROM information_schema.columns "
                                  "WHERE table_schema=%s AND table_name='job_posting'", (SCHEMA,))}
    for c in ("linkedin_job_id", "apply_url", "alt_urls", "company_name", "company_key",
              "dedupe_key", "reposted_at", "repost_count", "duplicate_of", "eff_at",
              "closed_at", "last_seen_run_id"):
        assert c in cols
    feed_cols = [r[0] for r in q(conn, "SELECT column_name FROM information_schema.columns "
                                       "WHERE table_schema=%s AND table_name='job_feed' "
                                       "ORDER BY ordinal_position", (SCHEMA,))]
    assert feed_cols == ["feed_position", "id", "company_slug", "company_key", "company_name",
                         "company_type", "title", "url", "apply_url", "alt_urls", "location",
                         "location_type", "country", "seniority", "source", "posted_at",
                         "reposted_at", "repost_count", "eff_at", "first_seen_at"]
    assert q(conn, "SELECT 1 FROM pg_roles WHERE rolname='crawler_writer'")
    idx = {r[0] for r in q(conn, "SELECT indexname FROM pg_indexes WHERE schemaname=%s", (SCHEMA,))}
    assert {"job_posting_linkedin_uidx", "job_posting_src_ext_uidx",
            "job_feed_position_uidx", "job_feed_company_eff_idx"} <= idx


def test_company_autoinsert_and_triggers(db):
    _, conn = db
    _reset(conn)
    q(conn, f"INSERT INTO {SCHEMA}.company (slug) VALUES ('acme-technologies-inc')")
    name, ctype = q(conn, f"SELECT name, company_type FROM {SCHEMA}.company "
                          "WHERE slug='acme-technologies-inc'")[0]
    assert name == "Acme Technologies Inc" and ctype == "unknown"
    q(conn, f"INSERT INTO {SCHEMA}.job_posting (company_slug, source, url, title, location, "
            "linkedin_job_id, company_name, posted_at, reposted_at) VALUES "
            "('acme-technologies-inc','linkedin','https://www.linkedin.com/jobs/view/1/',"
            "'Senior Software Engineer','Bengaluru, India','1','Acme Technologies, Inc.',"
            "now() - interval '5 hours', now() - interval '1 hour')")
    key, dk, eff, posted, rep = q(
        conn, f"SELECT company_key, dedupe_key, eff_at, posted_at, reposted_at "
              f"FROM {SCHEMA}.job_posting")[0]
    from src.pgpipe.keys import dedupe_key
    assert key == "acme"
    assert dk == dedupe_key("acme", "Senior Software Engineer", "Bengaluru, India")
    assert eff == rep  # GREATEST(posted_at, reposted_at)
    with pytest.raises(psycopg2.errors.UniqueViolation):
        q(conn, f"INSERT INTO {SCHEMA}.job_posting (company_slug, source, url, linkedin_job_id) "
                "VALUES ('acme-technologies-inc','linkedin','https://x/2','1')")


def _seed_feed(conn, seed, n_jobs, n_companies):
    rng = random.Random(seed)
    _reset(conn)
    for c in range(n_companies):
        _company(conn, f"co{c:02d}", f"Company {c:02d}")
    now = datetime.now(timezone.utc)
    for i in range(n_jobs):
        c = int((rng.random() ** 2) * n_companies)  # skewed: a few dominant companies
        age = timedelta(minutes=rng.randint(1, 23 * 60))
        q(conn, f"INSERT INTO {SCHEMA}.job_posting (company_slug, source, url, title, location, "
                "country, seniority, location_type, posted_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
          (f"co{c:02d}", rng.choice(["greenhouse", "lever"]), f"https://j/{i}",
           f"Engineer {i}", rng.choice(["Austin, TX", "Pune, India", "Remote"]),
           rng.choice(["US", "IN", "remote"]), rng.choice(["junior", "senior"]),
           rng.choice(["remote", "onsite"]), now - age))


def test_build_feed_matches_python_reference(db):
    _, conn = db
    _seed_feed(conn, 11, 300, 25)
    # 3 old + 2 closed rows must be excluded
    q(conn, f"INSERT INTO {SCHEMA}.job_posting (company_slug, source, url, title, posted_at) "
            "VALUES ('co00','greenhouse','https://j/old','Old Engineer', now() - interval '3 days')")
    q(conn, f"INSERT INTO {SCHEMA}.job_posting (company_slug, source, url, title, posted_at, status) "
            "VALUES ('co01','greenhouse','https://j/closed','Closed Engineer', now(), 'closed')")
    n = q(conn, f"SELECT {SCHEMA}.build_feed()")[0][0]
    assert n == 300
    rows = q(conn, f"SELECT feed_position, id, company_key FROM {SCHEMA}.job_feed ORDER BY feed_position")
    assert [r[0] for r in rows] == list(range(1, n + 1))
    base = q(conn, f"SELECT id, company_key FROM {SCHEMA}.job_posting WHERE status='open' "
                   "AND eff_at >= now() - interval '24 hours' ORDER BY eff_at DESC, id DESC")
    order = cooldown_order([b[1] for b in base])
    assert [r[1] for r in rows] == [base[i][0] for i in order]
    # rebuilding is stable
    assert q(conn, f"SELECT {SCHEMA}.build_feed()")[0][0] == 300
    # the cooldown really spreads things: no repeat within 9 unless forced
    seq = [r[2] for r in rows]
    repeats = sum(1 for p, k in enumerate(seq) if k in seq[max(0, p - 9):p])
    assert repeats < len(seq) * 0.5


def test_feed_page_filters_and_interleave(db):
    _, conn = db
    _seed_feed(conn, 5, 200, 20)
    q(conn, f"SELECT {SCHEMA}.build_feed()")
    sig = (f"SELECT * FROM {SCHEMA}.feed_page(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
    cols = [r[0] for r in q(conn, "SELECT column_name FROM information_schema.columns WHERE "
                                  "table_schema=%s AND table_name='job_feed' ORDER BY ordinal_position",
                            (SCHEMA,))]
    # unfiltered, paged
    p1 = q(conn, sig, (None,) * 9 + (0, 25))
    p2 = q(conn, sig, (None,) * 9 + (25, 25))
    assert len(p1) == 25 and len(p2) == 25
    assert len(p1[0]) == len(cols) + 1
    assert p1[0][-1] == 200 and p2[0][-1] == 200  # total_count
    ids = [r[1] for r in p1 + p2]
    assert len(set(ids)) == 50
    # matches the reference over the filtered set
    base = q(conn, f"SELECT id, company_key FROM {SCHEMA}.job_feed "
                   "WHERE country = ANY(%s) AND seniority = ANY(%s) ORDER BY eff_at DESC, id DESC",
             (["US", "remote"], ["senior"]))
    got = q(conn, sig, (None, None, None, None, ["senior"], None, ["US", "remote"], None, None, 0, 30))
    order = cooldown_order([b[1] for b in base], limit=30)
    assert [r[1] for r in got] == [base[i][0] for i in order]
    assert got and got[0][-1] == len(base)
    # '|'-joined multi location (OR, ILIKE), text search, hours, no match
    multi = q(conn, sig, (None, None, "austin|PUNE", None, None, None, None, None, None, 0, 500))
    assert multi and all(("Austin" in r[10]) or ("Pune" in r[10]) for r in multi)
    assert multi[0][-1] == len(multi)
    assert q(conn, sig, ("engineer 7", None, None, None, None, None, None, None, None, 0, 50))
    assert q(conn, sig, ("zzz-nothing", None, None, None, None, None, None, None, None, 0, 50)) == []
    assert len(q(conn, sig, (None,) * 8 + (1, 0, 500))) <= 200


def test_build_feed_marks_linkedin_duplicates(db):
    _, conn = db
    _reset(conn)
    _company(conn, "acme", "Acme")
    q(conn, f"INSERT INTO {SCHEMA}.job_posting (company_slug, source, url, title, location, posted_at) "
            "VALUES ('acme','greenhouse','https://boards.greenhouse.io/acme/1','Backend Engineer',"
            "'Pune, India', now() - interval '2 hours')")
    q(conn, f"INSERT INTO {SCHEMA}.job_posting (company_slug, source, url, title, location, posted_at, "
            "linkedin_job_id, company_name) VALUES ('acme','linkedin',"
            "'https://www.linkedin.com/jobs/view/99/','Backend  Engineer','Pune, Maharashtra, India',"
            "now() - interval '1 hour','99','Acme Inc.')")
    assert q(conn, f"SELECT {SCHEMA}.build_feed()")[0][0] == 1
    ats_id = q(conn, f"SELECT id FROM {SCHEMA}.job_posting WHERE source='greenhouse'")[0][0]
    dup, = q(conn, f"SELECT duplicate_of FROM {SCHEMA}.job_posting WHERE source='linkedin'")[0]
    assert dup == ats_id
    alt = q(conn, f"SELECT alt_urls FROM {SCHEMA}.job_posting WHERE id=%s", (ats_id,))[0][0]
    assert alt == [{"url": "https://www.linkedin.com/jobs/view/99/", "source": "linkedin"}]
    # ATS twin closes -> LinkedIn row comes back
    q(conn, f"UPDATE {SCHEMA}.job_posting SET status='closed' WHERE id=%s", (ats_id,))
    assert q(conn, f"SELECT {SCHEMA}.build_feed()")[0][0] == 1
    assert q(conn, f"SELECT source FROM {SCHEMA}.job_feed")[0][0] == "linkedin"


def test_writer_role_grants(db):
    _, conn = db
    q(conn, "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='ci_test') "
            "THEN CREATE ROLE ci_test LOGIN IN ROLE crawler_writer; END IF; END $$")
    q(conn, "SET ROLE ci_test")
    try:
        _reset_ok = q(conn, f"SELECT {SCHEMA}.build_feed()")
        assert _reset_ok is not None
        q(conn, f"INSERT INTO {SCHEMA}.board_health (board_slug, event) VALUES ('x','gone')")
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            q(conn, f"SELECT * FROM {SCHEMA}.feed_page(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,1)")
    finally:
        q(conn, "RESET ROLE")


# --- ingest / retention (asyncpg, real DB) -------------------------------------
@pytest.fixture
async def pool(db):
    uri, conn = db
    _reset(conn)
    q(conn, f"TRUNCATE {SCHEMA}.job_board, {SCHEMA}.company CASCADE")
    _company(conn, "acme", "Acme")
    q(conn, f"INSERT INTO {SCHEMA}.job_board (board_slug, company_slug, board_url, monitor_type) "
            "VALUES ('acme-gh','acme','https://boards.greenhouse.io/acme','greenhouse')")
    p = await asyncpg.create_pool(uri, min_size=1, max_size=3, statement_cache_size=0,
                                  server_settings={"search_path": f"{SCHEMA},public"})
    yield p
    await p.close()


class _Boards:
    def __init__(self, jobs):
        self.jobs = jobs


async def _crawl(pool, monkeypatch, jobs, run_id):
    async def fake(_mt, _bd, _http):
        return list(jobs)
    monkeypatch.setattr(ingest, "_discover", fake)
    board = await pool.fetchrow("SELECT board_slug, company_slug, board_url, monitor_type, "
                                "monitor_config FROM job_board WHERE board_slug='acme-gh'")
    return await ingest.crawl_board(pool, board, None, run_id)


def _job(n, date=None, **kw):
    return DiscoveredJob(url=f"https://boards.greenhouse.io/acme/jobs/{n}?gh_jid={n}&utm_source=x",
                         title=f"Software Engineer {n}", locations=["Austin, TX"],
                         date_posted=date, source_identity=f"greenhouse:acme:{n}", **kw)


async def test_ids_and_posted_at_stable_across_crawls(pool, monkeypatch):
    await _crawl(pool, monkeypatch, [_job(1), _job(2, "2026-10-01T00:00:00+00:00")], 1)
    before = {r["external_id"]: r for r in await pool.fetch("SELECT * FROM job_posting")}
    assert len(before) == 2
    assert all("utm_source" not in r["url"] for r in before.values())
    await pool.execute("UPDATE job_posting SET first_seen_at = first_seen_at - interval '10 days', "
                       "posted_at = posted_at - interval '10 days' WHERE external_id LIKE '%:1'")
    old_posted = await pool.fetchval("SELECT posted_at FROM job_posting WHERE external_id LIKE '%:1'")
    await _crawl(pool, monkeypatch, [_job(1), _job(2, "2026-10-01T00:00:00+00:00")], 2)
    after = {r["external_id"]: r for r in await pool.fetch("SELECT * FROM job_posting")}
    assert {k: v["id"] for k, v in after.items()} == {k: v["id"] for k, v in before.items()}
    # undated job keeps its original posted_at (not reset to now)
    assert after["greenhouse:acme:1"]["posted_at"] == old_posted
    assert after["greenhouse:acme:2"]["posted_at"] == datetime(2026, 10, 1, tzinfo=timezone.utc)
    # open rows are never hard-deleted, however old
    assert await cleanup.hard_delete_stale(pool, 1) == 0


async def test_missing_job_closed_only_on_good_crawl_and_reopens(pool, monkeypatch):
    await _crawl(pool, monkeypatch, [_job(1), _job(2)], 1)
    ids = {r["external_id"]: r["id"] for r in await pool.fetch("SELECT id, external_id FROM job_posting")}
    # empty listing (API glitch): nothing closed
    res = await _crawl(pool, monkeypatch, [], 2)
    assert res["closed_missing"] == 0
    assert await pool.fetchval("SELECT count(*) FROM job_posting WHERE status='open'") == 2
    # job 2 disappears in a successful crawl -> closed, job 1 stays open
    res = await _crawl(pool, monkeypatch, [_job(1)], 3)
    assert res["closed_missing"] == 1
    st = {r["external_id"]: r["status"] for r in await pool.fetch("SELECT external_id, status FROM job_posting")}
    assert st == {"greenhouse:acme:1": "open", "greenhouse:acme:2": "closed"}
    assert await pool.fetchval("SELECT last_success_run_id FROM job_board") == 3
    # it comes back: same id, reopened, closed_at cleared
    await _crawl(pool, monkeypatch, [_job(1), _job(2)], 4)
    row = await pool.fetchrow("SELECT id, status, closed_at FROM job_posting WHERE external_id='greenhouse:acme:2'")
    assert (row["id"], row["status"], row["closed_at"]) == (ids["greenhouse:acme:2"], "open", None)


async def test_url_change_keeps_row_id(pool, monkeypatch):
    await _crawl(pool, monkeypatch, [_job(1)], 1)
    old_id = await pool.fetchval("SELECT id FROM job_posting")
    moved = DiscoveredJob(url="https://job-boards.greenhouse.io/acme/jobs/1", title="Software Engineer 1",
                          locations=["Austin, TX"], source_identity="greenhouse:acme:1")
    await _crawl(pool, monkeypatch, [moved], 2)
    rows = await pool.fetch("SELECT id, url FROM job_posting")
    assert [(r["id"], r["url"]) for r in rows] == [(old_id, "https://job-boards.greenhouse.io/acme/jobs/1")]


async def test_stale_close_prune_and_delete_windows(pool, monkeypatch):
    await _crawl(pool, monkeypatch, [_job(i) for i in range(1, 6)], 1)
    # 40h unseen board job -> closed; 20h unseen -> still open
    await pool.execute("UPDATE job_posting SET last_seen_at = now() - interval '40 hours' "
                       "WHERE external_id='greenhouse:acme:1'")
    await pool.execute("UPDATE job_posting SET last_seen_at = now() - interval '20 hours' "
                       "WHERE external_id='greenhouse:acme:2'")
    # linkedin: 31h -> closed (30h rule), 29h -> open
    await pool.execute("INSERT INTO job_posting (company_slug, source, url, title, linkedin_job_id, last_seen_at) "
                       "VALUES ('acme','linkedin','https://www.linkedin.com/jobs/view/1/','SWE','1', now() - interval '31 hours'),"
                       "('acme','linkedin','https://www.linkedin.com/jobs/view/2/','SWE','2', now() - interval '29 hours')")
    assert await cleanup.close_stale(pool) == 2
    open_ext = {r["external_id"] or r["linkedin_job_id"] for r in
                await pool.fetch("SELECT external_id, linkedin_job_id FROM job_posting WHERE status='open'")}
    assert "greenhouse:acme:1" not in open_ext and "greenhouse:acme:2" in open_ext and "2" in open_ext
    # prune keeps newest N open, CLOSES the rest (rows survive)
    total = await pool.fetchval("SELECT count(*) FROM job_posting")
    pruned = await cleanup.prune_per_company(pool, 3)
    assert pruned >= 1
    assert await pool.fetchval("SELECT count(*) FROM job_posting") == total
    assert await pool.fetchval("SELECT count(*) FROM job_posting WHERE status='open'") == 3
    # hard delete only touches rows closed > 7 days ago
    assert await cleanup.hard_delete_stale(pool, 7) == 0
    await pool.execute("UPDATE job_posting SET closed_at = now() - interval '8 days' "
                       "WHERE status='closed' AND external_id='greenhouse:acme:1'")
    assert await cleanup.hard_delete_stale(pool, 7) == 1
    assert await pool.fetchval("SELECT count(*) FROM job_posting") == total - 1


async def test_board_gone_is_logged(pool, monkeypatch):
    from src.core.monitors import BoardGoneError

    async def gone(*_a):
        raise BoardGoneError("404")
    monkeypatch.setattr(ingest, "_discover", gone)
    board = await pool.fetchrow("SELECT board_slug, company_slug, board_url, monitor_type, "
                                "monitor_config FROM job_board")
    res = await ingest.crawl_board(pool, board, None, 5)
    assert res["gone"] is True
    assert await pool.fetchval("SELECT is_enabled FROM job_board") is False
    row = await pool.fetchrow("SELECT board_slug, event, run_id FROM board_health WHERE board_slug='acme-gh'")
    assert (row["board_slug"], row["event"], row["run_id"]) == ("acme-gh", "gone", 5)
