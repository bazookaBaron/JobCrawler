"""Unit tests for the market-report pipeline (src/report/). No network, no DB."""

from __future__ import annotations

import math
from datetime import date

import polars as pl
import pytest

from src.core.monitors import DiscoveredJob
from src.report import build, classify, derive, geo, rollup, salary, sections, state
from src.report.crawl import SCHEMA, board_shard

USD = {"USD": 1.0, "GBP": 1.25, "EUR": 1.1}
DAY = date(2026, 10, 9)


# --- geo ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw, country, city",
    [
        ("San Francisco, CA", "US", "San Francisco"),
        ("Toronto, ON", "CA", "Toronto"),
        ("Perth, WA", "AU", "Perth"),
        ("Seattle, WA", "US", "Seattle"),
        ("Bangalore, Karnataka, India", "IN", "Bengaluru"),
        ("Remote - US", "US", None),
        ("Remote in USA", "US", None),
        ("London, UK", "GB", "London"),
        ("Bay Area", "US", "San Francisco"),
        ("Hybrid - Paris, France", "FR", "Paris"),
        ("Multiple Locations", None, None),
    ],
)
def test_geo_resolve(raw, country, city):
    p = geo.resolve(raw)
    assert (p.country, p.city) == (country, city)


def test_geo_flags_and_region_hint():
    p = geo.resolve("Remote (EMEA)")
    assert p.remote and p.country is None and p.region == "EMEA"
    assert geo.resolve("Hybrid - Berlin").hybrid


# --- classify -----------------------------------------------------------------
@pytest.mark.parametrize(
    "title, profile, family",
    [
        ("Senior Software Engineer, Backend", "software-engineer", "software-engineering"),
        ("Registered Nurse - ICU", "registered-nurse", "healthcare"),
        ("Barista", "food-service", "hospitality-retail"),
        ("Server Administrator", "it-support", "infrastructure-security"),
        ("Head of Finance", "finance-manager", "corporate-functions"),
        ("Civil Engineer", "engineer-other", "engineering"),
        ("Zzz Qqq", "other", "other"),
    ],
)
def test_profile(title, profile, family):
    assert classify.classify_profile(title) == (profile, family)


def test_seniority_and_title_cleaning():
    assert classify.classify_seniority("Staff Data Scientist") == "staff"
    assert classify.classify_seniority("Software Engineer") == "mid"
    assert classify.classify_seniority("Barista") == "unspecified"
    assert classify.clean_title("Senior Software Engineer II, Payments") == "Software Engineer"
    assert classify.clean_title("Softwareentwickler (m/w/d)") == "Softwareentwickler"


def test_description_signals():
    assert classify.classify_education("Bachelor's or Master's degree") == "bachelor"
    assert classify.classify_education("nothing here") == "none_stated"
    assert classify.classify_visa("We are unable to sponsor visas.") == "not_offered"
    assert classify.classify_visa("Visa sponsorship is available.") == "offered"
    assert not classify.has_equity("We value diversity, equity and inclusion.")
    assert classify.has_equity("Competitive equity package and RSUs")
    assert classify.yoe_band(None) == "unspecified" and classify.yoe_band(5) == "5-8"


# --- salary -------------------------------------------------------------------
def test_salary_structured_wins_and_annualises():
    lo, hi, cur, unit, src = salary.normalize(
        {"currency": "usd", "min": 40, "max": 50, "unit": "hour"}, "<p>$150,000 - $200,000</p>"
    )
    assert (lo, hi, cur, unit, src) == (40 * 2080, 50 * 2080, "USD", "hour", "structured")


def test_salary_from_description_and_none():
    lo, hi, cur, _, src = salary.normalize(
        None, "<p>The base salary range is $150,000 - $200,000 per year.</p>"
    )
    assert (lo, hi, cur, src) == (150000, 200000, "USD", "description")
    assert salary.normalize(None, "<p>Great culture</p>")[-1] == "none"


def test_usd_annual_filters():
    assert salary.usd_annual(100_000, 120_000, "GBP", USD) == pytest.approx(137_500)
    assert salary.usd_annual(10_000, 90_000, "USD", USD) is None  # range too wide
    assert salary.usd_annual(1_000, 1_200, "USD", USD) is None  # implausibly low
    assert salary.usd_annual(100_000, 120_000, "XYZ", USD) is None  # no FX


# --- state --------------------------------------------------------------------
def test_state_roundtrip_and_compare():
    base = state.compare(None, [1, 2, 3], [10, 10, 20], [100, None, 99], set(), 100)
    assert base.removed is None and base.is_new == [False, False, False]
    blob = state.encode(base.new_state)
    prev = state.decode(blob)
    assert list(prev.keys) == [1, 2, 3]
    # day 2: job 1 gone, job 3's board failed (carried), job 4 new
    cmp = state.compare(prev, [2, 4], [10, 10], [None, 101], {20}, 101)
    assert cmp.is_new == [False, True]
    assert cmp.removed == 1 and cmp.removed_days == {"1": 1}
    assert cmp.carried == 1
    assert sorted(cmp.new_state.keys) == [2, 3, 4]


# --- derive / rollup / sections ---------------------------------------------------
def _job(i: int, title: str, loc: str, desc: str, sal: dict | None = None) -> DiscoveredJob:
    return DiscoveredJob(
        url=f"https://example.com/{i}",
        title=title,
        description=desc,
        locations=[loc],
        date_posted="2026-10-08T00:00:00Z",
        base_salary=sal,
    )


def _frame(n: int, offset: int = 0) -> pl.DataFrame:
    rows = []
    for i in range(offset, offset + n):
        title = ["Senior Software Engineer", "Registered Nurse", "Account Executive"][i % 3]
        loc = ["San Francisco, CA", "London, UK", "Remote - US"][i % 3]
        desc = "<p>5+ years of experience. Bachelor's degree. Python and AWS.</p>"
        sal = {
            "currency": "USD",
            "min": 100_000 + 1000 * (i % 50),
            "max": 120_000 + 1000 * (i % 50),
            "unit": "year",
        }
        rows.append(
            derive.row(
                f"co{i % 7}", f"co{i % 7}-board", "greenhouse", _job(i, title, loc, desc, sal)
            )
        )
    return pl.DataFrame(rows, schema=SCHEMA)


def _statuses(df: pl.DataFrame) -> list[dict]:
    return [
        {
            "board_slug": b,
            "company_slug": b.split("-")[0],
            "monitor_type": "greenhouse",
            "status": "ok",
            "jobs": 0,
        }
        for b in df.get_column("board_slug").unique().to_list()
    ]


def test_derive_row_fields():
    r = derive.row(
        "acme",
        "acme-gh",
        "greenhouse",
        _job(
            1,
            "Senior Software Engineer",
            "Berlin, Germany",
            "<p>The salary range is €60.000 - €75.000 per year. "
            "3+ years of experience with Python.</p>",
        ),
    )
    assert r["profile"] == "software-engineer" and r["seniority"] == "senior"
    assert r["country"] == "DE" and r["city"] == "Berlin"
    assert "python" in r["technologies"]
    assert r["yoe_min"] == 3.0
    assert set(derive.COLUMNS) == set(r)


def test_day_rollup_and_exact_median():
    df = _frame(300)
    day = build.compute_day(df, _statuses(df), USD, None, None, DAY)
    kpi, sec, base = sections.build_sections(day["rollup"], None)
    assert kpi["total_jobs"] == 300 and kpi["new_jobs"] is None  # baseline run
    exact = build.enrich(df, USD, 0)["sal_usd"].median()
    assert abs(kpi["salary_median_usd"] - exact) / exact < 0.01
    fams = {r["bucket"]: r["jobs"] for r in sec["profiles"]["job_family"]}
    assert fams["software-engineering"] == 100 and fams["healthcare"] == 100
    cells = sec["experience"]["cross"]["profile|yoe_band"]
    assert any(c["a"] == "software-engineer" and c["b"] == "5-8" for c in cells)
    assert sec["salary"]["histograms"]["overall"]


def test_second_day_new_removed_and_week_merge():
    d1_df = _frame(300)
    d1 = build.compute_day(d1_df, _statuses(d1_df), USD, None, None, DAY)
    k1, s1, base1 = sections.build_sections(d1["rollup"], None)
    base1["date"] = DAY.isoformat()
    d2_df = pl.concat([d1_df.head(250), _frame(20, offset=1000)])
    d2 = build.compute_day(
        d2_df, _statuses(d2_df), USD, d1["comparison"].new_state, base1, date(2026, 10, 10)
    )
    k2, s2, _ = sections.build_sections(d2["rollup"], base1)
    assert (k2["new_jobs"], k2["removed_jobs"]) == (20, 50)
    assert s2["profiles"]["profile"][0]["d_jobs"] is not None

    week = rollup.merge(rollup.merge({}, d1["rollup"]), d2["rollup"])
    week["days"] = 2
    wk, _, _ = sections.build_sections(week, None)
    assert wk["total_jobs"] == pytest.approx((300 + 270) / 2)
    assert wk["removed_jobs"] == 50

    # re-running day 2: subtracting it restores day 1 exactly
    rollup.merge(week, d2["rollup"], -1)
    week["days"] = 1
    assert week["dims"] == d1["rollup"]["dims"]


def test_quantiles_from_hist():
    hist: dict[str, int] = {}
    for v in range(50_000, 150_001, 1000):
        b = str(round(math.log(v) * rollup.SAL_SCALE))
        hist[b] = hist.get(b, 0) + 1
    med = sections._q(hist, (0.5,), rollup.SAL_SCALE)[0]
    assert abs(med - 100_000) / 100_000 < 0.01


def test_board_shards_are_disjoint_and_stable():
    slugs = [f"b{i}" for i in range(200)]
    shards = {s: board_shard(s, 6) for s in slugs}
    assert set(shards.values()) <= set(range(6))
    assert shards == {s: board_shard(s, 6) for s in slugs}
    assert build.iso_week(date(2026, 10, 9)) == "2026-W41"
    assert build.week_bounds(date(2026, 10, 9)) == (date(2026, 10, 5), date(2026, 10, 11))
