"""Rollup (one day, or a week's sum) -> chart-ready report sections.

The same code renders the daily row and the weekly row; for a week every
count is shown as an average per covered day (`jobs`), while flows (`new`,
`removed`) are week totals. Every list is ordered for direct charting:
[{bucket, label, ...metrics}].
"""

from __future__ import annotations

import math
from typing import Any

from src.report import classify, geo
from src.report.rollup import CROSS_SCALE, SAL_SCALE

THIN_SALARY_N = 30  # salary figures from fewer samples are flagged `thin`
MIN_BASE_FOR_PCT = 20  # risers/fallers by % need this many jobs before
TOP_MOVERS = 25
CROSS_MIN_JOBS = 3  # drop two-way cells smaller than this (per day)
HIST_STEP = 10_000  # display histogram bin width, USD
HIST_CAP = 500_000

_ORDER = {
    "seniority": classify.SENIORITY_ORDER,
    "yoe_band": classify.YOE_BANDS,
    "education": classify.EDUCATION_ORDER,
    "posting_age": ("0-1d", "2-7d", "8-14d", "15-30d", "31-90d", "90d+", "unknown"),
    "salary_band": (
        "<25k",
        "25-50k",
        "50-75k",
        "75-100k",
        "100-150k",
        "150-200k",
        "200-300k",
        "300k+",
        "undisclosed",
    ),
    "company_size": (
        "1-10",
        "11-50",
        "51-200",
        "201-500",
        "501-1k",
        "1k-5k",
        "5k-10k",
        "10k+",
        "unknown",
    ),
    "company_age": ("<5y", "5-10y", "10-20y", "20-50y", "50y+", "unknown"),
    "yoe_years": tuple([str(i) for i in range(0, 20)] + ["20+"]),
}


# --- small helpers ----------------------------------------------------------
def pct(a: Any, b: Any) -> float | None:
    return round(100.0 * a / b, 2) if b else None


def _q(hist: dict | None, qs: tuple[float, ...], scale: int) -> list[float | None]:
    """Quantiles from a sparse log-bin histogram {bin: count}."""
    if not hist:
        return [None] * len(qs)
    items = sorted((int(b), c) for b, c in hist.items() if c > 0)
    total = sum(c for _, c in items)
    if not total:
        return [None] * len(qs)
    out: list[float | None] = []
    for q in qs:
        target, run = q * total, 0
        val = None
        for b, c in items:
            run += c
            if run >= target:
                val = math.exp(b / scale)
                break
        out.append(round(val) if val is not None else None)
    return out


def _yoe_median(yh: dict | None) -> float | None:
    if not yh:
        return None
    items = sorted((int(y), c) for y, c in yh.items())
    total = sum(c for _, c in items)
    run = 0
    for y, c in items:
        run += c
        if run >= total / 2:
            return float(y)
    return None


def label_for(dim: str, bucket: str) -> str:
    if dim in ("profile",):
        return classify.profile_label(bucket)
    if dim == "job_family":
        return classify.FAMILY_LABELS.get(bucket, bucket)
    if dim == "country":
        return geo.country_name(bucket) or bucket
    if dim == "technology":
        return bucket
    return bucket


def salary_block(s: dict, days: int, scale: int = SAL_SCALE, prefix: str = "salary") -> dict:
    sn, ss = s.get("sn", 0), s.get("ss", 0)
    p10, p25, p50, p75, p90, p0, p100 = _q(
        s.get("sh"), (0.1, 0.25, 0.5, 0.75, 0.9, 0.0001, 1.0), scale
    )
    return {
        f"{prefix}_n": round(sn / days, 1) if days > 1 else sn,
        f"{prefix}_disclosure_pct": pct(sn, s.get("j", 0)),
        f"{prefix}_avg_usd": round(ss / sn) if sn else None,
        f"{prefix}_median_usd": p50,
        f"{prefix}_p10_usd": p10,
        f"{prefix}_p25_usd": p25,
        f"{prefix}_p75_usd": p75,
        f"{prefix}_p90_usd": p90,
        f"{prefix}_min_usd": p0,
        f"{prefix}_max_usd": p100,
        f"{prefix}_thin": (sn / days) < THIN_SALARY_N,
    }


def bucket_row(dim: str, bucket: str, s: dict, days: int, total_jobs: float) -> dict:
    j = s.get("j", 0)
    row = {
        "bucket": bucket,
        "label": label_for(dim, bucket),
        "jobs": round(j / days, 1) if days > 1 else j,
        "share_pct": pct(j, total_jobs),
        "companies": round(s.get("c", 0) / days, 1) if days > 1 else s.get("c", 0),
        "new": s.get("n", 0),
        "removed": s.get("rm"),
        "remote_pct": pct(s.get("r", 0), j),
        "hybrid_pct": pct(s.get("h", 0), j),
        "onsite_pct": pct(s.get("o", 0), j),
        **salary_block(s, days),
        "new_listing_salary_n": s.get("nn", 0),
        "new_listing_salary_median_usd": _q(s.get("nh"), (0.5,), SAL_SCALE)[0],
        "new_listing_salary_avg_usd": round(s["ns"] / s["nn"]) if s.get("nn") else None,
        "yoe_median": _yoe_median(s.get("yh")),
        "yoe_stated_pct": pct(s.get("yn", 0), j),
        "visa_offered_pct": pct(s.get("v", 0), j),
        "equity_pct": pct(s.get("e", 0), j),
        "premium_index": round(s["ps"] / s["pn"], 3) if s.get("pn") else None,
    }
    return {k: v for k, v in row.items() if v is not None}


def _sort(dim: str, rows: list[dict]) -> list[dict]:
    order = _ORDER.get(dim)
    if order:
        idx = {b: i for i, b in enumerate(order)}
        return sorted(rows, key=lambda r: (idx.get(r["bucket"], len(idx)), -r["jobs"]))
    return sorted(rows, key=lambda r: (-r["jobs"], r["bucket"]))


# High-cardinality dimensions are capped for display (the rollup keeps more,
# so weekly sums stay accurate for buckets that enter the top list mid-week).
DISPLAY_TOP = {"title": 300, "city": 300, "company": 300}


def dim_rows(roll: dict, dim: str, prev: dict[str, dict] | None = None) -> list[dict]:
    days = max(1, roll.get("days", 1))
    total = roll["dims"].get("_all", {}).get("all", {}).get("j", 0)
    rows = [bucket_row(dim, b, s, days, total) for b, s in roll["dims"].get(dim, {}).items()]
    if dim in DISPLAY_TOP:
        rows = sorted(rows, key=lambda r: -r["jobs"])[: DISPLAY_TOP[dim]]
    if prev is not None:
        add_deltas(rows, prev)
    return _sort(dim, rows)


def add_deltas(rows: list[dict], prev: dict[str, dict], suffix: str = "") -> None:
    """prev: {bucket: {"jobs", "share_pct", "salary_median_usd"}} of the comparison period."""
    for r in rows:
        p = prev.get(r["bucket"])
        pj = p["jobs"] if p else 0
        r[f"d_jobs{suffix}"] = round(r["jobs"] - pj, 1)
        r[f"d_jobs_pct{suffix}"] = pct(r["jobs"] - pj, pj) if pj else None
        if not suffix:
            r["d_share_pp"] = (
                round((r.get("share_pct") or 0) - (p.get("share_pct") or 0), 2) if p else None
            )
            pm = p.get("salary_median_usd") if p else None
            m = r.get("salary_median_usd")
            r["d_salary_median_usd"] = (m - pm) if (m and pm) else None
            r["d_salary_median_pct"] = pct(m - pm, pm) if (m and pm) else None


def compact(rows_by_dim: dict[str, list[dict]]) -> dict:
    """{dim: {bucket: {jobs, share_pct, salary_median_usd}}} — the comparison
    baseline kept for the next run (small: no histograms)."""
    return {
        dim: {
            r["bucket"]: {
                "jobs": r["jobs"],
                "share_pct": r.get("share_pct"),
                "salary_median_usd": r.get("salary_median_usd"),
            }
            for r in rows
        }
        for dim, rows in rows_by_dim.items()
    }


_MOVER_FIELDS = (
    "bucket",
    "label",
    "jobs",
    "d_jobs",
    "d_jobs_pct",
    "salary_median_usd",
    "d_salary_median_usd",
)


def movers(rows: list[dict], prev: dict[str, dict] | None) -> dict:
    if not prev:
        return {
            "risers": [],
            "fallers": [],
            "risers_pct": [],
            "fallers_pct": [],
            "appeared": [],
            "disappeared": [],
        }

    def slim(r: dict) -> dict:
        return {k: r.get(k) for k in _MOVER_FIELDS}

    with_base = [
        r for r in rows if (prev.get(r["bucket"]) or {}).get("jobs", 0) >= MIN_BASE_FOR_PCT
    ]
    cur = {r["bucket"] for r in rows}
    return {
        "risers": [
            slim(r)
            for r in sorted(rows, key=lambda r: -(r.get("d_jobs") or 0))[:TOP_MOVERS]
            if (r.get("d_jobs") or 0) > 0
        ],
        "fallers": [
            slim(r)
            for r in sorted(rows, key=lambda r: r.get("d_jobs") or 0)[:TOP_MOVERS]
            if (r.get("d_jobs") or 0) < 0
        ],
        "risers_pct": [
            slim(r)
            for r in sorted(with_base, key=lambda r: -(r.get("d_jobs_pct") or 0))[:TOP_MOVERS]
            if (r.get("d_jobs_pct") or 0) > 0
        ],
        "fallers_pct": [
            slim(r)
            for r in sorted(with_base, key=lambda r: r.get("d_jobs_pct") or 0)[:TOP_MOVERS]
            if (r.get("d_jobs_pct") or 0) < 0
        ],
        "appeared": [slim(r) for r in rows if r["bucket"] not in prev and r["jobs"] >= 5][
            :TOP_MOVERS
        ],
        "disappeared": [
            {"bucket": b, "prev_jobs": p["jobs"]}
            for b, p in prev.items()
            if b not in cur and p["jobs"] >= 5
        ][:TOP_MOVERS],
    }


def cross_rows(roll: dict, pair: str) -> list[dict]:
    days = max(1, roll.get("days", 1))
    a_dim, b_dim = pair.split("|")
    cells = roll["cross"].get(pair, {})
    a_tot: dict[str, int] = {}
    for key, s in cells.items():
        a = key.split("\u001f")[0]
        a_tot[a] = a_tot.get(a, 0) + s.get("j", 0)
    out = []
    for key, s in cells.items():
        if s.get("j", 0) < CROSS_MIN_JOBS * days:
            continue
        a, b = key.split("\u001f")
        sn = s.get("sn", 0)
        p25, p50, p75 = _q(s.get("sh"), (0.25, 0.5, 0.75), CROSS_SCALE)
        out.append(
            {
                "a": a,
                "a_label": label_for(a_dim, a),
                "b": b,
                "b_label": label_for(b_dim, b),
                "jobs": round(s.get("j", 0) / days, 1) if days > 1 else s.get("j", 0),
                "share_of_a_pct": pct(s.get("j", 0), a_tot.get(a, 0)),
                "new": s.get("n", 0),
                "remote_pct": pct(s.get("r", 0), s.get("j", 0)),
                "salary_n": round(sn / days, 1) if days > 1 else sn,
                "salary_avg_usd": round(s["ss"] / sn) if sn else None,
                "salary_median_usd": p50,
                "salary_p25_usd": p25,
                "salary_p75_usd": p75,
                "salary_thin": (sn / days) < THIN_SALARY_N,
                "yoe_stated_pct": pct(s.get("yn", 0), s.get("j", 0)),
            }
        )
        out[-1] = {k: v for k, v in out[-1].items() if v is not None}
    out.sort(key=lambda r: (r["a"], -r["jobs"]))
    return out


def histogram(s: dict | None, days: int) -> list[dict]:
    """Log-bin histogram -> fixed $10k display bins (job counts per day)."""
    bins: dict[int, float] = {}
    for b, c in (s or {}).get("sh", {}).items():
        v = math.exp(int(b) / SAL_SCALE)
        lo = min(int(v // HIST_STEP) * HIST_STEP, HIST_CAP)
        bins[lo] = bins.get(lo, 0) + c
    return [
        {
            "from": lo,
            "to": (lo + HIST_STEP) if lo < HIST_CAP else None,
            "count": round(c / days, 1) if days > 1 else c,
        }
        for lo, c in sorted(bins.items())
    ]


def _local_rows(entries: dict, days: int, keys: tuple[str, ...]) -> list[dict]:
    out = []
    for key, s in entries.items():
        sn = s.get("sn", 0)
        if sn < 5 * days:
            continue
        p25, p50, p75 = _q(s.get("sh"), (0.25, 0.5, 0.75), SAL_SCALE)
        row = dict(zip(keys, key.split("\u001f"), strict=True))
        row.update(
            {
                "salary_n": round(sn / days, 1) if days > 1 else sn,
                "avg": round(s["ss"] / sn) if sn else None,
                "median": p50,
                "p25": p25,
                "p75": p75,
                "thin": (sn / days) < THIN_SALARY_N,
            }
        )
        out.append(row)
    out.sort(key=lambda r: -r["salary_n"])
    return out


_SECTION_DIMS = {
    "profiles": ("job_family", "profile", "title", "seniority"),
    "experience": ("yoe_band", "yoe_years"),
    "geography": ("region", "country", "city", "multi_location"),
    "skills": ("technology",),
    "companies": ("company", "industry", "company_size", "company_age"),
    "attributes": (
        "work_mode",
        "employment_type",
        "education",
        "visa",
        "equity",
        "clearance",
        "language",
        "source",
        "posting_age",
    ),
    "salary": ("salary_band", "salary_currency", "salary_source"),
}
_SECTION_CROSS = {
    "experience": (
        "profile|yoe_band",
        "job_family|yoe_band",
        "seniority|yoe_band",
        "country|yoe_band",
    ),
    "profiles": (
        "profile|seniority",
        "job_family|seniority",
        "profile|work_mode",
        "job_family|work_mode",
        "job_family|industry",
        "job_family|company_size",
        "job_family|education",
        "industry|seniority",
    ),
    "geography": (
        "profile|country",
        "job_family|country",
        "region|job_family",
        "country|seniority",
    ),
    "skills": ("technology|job_family", "technology|yoe_band", "technology|country"),
}
MOVER_DIMS = (
    "job_family",
    "profile",
    "title",
    "seniority",
    "country",
    "city",
    "technology",
    "company",
    "industry",
)


def build_sections(roll: dict, prev: dict | None) -> tuple[dict, dict, dict]:
    """-> (kpi, sections, baseline). `prev` is the comparison baseline
    (compact()-shaped, plus "kpi") of the previous day / week, or None."""
    days = max(1, roll.get("days", 1))
    prev_dims = (prev or {}).get("dims") or {}
    rows: dict[str, list[dict]] = {}
    for section_dims in _SECTION_DIMS.values():
        for dim in section_dims:
            rows[dim] = dim_rows(roll, dim, prev_dims.get(dim) if prev else None)
    all_s = roll["dims"].get("_all", {}).get("all", {})
    overall = bucket_row("_all", "all", all_s, days, all_s.get("j", 0))

    sections: dict[str, dict[str, Any]] = {
        name: {dim: rows[dim] for dim in dims} for name, dims in _SECTION_DIMS.items()
    }
    for name, pairs in _SECTION_CROSS.items():
        sections[name]["cross"] = {p: cross_rows(roll, p) for p in pairs}
    sections["profiles"]["movers"] = {
        dim: movers(rows[dim], prev_dims.get(dim) if prev else None) for dim in MOVER_DIMS
    }

    # --- salary ----------------------------------------------------------
    sal = sections["salary"]
    sal["overall"] = {k: v for k, v in overall.items() if "salary" in k}
    overall = {
        **{
            k: None
            for k in (
                "remote_pct",
                "hybrid_pct",
                "onsite_pct",
                "yoe_median",
                "yoe_stated_pct",
                "visa_offered_pct",
                "equity_pct",
                "new_listing_salary_median_usd",
                "salary_avg_usd",
                "salary_median_usd",
                "salary_p10_usd",
                "salary_p25_usd",
                "salary_p75_usd",
                "salary_p90_usd",
                "salary_disclosure_pct",
            )
        },
        **overall,
    }
    # Salary by the core dimensions in one place. Every breakdown row in the
    # other sections (title, city, technology, company, ...) carries the same
    # salary_* fields, so they aren't repeated here.
    sal["by"] = {
        dim: [
            {
                k: r[k]
                for k in (
                    "bucket",
                    "label",
                    "jobs",
                    "salary_n",
                    "salary_disclosure_pct",
                    "salary_avg_usd",
                    "salary_median_usd",
                    "salary_p10_usd",
                    "salary_p25_usd",
                    "salary_p75_usd",
                    "salary_p90_usd",
                    "salary_thin",
                    "d_salary_median_usd",
                    "new_listing_salary_median_usd",
                )
                if k in r
            }
            for r in rows[dim]
            if r.get("salary_n")
        ]
        for dim in (
            "job_family",
            "profile",
            "seniority",
            "yoe_band",
            "yoe_years",
            "country",
            "region",
            "work_mode",
            "employment_type",
            "industry",
            "company_size",
            "company_age",
            "education",
            "visa",
            "equity",
            "language",
        )
    }
    dims = roll["dims"]
    sal["histograms"] = histograms = {"overall": histogram(all_s, days)}
    for dim, top in (
        ("job_family", None),
        ("seniority", None),
        ("yoe_band", None),
        ("region", None),
        ("country", 15),
        ("profile", 25),
        ("work_mode", None),
    ):
        ranked = sorted(dims.get(dim, {}).items(), key=lambda kv: -kv[1].get("sn", 0))
        if top:
            ranked = ranked[:top]
        histograms[dim] = {b: histogram(s, days) for b, s in ranked if s.get("sn")}  # type: ignore[assignment]
    sal["premiums"] = {
        dim: sorted(
            [
                {
                    "bucket": r["bucket"],
                    "label": r["label"],
                    "premium_index": r["premium_index"],
                    "salary_n": r["salary_n"],
                }
                for r in rows[dim]
                if r.get("premium_index") and r.get("salary_n", 0) >= 10
            ],
            key=lambda r: -r["premium_index"],
        )
        for dim in (
            "country",
            "city",
            "technology",
            "industry",
            "company_size",
            "education",
            "work_mode",
            "seniority",
            "company",
        )
        if dim in rows
    }
    remote_vs_onsite = []
    wm = {}
    for r in cross_rows(roll, "profile|work_mode"):
        wm.setdefault(r["a"], {})[r["b"]] = r
    for prof, modes in wm.items():
        rm_, on = modes.get("remote"), modes.get("onsite")
        if (
            rm_
            and on
            and rm_.get("salary_median_usd")
            and on.get("salary_median_usd")
            and rm_["salary_n"] >= 10
            and on["salary_n"] >= 10
        ):
            remote_vs_onsite.append(
                {
                    "profile": prof,
                    "label": classify.profile_label(prof),
                    "remote_median_usd": rm_["salary_median_usd"],
                    "onsite_median_usd": on["salary_median_usd"],
                    "remote_premium_pct": pct(
                        rm_["salary_median_usd"] - on["salary_median_usd"], on["salary_median_usd"]
                    ),
                }
            )
    sal["premiums"]["remote_vs_onsite_by_profile"] = sorted(
        remote_vs_onsite, key=lambda r: -(r["remote_premium_pct"] or 0)
    )
    local = roll.get("local", {})
    sal["local_currency"] = {
        "by_country": _local_rows(local.get("country_currency", {}), days, ("country", "currency")),
        "by_country_family": _local_rows(
            local.get("country_currency_family", {}), days, ("country", "currency", "job_family")
        ),
    }
    sal["disclosure"] = {
        dim: [
            {
                "bucket": r["bucket"],
                "label": r["label"],
                "jobs": r["jobs"],
                "salary_n": r["salary_n"],
                "salary_disclosure_pct": r.get("salary_disclosure_pct"),
            }
            for r in rows[dim][: (50 if dim == "company" else None)]
        ]
        for dim in ("country", "source", "company", "job_family", "region")
    }

    # --- experience / skills / geography / companies extras -------------
    ent = sum(r["jobs"] for r in rows["seniority"] if r["bucket"] in ("intern", "entry"))
    sections["experience"]["entry_level_share_pct"] = pct(ent, overall["jobs"])
    sections["experience"]["salary_curve_by_years"] = [
        {
            "years": r["bucket"],
            "jobs": r["jobs"],
            "salary_n": r["salary_n"],
            "salary_median_usd": r.get("salary_median_usd"),
            "salary_p25_usd": r.get("salary_p25_usd"),
            "salary_p75_usd": r.get("salary_p75_usd"),
        }
        for r in rows["yoe_years"]
    ]
    pairs = sorted(roll.get("tech_pairs", {}).items(), key=lambda kv: -kv[1])[:50]
    sections["skills"]["pairs"] = [
        {
            "a": k.split("+")[0],
            "b": k.split("+", 1)[1],
            "jobs": round(v / days, 1) if days > 1 else v,
        }
        for k, v in pairs
    ]
    rd = roll.get("removed_days", {})
    sections["attributes"]["removed_days_open"] = _days_open_bands(rd)

    # --- kpi --------------------------------------------------------------
    kx = roll.get("kpi", {})
    removed = kx.get("removed")
    kpi = {
        "days": days,
        "total_jobs": overall["jobs"],
        "unique_roles": round(kx.get("roles", 0) / days, 1) if days > 1 else kx.get("roles", 0),
        "companies_hiring": round(kx.get("companies", 0) / days, 1)
        if days > 1
        else kx.get("companies", 0),
        "countries": round(kx.get("countries", 0) / days, 1)
        if days > 1
        else kx.get("countries", 0),
        # The first ever run has no yesterday to compare with: new/removed unknown.
        "new_jobs": None if kx.get("baseline_days", 0) >= days else all_s.get("n", 0),
        "removed_jobs": None if kx.get("baseline_days", 0) >= days else (removed or 0),
        "removed_median_days_open": _median_days(rd),
        "remote_pct": overall["remote_pct"],
        "hybrid_pct": overall["hybrid_pct"],
        "onsite_pct": overall["onsite_pct"],
        **{k: overall[k] for k in overall if k.startswith("salary_")},
        "new_listing_salary_median_usd": overall["new_listing_salary_median_usd"],
        "yoe_median": overall["yoe_median"],
        "yoe_stated_pct": overall["yoe_stated_pct"],
        "visa_offered_pct": overall["visa_offered_pct"],
        "equity_pct": overall["equity_pct"],
        "entry_level_share_pct": sections["experience"]["entry_level_share_pct"],
    }
    baseline = {"kpi": kpi, "dims": compact({d: rows[d] for d in rows})}
    return kpi, sections, baseline


_DAY_BANDS = (
    (0, 7, "0-7"),
    (8, 14, "8-14"),
    (15, 30, "15-30"),
    (31, 60, "31-60"),
    (61, 90, "61-90"),
    (91, 10_000, "90+"),
)


def _days_open_bands(rd: dict) -> list[dict]:
    counts = {label: 0 for *_, label in _DAY_BANDS}
    for d, c in rd.items():
        d = int(d)
        for lo, hi, label in _DAY_BANDS:
            if lo <= d <= hi:
                counts[label] += c
                break
    return [{"bucket": k, "removed": v} for k, v in counts.items()]


def _median_days(rd: dict) -> float | None:
    if not rd:
        return None
    items = sorted((int(d), c) for d, c in rd.items())
    total = sum(c for _, c in items)
    run = 0
    for d, c in items:
        run += c
        if run >= total / 2:
            return float(d)
    return None


HEADLINE_KEYS = (
    "total_jobs",
    "unique_roles",
    "companies_hiring",
    "countries",
    "new_jobs",
    "removed_jobs",
    "remote_pct",
    "hybrid_pct",
    "onsite_pct",
    "salary_disclosure_pct",
    "salary_avg_usd",
    "salary_median_usd",
    "salary_p10_usd",
    "salary_p25_usd",
    "salary_p75_usd",
    "salary_p90_usd",
    "new_listing_salary_median_usd",
    "yoe_median",
    "visa_offered_pct",
    "equity_pct",
    "entry_level_share_pct",
    "removed_median_days_open",
)


def kpi_delta(cur: dict, prev: dict | None) -> dict | None:
    if not prev:
        return None
    out = {}
    for k in HEADLINE_KEYS:
        c, p = cur.get(k), prev.get(k)
        if c is None or p is None:
            out[k] = {"prev": p, "delta": None, "pct": None}
        else:
            out[k] = {"prev": p, "delta": round(c - p, 2), "pct": pct(c - p, p)}
    return out
