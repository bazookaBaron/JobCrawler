"""The rollup: every figure the report shows, kept as ADDITIVE building blocks.

Counts, sums and sparse histograms only — so a week is just the sum of its
days (merge), and re-running a day is "subtract the old day, add the new one".
Percentiles come from log-scale histograms (1% wide bins for single
dimensions, 4% for two-way tables), so medians are accurate to about ±0.5%
(±2% in two-way tables) without storing a single job.

Per bucket keys (all additive):
  j   jobs                     n   new jobs           rm  removed (derived)
  c   distinct companies       r/h/o remote/hybrid/onsite jobs
  sn/ss/sh   salary count / sum (USD/yr) / histogram {log-bin: count}
  nn/ns/nh   the same for NEW listings only
  yn/yh      jobs stating years of experience / histogram {years: count}
  v   visa sponsorship offered  e   equity mentioned
  pn/ps      salary-premium sample / sum of (salary ÷ profile median)
"""

from __future__ import annotations

import itertools
from collections import Counter

import polars as pl

SAL_SCALE = 100  # 1% log bins
CROSS_SCALE = 25  # 4% log bins

# dimension name -> (column, explode list column?, keep top-K buckets by jobs)
DIMS: dict[str, tuple[str, bool, int | None]] = {
    "_all": ("_all", False, None),
    "job_family": ("job_family", False, None),
    "profile": ("profile", False, None),
    "title": ("title_clean", False, 500),
    "seniority": ("seniority", False, None),
    "yoe_band": ("yoe_band", False, None),
    "yoe_years": ("yoe_years", False, None),
    "country": ("country", False, None),
    "region": ("region", False, None),
    "city": ("city_key", False, 500),
    "work_mode": ("work_mode", False, None),
    "employment_type": ("employment_type", False, None),
    "industry": ("industry", False, None),
    "company_size": ("company_size", False, None),
    "company_age": ("company_age", False, None),
    "company": ("company_slug", False, 1000),
    "education": ("education", False, None),
    "visa": ("visa", False, None),
    "equity": ("equity_flag", False, None),
    "clearance": ("clearance_flag", False, None),
    "technology": ("technologies", True, None),
    "language": ("language", False, None),
    "source": ("source", False, None),
    "posting_age": ("posting_age", False, None),
    "salary_band": ("salary_band", False, None),
    "salary_currency": ("sal_currency", False, None),
    "salary_source": ("sal_source", False, None),
    "multi_location": ("multi_location", False, None),
}

# two-way tables: (a, b, top-K for b or None)
CROSS: list[tuple[str, str, int | None]] = [
    ("profile", "yoe_band", None),
    ("job_family", "yoe_band", None),
    ("seniority", "yoe_band", None),
    ("country", "yoe_band", 40),
    ("profile", "seniority", None),
    ("job_family", "seniority", None),
    ("profile", "work_mode", None),
    ("job_family", "work_mode", None),
    ("job_family", "industry", None),
    ("job_family", "company_size", None),
    ("job_family", "education", None),
    ("profile", "country", 40),
    ("job_family", "country", 40),
    ("region", "job_family", None),
    ("country", "seniority", 40),
    ("technology", "job_family", None),
    ("technology", "yoe_band", None),
    ("technology", "country", 25),
    ("industry", "seniority", None),
]


def _num(v):
    if v is None:
        return 0
    if isinstance(v, float):
        return round(v, 2)
    return int(v)


def _base_aggs() -> list[pl.Expr]:
    return [
        pl.len().alias("j"),
        pl.col("is_new").sum().alias("n"),
        pl.col("company_slug").n_unique().alias("c"),
        (pl.col("work_mode") == "remote").sum().alias("r"),
        (pl.col("work_mode") == "hybrid").sum().alias("h"),
        (pl.col("work_mode") == "onsite").sum().alias("o"),
        pl.col("sal_usd").is_not_null().sum().alias("sn"),
        pl.col("sal_usd").sum().alias("ss"),
        (pl.col("sal_usd").is_not_null() & pl.col("is_new")).sum().alias("nn"),
        pl.when(pl.col("is_new")).then(pl.col("sal_usd")).otherwise(None).sum().alias("ns"),
        pl.col("yoe_min").is_not_null().sum().alias("yn"),
        (pl.col("visa") == "offered").sum().alias("v"),
        pl.col("equity").sum().alias("e"),
        pl.col("rel").is_not_null().sum().alias("pn"),
        pl.col("rel").sum().alias("ps"),
    ]


def _cross_aggs() -> list[pl.Expr]:
    return [
        pl.len().alias("j"),
        pl.col("is_new").sum().alias("n"),
        (pl.col("work_mode") == "remote").sum().alias("r"),
        pl.col("sal_usd").is_not_null().sum().alias("sn"),
        pl.col("sal_usd").sum().alias("ss"),
        pl.col("yoe_min").is_not_null().sum().alias("yn"),
    ]


def _hist(d: pl.DataFrame, keys: list[str], bin_col: str, mask: pl.Expr) -> dict:
    out: dict = {}
    h = d.filter(mask).group_by([*keys, bin_col]).len()
    for row in h.iter_rows():
        *k, b, cnt = row
        out.setdefault(tuple(k), {})[str(b)] = int(cnt)
    return out


def _bucket_frame(df: pl.DataFrame, dim: str) -> pl.DataFrame:
    col, explode, _ = DIMS[dim]
    d = df.explode(col, empty_as_null=True) if explode else df
    return d.with_columns(pl.col(col).cast(pl.Utf8).fill_null("unknown").alias(f"_b_{dim}"))


def dim_stats(df: pl.DataFrame, dim: str) -> dict[str, dict]:
    _, _, top = DIMS[dim]
    key = f"_b_{dim}"
    d = _bucket_frame(df, dim)
    base = d.group_by(key).agg(_base_aggs()).sort("j", descending=True)
    if top:
        base = base.head(top)
        d = d.filter(pl.col(key).is_in(base[key].to_list()))
    sal = pl.col("sal_usd").is_not_null()
    sh = _hist(d, [key], "_sbin", sal)
    nh = _hist(d, [key], "_sbin", sal & pl.col("is_new"))
    yh = _hist(d, [key], "_ybin", pl.col("yoe_min").is_not_null())
    out: dict[str, dict] = {}
    for rec in base.iter_rows(named=True):
        b = rec.pop(key)
        s = {k: _num(v) for k, v in rec.items() if v}
        for name, hist in (("sh", sh), ("nh", nh), ("yh", yh)):
            if (b,) in hist:
                s[name] = hist[(b,)]
        out[b] = s
    return out


def cross_stats(df: pl.DataFrame, a: str, b: str, top_b: int | None) -> dict[str, dict]:
    ka, kb = f"_b_{a}", f"_b_{b}"
    d = _bucket_frame(_bucket_frame(df, a), b)
    if top_b:
        keep = d.group_by(kb).len().sort("len", descending=True).head(top_b)[kb].to_list()
        d = d.filter(pl.col(kb).is_in(keep))
    base = d.group_by([ka, kb]).agg(_cross_aggs())
    sh = _hist(d, [ka, kb], "_cbin", pl.col("sal_usd").is_not_null())
    out: dict[str, dict] = {}
    for rec in base.iter_rows(named=True):
        x, y = rec.pop(ka), rec.pop(kb)
        s = {k: _num(v) for k, v in rec.items() if v}
        if (x, y) in sh:
            s["sh"] = sh[(x, y)]
        out[f"{x}\u001f{y}"] = s
    return out


def local_stats(df: pl.DataFrame, keys: list[str]) -> dict[str, dict]:
    d = df.filter(pl.col("sal_local").is_not_null())
    base = d.group_by(keys).agg(pl.len().alias("sn"), pl.col("sal_local").sum().alias("ss"))
    sh = _hist(d, keys, "_lbin", pl.lit(True))
    out: dict[str, dict] = {}
    for rec in base.iter_rows(named=True):
        k = tuple(str(rec.pop(x)) for x in keys)
        s = {kk: _num(v) for kk, v in rec.items() if v}
        if k in sh:
            s["sh"] = sh[k]
        out["\u001f".join(k)] = s
    return out


def tech_pairs(df: pl.DataFrame, top: int = 300) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for techs in df.get_column("technologies").to_list():
        if techs and len(techs) > 1:
            for x, y in itertools.combinations(sorted(techs), 2):
                counts[f"{x}+{y}"] += 1
    return dict(counts.most_common(top))


def build(df: pl.DataFrame, extra_kpi: dict, removed_days: dict[str, int]) -> dict:
    """df: today's deduplicated, enriched job frame (see build.enrich)."""
    df = df.with_columns(
        (pl.col("sal_usd").log() * SAL_SCALE).round().cast(pl.Int32).alias("_sbin"),
        (pl.col("sal_usd").log() * CROSS_SCALE).round().cast(pl.Int32).alias("_cbin"),
        (pl.col("sal_local").log() * SAL_SCALE).round().cast(pl.Int32).alias("_lbin"),
        pl.col("yoe_min").floor().clip(0, 20).cast(pl.Int32).alias("_ybin"),
    )
    return {
        "v": 1,
        "days": 1,
        "kpi": {k: _num(v) for k, v in extra_kpi.items()},
        "removed_days": removed_days,
        "dims": {dim: dim_stats(df, dim) for dim in DIMS},
        "cross": {f"{a}|{b}": cross_stats(df, a, b, top) for a, b, top in CROSS},
        "local": {
            "country_currency": local_stats(df, ["country", "sal_currency"]),
            "country_currency_family": local_stats(df, ["country", "sal_currency", "job_family"]),
        },
        "tech_pairs": tech_pairs(df),
    }


# --- merging --------------------------------------------------------------
def merge(into: dict, other: dict, sign: int = 1) -> dict:
    """In-place recursive add (sign=+1) or subtract (sign=-1). Leaves that
    reach zero are dropped so subtracted days don't leave empty buckets."""
    for k, v in other.items():
        if isinstance(v, dict):
            child = into.get(k)
            if not isinstance(child, dict):
                child = into[k] = {}
            merge(child, v, sign)
            if not child:
                into.pop(k, None)
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            new = (into.get(k) or 0) + sign * v
            if isinstance(new, float):
                new = round(new, 2)
            if new:
                into[k] = new
            else:
                into.pop(k, None)
    return into
