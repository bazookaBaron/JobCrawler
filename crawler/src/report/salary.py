"""Salary normalisation (-> annual, local currency) and FX (-> USD).

Sources, in order of trust:
  1. the monitor's structured `base_salary` ({currency, min, max, unit}) —
     Ashby, Lever, Recruitee, SmartRecruiters, Amazon, ...
  2. src.core.salary_extract over the description HTML (upstream's
     zero-false-positive parser; covers Greenhouse pay-transparency blocks)

FX: ECB daily reference rates (no key), gaps filled from open.er-api.com,
and if both are unreachable the caller falls back to yesterday's rates.
"""

from __future__ import annotations

import math
import xml.etree.ElementTree as ET

import httpx
import structlog

from src.core.salary_extract import parse_salary_text

log = structlog.get_logger()

_ANNUAL = {"year": 1, "month": 12, "week": 52, "day": 260, "hour": 2080}
# Plausible annual band in USD; outside it the figure is almost always a
# parse/unit error (e.g. an hourly rate tagged as yearly) and is excluded.
MIN_ANNUAL_USD = 5_000
MAX_ANNUAL_USD = 2_000_000
# A range wider than this (max/min) says nothing useful about pay.
MAX_RANGE_RATIO = 4.0


def _num(v: object) -> float | None:
    try:
        f = float(str(v).replace(",", "").strip()) if v is not None else None
    except ValueError:
        return None
    if f is None or math.isnan(f) or f <= 0:
        return None
    return f


def normalize(
    base_salary: dict | None, description: str | None
) -> tuple[float | None, float | None, str | None, str | None, str]:
    """-> (annual_min, annual_max, currency, unit, source) in LOCAL currency.
    source is 'structured' | 'description' | 'none'."""
    for source, sal in (("structured", base_salary), ("description", None)):
        if source == "description":
            if not description:
                break
            try:
                sal = parse_salary_text(description)
            except Exception:  # noqa: BLE001 - a parser bug must not drop the job
                sal = None
        if not isinstance(sal, dict):
            continue
        cur = (str(sal.get("currency") or "")).strip().upper()
        unit = (str(sal.get("unit") or "year")).strip().lower()
        lo, hi = _num(sal.get("min")), _num(sal.get("max"))
        if not cur or len(cur) != 3 or unit not in _ANNUAL or (lo is None and hi is None):
            continue
        lo = lo if lo is not None else hi
        hi = hi if hi is not None else lo
        assert lo is not None and hi is not None
        if lo > hi:
            lo, hi = hi, lo
        k = _ANNUAL[unit]
        return lo * k, hi * k, cur, unit, source
    return None, None, None, None, "none"


def usd_annual(
    lo: float | None, hi: float | None, currency: str | None, usd_per: dict[str, float]
) -> float | None:
    """Midpoint in USD/year, or None when unconvertible or implausible."""
    if lo is None or hi is None or not currency:
        return None
    rate = usd_per.get(currency)
    if not rate:
        return None
    if lo > 0 and hi / lo > MAX_RANGE_RATIO:
        return None
    mid = (lo + hi) / 2 * rate
    if mid < MIN_ANNUAL_USD or mid > MAX_ANNUAL_USD:
        return None
    return mid


_ECB = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
_ER_API = "https://open.er-api.com/v6/latest/USD"


async def fetch_usd_rates(client: httpx.AsyncClient) -> dict[str, float]:
    """{currency: USD per 1 unit}. Empty dict if every source fails."""
    usd_per: dict[str, float] = {}
    try:
        r = await client.get(_ECB, timeout=30)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        eur_per: dict[str, float] = {"EUR": 1.0}
        for node in root.iter():
            cur, rate = node.get("currency"), node.get("rate")
            if cur and rate:
                eur_per[cur] = float(rate)
        usd_in_eur = eur_per.get("USD")
        if usd_in_eur:
            usd_per = {cur: usd_in_eur / rate for cur, rate in eur_per.items()}
    except Exception as exc:  # noqa: BLE001
        log.warning("report.fx.ecb_failed", error=str(exc))
    try:
        r = await client.get(_ER_API, timeout=30)
        r.raise_for_status()
        rates = (r.json() or {}).get("rates") or {}
        for cur, per_usd in rates.items():
            if per_usd and cur not in usd_per:
                usd_per[cur] = 1.0 / float(per_usd)
    except Exception as exc:  # noqa: BLE001
        log.warning("report.fx.er_api_failed", error=str(exc))
    usd_per["USD"] = 1.0
    return usd_per if len(usd_per) > 1 else {}
