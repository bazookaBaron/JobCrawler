"""Regenerate the data files used by the market-report job (src/report/).

Unlike tools/trim_csvs.py (which keeps ~500 companies for the jobs board),
the report crawls EVERY upstream board this fork can read over plain HTTP
with a rich monitor — ~5k boards / ~4.5k companies, all industries, all roles.

Outputs
  crawler/data/report/companies.csv   every company with >=1 usable board
  crawler/data/report/boards.csv      every usable board (rich, non-browser,
                                      monitor present in src/core/monitors)
  crawler/data/{occupations,occupation_domains,seniority,technologies,
                industries}.csv       upstream taxonomies, read by the
                                      src/core/*_resolve.py resolvers via
                                      get_data_dir()
  crawler/data/report/geo_countries.csv  ISO-2, ISO-3, name, region, aliases
  crawler/data/report/geo_cities.csv     cities >= 15k population (GeoNames,
                                         CC-BY 4.0, via the geonamescache wheel)

Run (from the repo root, with a fresh upstream clone at $JOBSEEK):
    uv run --project crawler --no-sync --with geonamescache \
        python tools/build_report_data.py "$JOBSEEK"
"""

from __future__ import annotations

import csv
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from trim_csvs import LOCAL_RICH, monitor_needs_browser  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "crawler" / "data"
OUT = DATA / "report"
MONITORS = REPO / "crawler" / "src" / "core" / "monitors"

TAXONOMIES = (
    "occupations.csv",
    "occupation_domains.csv",
    "seniority.csv",
    "technologies.csv",
    "industries.csv",
)

# Business regions used by the report (GeoNames continent codes are too coarse:
# they put Mexico with the US and Israel with Japan).
_MIDDLE_EAST = {
    "AE",
    "SA",
    "QA",
    "KW",
    "BH",
    "OM",
    "IL",
    "JO",
    "LB",
    "IQ",
    "IR",
    "SY",
    "YE",
    "PS",
    "TR",
    "CY",
}
_NORTH_AMERICA = {"US", "CA", "PM", "BM", "GL"}

# Common ways job boards spell countries that GeoNames' name doesn't cover.
_COUNTRY_ALIASES = {
    "US": ["usa", "u.s.", "u.s.a.", "united states of america", "america", "us"],
    "GB": [
        "uk",
        "u.k.",
        "great britain",
        "britain",
        "england",
        "scotland",
        "wales",
        "northern ireland",
        "gb",
    ],
    "AE": ["uae", "u.a.e."],
    "KR": ["south korea", "korea", "republic of korea"],
    "KP": ["north korea"],
    "CZ": ["czechia", "czech republic"],
    "NL": ["holland", "the netherlands"],
    "RU": ["russian federation"],
    "VN": ["viet nam"],
    "TW": ["taiwan, province of china"],
    "HK": ["hong kong sar", "hong kong s.a.r."],
    "CN": ["prc", "mainland china", "people's republic of china"],
    "IR": ["iran, islamic republic of"],
    "TR": ["turkiye", "türkiye"],
    "CI": ["ivory coast", "cote d'ivoire", "côte d'ivoire"],
    "CD": ["drc", "dr congo", "democratic republic of the congo"],
    "MK": ["north macedonia", "macedonia"],
    "MM": ["burma"],
    "SZ": ["eswatini", "swaziland"],
    "PS": ["palestine"],
    "BO": ["bolivia"],
    "VE": ["venezuela"],
    "TZ": ["tanzania"],
    "LA": ["laos"],
    "MD": ["moldova"],
    "SY": ["syria"],
}


def _region(iso: str, continent: str) -> str:
    if iso in _NORTH_AMERICA:
        return "North America"
    if iso in _MIDDLE_EAST:
        return "Middle East"
    return {
        "NA": "Latin America",
        "SA": "Latin America",
        "EU": "Europe",
        "AF": "Africa",
        "AS": "Asia",
        "OC": "Oceania",
        "AN": "Antarctica",
    }.get(continent, "Other")


def _fold(text: str) -> str:
    import unicodedata

    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
    )


# (country, GeoNames name) -> extra spellings seen on job boards.
_CITY_EXONYMS: dict[tuple[str, str], tuple[str, ...]] = {
    ("IN", "Bengaluru"): ("Bangalore",),
    ("IN", "Mumbai"): ("Bombay",),
    ("IN", "Gurugram"): ("Gurgaon",),
    ("IN", "Kolkata"): ("Calcutta",),
    ("IN", "Chennai"): ("Madras",),
    ("DE", "Köln"): ("Cologne",),
    ("DE", "Nürnberg"): ("Nuremberg",),
    ("DE", "Munich"): ("München", "Muenchen"),
    ("DE", "Düsseldorf"): ("Duesseldorf",),
    ("AT", "Vienna"): ("Wien",),
    ("CH", "Zürich"): ("Zuerich",),
    ("CH", "Geneva"): ("Genève", "Geneve", "Genf"),
    ("IT", "Milan"): ("Milano",),
    ("IT", "Rome"): ("Roma",),
    ("PT", "Lisbon"): ("Lisboa",),
    ("CZ", "Prague"): ("Praha",),
    ("PL", "Warsaw"): ("Warszawa",),
    ("BE", "Brussels"): ("Bruxelles", "Brussel"),
    ("DK", "Copenhagen"): ("København", "Kobenhavn"),
    ("UA", "Kyiv"): ("Kiev",),
    ("VN", "Ho Chi Minh City"): ("Saigon", "HCMC"),
    ("CN", "Beijing"): ("Peking",),
    ("CN", "Guangzhou"): ("Canton",),
    ("US", "New York City"): ("NYC", "New York"),
    ("US", "Washington"): ("Washington DC", "Washington D.C."),
}


def _cfg(raw: str | None) -> dict:
    try:
        return json.loads((raw or "").strip() or "{}")
    except ValueError:
        return {}


def build_boards(upstream: Path) -> None:
    src = upstream / "apps" / "crawler" / "data"
    local_monitors = {p.stem for p in MONITORS.glob("*.py")}
    with open(src / "companies.csv", encoding="utf-8") as f:
        companies = list(csv.DictReader(f))
    with open(src / "boards.csv", encoding="utf-8") as f:
        boards = list(csv.DictReader(f))
    comp_by_slug = {c["slug"]: c for c in companies}

    kept_boards = [
        b
        for b in boards
        if b["company_slug"] in comp_by_slug
        and b["monitor_type"] in LOCAL_RICH
        and (b["monitor_type"] in local_monitors or b["monitor_type"] == "workday")
        and not monitor_needs_browser(b["monitor_type"], _cfg(b.get("monitor_config")))
    ]
    kept_slugs = {b["company_slug"] for b in kept_boards}
    kept_companies = [c for c in companies if c["slug"] in kept_slugs]

    OUT.mkdir(parents=True, exist_ok=True)
    comp_fields = ["slug", "name", "industry", "employee_count_range", "founded_year"]
    with open(OUT / "companies.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=comp_fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(kept_companies, key=lambda r: r["slug"]))
    board_fields = ["company_slug", "board_slug", "board_url", "monitor_type", "monitor_config"]
    with open(OUT / "boards.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=board_fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(kept_boards, key=lambda r: (r["company_slug"], r["board_slug"])))

    mix: dict[str, int] = {}
    for b in kept_boards:
        mix[b["monitor_type"]] = mix.get(b["monitor_type"], 0) + 1
    print(f"report companies: {len(kept_companies)}  boards: {len(kept_boards)}")
    print(f"monitor mix: {dict(sorted(mix.items(), key=lambda x: -x[1]))}")

    for name in TAXONOMIES:
        shutil.copyfile(src / name, DATA / name)
    print(f"taxonomies copied: {', '.join(TAXONOMIES)}")


def build_geo() -> None:
    import geonamescache

    gc = geonamescache.GeonamesCache(min_city_population=15000)
    countries = gc.get_countries()

    with open(OUT / "geo_countries.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["iso2", "iso3", "name", "region", "currency", "aliases"])
        for iso, c in sorted(countries.items()):
            aliases = sorted(set(_COUNTRY_ALIASES.get(iso, [])))
            w.writerow(
                [
                    iso,
                    c["iso3"],
                    c["name"],
                    _region(iso, c["continentcode"]),
                    c.get("currencycode") or "",
                    "|".join(aliases),
                ]
            )

    # Aliases: the ASCII-folded name (Zürich -> Zurich), "X City" -> "X",
    # "Frankfurt am Main" -> "Frankfurt", plus a short hand list of exonyms
    # job boards use. GeoNames' raw alternate names are NOT used — they carry
    # IATA codes and transliterations that would false-match ("SHA", "BJS").
    rows = []
    for city in gc.get_cities().values():
        if city["countrycode"] not in countries:
            continue
        name = city["name"]
        alts = {_fold(name)}
        for suffix in (" City", " am Main"):
            if name.endswith(suffix):
                alts |= {name[: -len(suffix)], _fold(name[: -len(suffix)])}
        alts |= set(_CITY_EXONYMS.get((city["countrycode"], name), ()))
        alts.discard(name)
        rows.append(
            (
                name,
                city["countrycode"],
                city.get("admin1code") or "",
                int(city["population"]),
                "|".join(sorted(alts)),
            )
        )
    rows.sort(key=lambda r: -r[3])
    with open(OUT / "geo_cities.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "country", "admin1", "population", "aliases"])
        w.writerows(rows)
    print(f"geo: {len(countries)} countries, {len(rows)} cities")


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    build_boards(Path(sys.argv[1]))
    build_geo()
    return 0


if __name__ == "__main__":
    sys.exit(main())
