"""Offline location resolver: free-text job location -> (country, city, region).

No geocoding API. Data comes from crawler/data/report/geo_*.csv (GeoNames via
tools/build_report_data.py) plus the US / Canadian / Australian / Indian
first-level divisions below. Built for precision over recall: a string we
can't place confidently resolves to country=None rather than a guess.
"""

from __future__ import annotations

import csv
import functools
import re
from dataclasses import dataclass

from src.report.config import REPORT_DATA_DIR

US_STATES = {
    "AL": "alabama",
    "AK": "alaska",
    "AZ": "arizona",
    "AR": "arkansas",
    "CA": "california",
    "CO": "colorado",
    "CT": "connecticut",
    "DE": "delaware",
    "FL": "florida",
    "GA": "georgia",
    "HI": "hawaii",
    "ID": "idaho",
    "IL": "illinois",
    "IN": "indiana",
    "IA": "iowa",
    "KS": "kansas",
    "KY": "kentucky",
    "LA": "louisiana",
    "ME": "maine",
    "MD": "maryland",
    "MA": "massachusetts",
    "MI": "michigan",
    "MN": "minnesota",
    "MS": "mississippi",
    "MO": "missouri",
    "MT": "montana",
    "NE": "nebraska",
    "NV": "nevada",
    "NH": "new hampshire",
    "NJ": "new jersey",
    "NM": "new mexico",
    "NY": "new york",
    "NC": "north carolina",
    "ND": "north dakota",
    "OH": "ohio",
    "OK": "oklahoma",
    "OR": "oregon",
    "PA": "pennsylvania",
    "RI": "rhode island",
    "SC": "south carolina",
    "SD": "south dakota",
    "TN": "tennessee",
    "TX": "texas",
    "UT": "utah",
    "VT": "vermont",
    "VA": "virginia",
    "WA": "washington",
    "WV": "west virginia",
    "WI": "wisconsin",
    "WY": "wyoming",
    "DC": "district of columbia",
    "PR": "puerto rico",
}
_CA_PROVINCES = {
    "AB": "alberta",
    "BC": "british columbia",
    "MB": "manitoba",
    "NB": "new brunswick",
    "NL": "newfoundland and labrador",
    "NS": "nova scotia",
    "NT": "northwest territories",
    "NU": "nunavut",
    "ON": "ontario",
    "PE": "prince edward island",
    "QC": "quebec",
    "SK": "saskatchewan",
    "YT": "yukon",
}
_AU_STATES = {
    "NSW": "new south wales",
    "VIC": "victoria",
    "QLD": "queensland",
    "TAS": "tasmania",
    "ACT": "australian capital territory",
}
_IN_STATES = (
    "karnataka",
    "maharashtra",
    "telangana",
    "tamil nadu",
    "haryana",
    "uttar pradesh",
    "west bengal",
    "gujarat",
    "kerala",
    "rajasthan",
    "andhra pradesh",
    "delhi",
    "punjab",
    "madhya pradesh",
    "odisha",
)

# Region-only hints ("Remote - EMEA") -> report region, no country.
_REGION_HINTS = {
    "emea": "EMEA",
    "apac": "Asia",
    "asia pacific": "Asia",
    "asia": "Asia",
    "latam": "Latin America",
    "latin america": "Latin America",
    "south america": "Latin America",
    "europe": "Europe",
    "eu": "Europe",
    "north america": "North America",
    "americas": "Americas",
    "middle east": "Middle East",
    "mena": "Middle East",
    "africa": "Africa",
    "nordics": "Europe",
    "dach": "Europe",
    "benelux": "Europe",
    "oceania": "Oceania",
    "anz": "Oceania",
}
# Metro names that aren't GeoNames places.
_METROS = {
    "bay area": ("US", "San Francisco"),
    "sf bay area": ("US", "San Francisco"),
    "san francisco bay area": ("US", "San Francisco"),
    "silicon valley": ("US", "San Francisco"),
    "nyc": ("US", "New York City"),
    "greater london": ("GB", "London"),
    "delhi ncr": ("IN", "New Delhi"),
    "ncr": ("IN", "New Delhi"),
    "tri-state area": ("US", "New York City"),
}

_REMOTE_RE = re.compile(
    r"\b(remote|anywhere|work from home|wfh|home[- ]based|distributed|telecommute)\b", re.I
)
_HYBRID_RE = re.compile(r"\bhybrid\b", re.I)
_ONSITE_RE = re.compile(r"\b(on-?site|in[- ]office|office[- ]based)\b", re.I)
_WORLDWIDE_RE = re.compile(r"\b(worldwide|global(ly)?|anywhere in the world)\b", re.I)
_SPLIT_RE = re.compile(r"\s*[,;/|•·()\[\]]\s*|\s+[-–—]\s+|\s+(?:or|and|&)\s+")
_NOISE_RE = re.compile(
    r"\b(remote|hybrid|on-?site|in[- ]office|office|hq|headquarters|"
    r"multiple locations|various locations|flexible|anywhere|work from home|wfh|"
    r"greater|metropolitan|metro|area|region|based|first|only|full|time|"
    r"country|countries|location|locations|optional|preferred|role)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class Place:
    country: str | None  # ISO-2
    city: str | None  # GeoNames display name
    region: str | None  # report region
    remote: bool
    hybrid: bool
    onsite: bool
    worldwide: bool


class _Geo:
    def __init__(self) -> None:
        self.country_name: dict[str, str] = {}
        self.country_region: dict[str, str] = {}
        self.country_currency: dict[str, str] = {}
        self.country_index: dict[str, str] = {}
        self.iso2: set[str] = set()
        with open(REPORT_DATA_DIR / "geo_countries.csv", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                iso = row["iso2"]
                self.iso2.add(iso)
                self.country_name[iso] = row["name"]
                self.country_region[iso] = row["region"]
                self.country_currency[iso] = row["currency"]
                for key in [row["name"], row["iso3"], *filter(None, row["aliases"].split("|"))]:
                    self.country_index.setdefault(key.lower(), iso)
        # admin-1 names -> country
        self.admin_index: dict[str, tuple[str, str | None]] = {}
        for code, name in US_STATES.items():
            self.admin_index[name] = ("US", code)
        for name in _CA_PROVINCES.values():
            self.admin_index[name] = ("CA", None)
        for name in _AU_STATES.values():
            self.admin_index[name] = ("AU", None)
        for name in _IN_STATES:
            self.admin_index.setdefault(name, ("IN", None))
        # cities: lower name -> [(population, iso2, admin1, display)], most populous first
        self.cities: dict[str, list[tuple[int, str, str, str]]] = {}
        with open(REPORT_DATA_DIR / "geo_cities.csv", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                entry = (int(row["population"]), row["country"], row["admin1"], row["name"])
                for key in [row["name"], *filter(None, row["aliases"].split("|"))]:
                    self.cities.setdefault(key.lower(), []).append(entry)
        for entries in self.cities.values():
            entries.sort(key=lambda e: -e[0])


@functools.cache
def _geo() -> _Geo:
    return _Geo()


def country_name(iso: str | None) -> str | None:
    return _geo().country_name.get(iso or "")


def country_region(iso: str | None) -> str | None:
    return _geo().country_region.get(iso or "")


def _pick_city(
    entries: list[tuple[int, str, str, str]], country: str | None, us_state: str | None
) -> tuple[int, str, str, str] | None:
    if country:
        for e in entries:
            if e[1] == country and (us_state is None or country != "US" or e[2] == us_state):
                return e
        for e in entries:
            if e[1] == country:
                return e
        return None
    return entries[0]


@functools.lru_cache(maxsize=200_000)
def resolve(raw: str | None) -> Place:
    """Resolve one location string. Cached: boards repeat the same strings."""
    text = (raw or "").strip()
    remote = bool(_REMOTE_RE.search(text))
    hybrid = bool(_HYBRID_RE.search(text))
    onsite = bool(_ONSITE_RE.search(text))
    worldwide = bool(_WORLDWIDE_RE.search(text))
    if not text:
        return Place(None, None, None, False, False, False, False)
    g = _geo()

    parts = [p.strip(" .") for p in _SPLIT_RE.split(text)]
    cleaned: list[str] = []
    for p in parts:
        if not p:
            continue
        if p.lower() in _METROS:
            cleaned.append(p)
            continue
        stripped = _NOISE_RE.sub(" ", p).strip(" .-")
        stripped = re.sub(r"\s+", " ", stripped)
        stripped = re.sub(r"^(?:in|within|from|across)\s+", "", stripped, flags=re.I)
        if stripped:
            cleaned.append(stripped)

    country: str | None = None
    us_state: str | None = None
    region_hint: str | None = None
    state_country: str | None = None

    for p in reversed(cleaned):
        low = p.lower()
        if low in _METROS:
            iso, city = _METROS[low]
            return Place(iso, city, g.country_region.get(iso), remote, hybrid, onsite, worldwide)
        if low in g.country_index and not (
            len(p) == 2 and p.upper() in US_STATES and len(cleaned) > 1
        ):
            country = country or g.country_index[low]
            continue
        if len(p) in (2, 3) and p.isupper():
            if p in US_STATES and len(cleaned) > 1 and not country:
                state_country, us_state = "US", p
                continue
            if p in _CA_PROVINCES and len(cleaned) > 1 and not country:
                state_country = "CA"
                continue
            if p in _AU_STATES and not country:
                state_country = "AU"
                continue
            if len(p) == 2 and p in g.iso2:
                country = country or p
                continue
        if low in g.admin_index and not country:
            state_country, code = g.admin_index[low]
            us_state = us_state or code
            continue
        if low in _REGION_HINTS:
            region_hint = region_hint or _REGION_HINTS[low]

    hint_country = country or state_country
    city: str | None = None
    for p in cleaned:
        entries = g.cities.get(p.lower())
        if not entries:
            continue
        picked = _pick_city(entries, hint_country, us_state)
        if picked is None and not country and entries[0][0] >= 500_000:
            # "Perth, WA": the state hint is US, but Perth is a big AU city.
            picked = entries[0]
        if picked is None:
            continue
        # A bare, small, ambiguous name with no country context is a guess.
        if not hint_country and len(entries) > 1 and picked[0] < 100_000:
            continue
        city = picked[3]
        country = country or picked[1]
        break

    country = country or state_country
    region = g.country_region.get(country) if country else region_hint
    return Place(country, city, region, remote, hybrid, onsite, worldwide)
