"""Per-job classifiers for the market report: profile (occupation), job family,
seniority, education, visa / equity / clearance signals, cleaned title.

Profiles come from upstream's occupation taxonomy (data/occupations.csv via
src.core.occupation_resolve) first. That taxonomy is tech- and corporate-heavy,
so a supplemental keyword list below covers the rest of the labour market
(healthcare, education, retail, hospitality, trades, logistics, ...). Every
rule is a whole-word title match — unmatched titles land in "other" rather
than a guess.
"""

# ruff: noqa: E501  (regex tables read better unwrapped)
from __future__ import annotations

import csv
import functools
import re

from src.core.occupation_resolve import match_occupation
from src.core.seniority_resolve import match_seniority
from src.pgpipe.config import DATA_DIR
from src.pgpipe.seniority import seniority_of

# --- job families ---------------------------------------------------------
# Upstream occupation domains + families for roles upstream doesn't cover.
FAMILY_LABELS = {
    "software-engineering": "Software Engineering",
    "data-ai": "Data & AI",
    "infrastructure-security": "Infrastructure & Security",
    "product-design": "Product & Design",
    "management-consulting": "Management & Consulting",
    "sales-customer": "Sales & Customer",
    "corporate-functions": "Corporate Functions",
    "operations-facilities": "Operations & Facilities",
    "healthcare": "Healthcare",
    "education": "Education",
    "science-research": "Science & Research",
    "hospitality-retail": "Hospitality, Food & Retail",
    "trades-manufacturing": "Skilled Trades & Manufacturing",
    "logistics-transport": "Logistics & Transport",
    "creative-media": "Creative & Media",
    "legal": "Legal",
    "engineering": "Engineering (Non-Software)",
    "other": "Other",
}

# (profile slug, family, title regex). First match wins, so specific before generic.
_SUPPLEMENTAL: list[tuple[str, str, str]] = [
    # healthcare
    (
        "registered-nurse",
        "healthcare",
        r"\b(registered nurse|rn\b|nurse practitioner|lpn|lvn|nurse)\b|\bnursing\b",
    ),
    (
        "physician",
        "healthcare",
        r"\b(physician|doctor|surgeon|psychiatrist|hospitalist|anesthesiologist|radiologist|md\b)",
    ),
    (
        "therapist",
        "healthcare",
        r"\b(physical therapist|occupational therapist|speech (language )?(pathologist|therapist)|therapist|counselor|psychologist)\b",
    ),
    (
        "medical-technician",
        "healthcare",
        r"\b(medical assistant|phlebotomist|radiologic technologist|lab technician|laboratory technician|sonographer|pharmacy technician|surgical technologist|emt\b|paramedic)",
    ),
    (
        "caregiver",
        "healthcare",
        r"\b(caregiver|care assistant|home health aide|personal care|support worker|nursing assistant|cna\b)",
    ),
    ("dentist", "healthcare", r"\b(dentist|dental hygienist|dental assistant)\b"),
    ("veterinarian", "healthcare", r"\b(veterinarian|vet tech|veterinary)\b"),
    (
        "clinical-research",
        "science-research",
        r"\b(clinical research|clinical trial|cra\b|clinical operations)",
    ),
    ("social-worker", "healthcare", r"\b(social worker|case manager)\b"),
    # science
    (
        "lab-scientist",
        "science-research",
        r"\b(scientist|chemist|biologist|microbiologist|research associate|postdoc|post-doctoral|lab(oratory)? (manager|associate))\b",
    ),
    ("regulatory-affairs", "science-research", r"\bregulatory affairs\b"),
    # education
    (
        "teacher",
        "education",
        r"\b(teacher|tutor|instructor|lecturer|professor|educator|teaching assistant|faculty)\b",
    ),
    # legal
    (
        "lawyer",
        "legal",
        r"\b(attorney|lawyer|solicitor|paralegal|legal assistant|associate general counsel|general counsel)\b",
    ),
    # hospitality / food / retail
    ("chef-cook", "hospitality-retail", r"\b(chef|cook|line cook|sous chef|kitchen)\b"),
    (
        "food-service",
        "hospitality-retail",
        r"\b(barista|server(?!\s*(admin|engineer|developer|architect|side))|bartender|waiter|waitress|host(ess)?|crew member|dishwasher|food service)\b",
    ),
    (
        "hotel-staff",
        "hospitality-retail",
        r"\b(front desk|housekeep|concierge|guest services|room attendant)\b",
    ),
    (
        "store-manager",
        "hospitality-retail",
        r"\b(store manager|shift manager|restaurant manager|assistant manager|general manager)\b",
    ),
    (
        "retail-associate",
        "hospitality-retail",
        r"\b(cashier|retail|sales associate|store associate|merchandiser|stocker|shop assistant)\b",
    ),
    # trades / manufacturing
    ("electrician", "trades-manufacturing", r"\belectrician\b"),
    ("plumber-hvac", "trades-manufacturing", r"\b(plumber|hvac|pipefitter|refrigeration)\b"),
    (
        "mechanic",
        "trades-manufacturing",
        r"\b(mechanic|automotive technician|diesel technician|aircraft maintenance|a&p)\b",
    ),
    ("welder-fabricator", "trades-manufacturing", r"\b(welder|fabricator|machinist|cnc)\b"),
    (
        "production-operator",
        "trades-manufacturing",
        r"\b(machine operator|production (operator|worker|associate|technician)|assembler|assembly|manufacturing (associate|technician|operator))\b",
    ),
    (
        "field-technician",
        "trades-manufacturing",
        r"\b(field (service )?technician|service technician|installer|maintenance)\b",
    ),
    (
        "carpenter-construction",
        "trades-manufacturing",
        r"\b(carpenter|construction worker|laborer|labourer|painter|roofer|foreman|superintendent)\b",
    ),
    # logistics / transport
    ("driver", "logistics-transport", r"\b(driver|cdl|chauffeur|courier)\b"),
    (
        "pilot-aviation",
        "logistics-transport",
        r"\b(pilot|flight attendant|cabin crew|dispatcher)\b",
    ),
    (
        "warehouse",
        "logistics-transport",
        r"\b(warehouse|forklift|picker|packer|material handler|fulfillment associate|loader)\b",
    ),
    (
        "logistics-coordinator",
        "logistics-transport",
        r"\b(logistics|shipping|freight|fleet|transportation)\b",
    ),
    ("procurement-buyer", "operations-facilities", r"\b(buyer|procurement|purchasing|sourcing)\b"),
    # security / facilities
    ("security-officer", "operations-facilities", r"\b(security (officer|guard)|guard)\b"),
    ("cleaner", "operations-facilities", r"\b(cleaner|janitor|custodian|housekeeper)\b"),
    # creative / media / marketing
    (
        "graphic-designer",
        "creative-media",
        r"\b(graphic designer|visual designer|illustrator|motion designer|animator|art director|creative director)\b",
    ),
    (
        "content-writer",
        "creative-media",
        r"\b(copywriter|content writer|writer|editor|journalist|reporter)\b",
    ),
    (
        "video-producer",
        "creative-media",
        r"\b(video|producer|photographer|videographer|sound|audio)\b",
    ),
    (
        "communications-pr",
        "corporate-functions",
        r"\b(communications|public relations|\bpr\b|social media|community manager)\b",
    ),
    (
        "growth-marketing",
        "corporate-functions",
        r"\b(growth|seo\b|sem\b|performance marketing|demand generation|lifecycle|crm marketing|product marketing|brand)",
    ),
    # sales / customer / admin
    (
        "sales-development-rep",
        "sales-customer",
        r"\b(sdr\b|bdr\b|sales development|business development representative)",
    ),
    (
        "business-development",
        "sales-customer",
        r"\b(business development|partnerships?|alliances|channel)\b",
    ),
    (
        "customer-service",
        "sales-customer",
        r"\b(customer service|customer support|call center|contact center|customer care|client service)",
    ),
    ("account-manager", "sales-customer", r"\b(account manager|account management|key account)\b"),
    (
        "insurance-agent",
        "sales-customer",
        r"\b(insurance agent|claims|underwriter|loan officer|mortgage|teller|banker)\b",
    ),
    ("real-estate", "sales-customer", r"\b(real estate|leasing|property manager)\b"),
    (
        "admin-assistant",
        "corporate-functions",
        r"\b(administrative|admin assistant|office manager|receptionist|office coordinator|secretary|clerk|data entry)\b",
    ),
    (
        "financial-analyst",
        "corporate-functions",
        r"\b(financial analyst|fp&a|investment|portfolio|trader|trading|analyst, finance|actuar|treasury|controller|bookkeeper|payroll|billing)\b",
    ),
    (
        "people-ops",
        "corporate-functions",
        r"\b(people (operations|partner)|hr\b|human resources|talent|benefits|compensation)\b",
    ),
    (
        "program-coordinator",
        "management-consulting",
        r"\b(coordinator|program manager|programme manager|project coordinator)\b",
    ),
    (
        "strategy-ops",
        "management-consulting",
        r"\b(strategy|chief of staff|business operations|bizops|operations)\b",
    ),
    (
        "it-support",
        "infrastructure-security",
        r"\b((server|system|it) (administrator|admin)|it support|help ?desk|desktop support|it technician|it specialist)\b",
    ),
    ("product-manager", "product-design", r"\bproduct (manager|owner|lead)\b"),
    (
        "finance-manager",
        "corporate-functions",
        r"\b(finance|financial|accounting|accountant|tax)\b",
    ),
    ("marketing-manager", "corporate-functions", r"\bmarketing\b"),
    ("hr-manager", "corporate-functions", r"\b(people|recruiting|recruitment)\b"),
    ("sales-other", "sales-customer", r"\b(sales|seller|selling)\b"),
    ("designer", "product-design", r"\bdesign(er)?\b"),
    (
        "executive-leadership",
        "management-consulting",
        r"\b(chief|vp|vice president|president|head of|director|general manager|managing director)\b",
    ),
    (
        "engineer-other",
        "engineering",
        r"\b(civil engineer|structural engineer|process engineer|manufacturing engineer|industrial engineer|chemical engineer|quality engineer|engineer)\b",
    ),
]
_SUPPLEMENTAL_RE = [(slug, fam, re.compile(rx, re.I)) for slug, fam, rx in _SUPPLEMENTAL]


@functools.cache
def _occupation_meta() -> dict[str, tuple[str, str]]:
    """slug -> (english label, domain) from data/occupations.csv."""
    out: dict[str, tuple[str, str]] = {}
    with open(DATA_DIR / "occupations.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out[row["slug"]] = (row["en"], row["domain"] or "other")
    return out


def classify_profile(title: str | None) -> tuple[str, str]:
    """(profile slug, job family slug). ('other', 'other') when unknown."""
    if not title:
        return "other", "other"
    occ = match_occupation(title)
    if occ:
        meta = _occupation_meta().get(occ)
        return occ, (meta[1] if meta else "other")
    for slug, fam, rx in _SUPPLEMENTAL_RE:
        if rx.search(title):
            return slug, fam
    return "other", "other"


def profile_label(slug: str) -> str:
    meta = _occupation_meta().get(slug)
    if meta:
        return meta[0]
    return slug.replace("-", " ").title()


# --- seniority ------------------------------------------------------------
SENIORITY_ORDER = (
    "intern",
    "entry",
    "mid",
    "senior",
    "lead",
    "staff",
    "principal",
    "director",
    "executive",
    "unspecified",
)
_PGPIPE_MAP = {
    "junior": "entry",
    "intern": "intern",
    "mid": "mid",
    "senior": "senior",
    "staff": "staff",
    "principal": "principal",
}


def classify_seniority(title: str | None) -> str:
    """Upstream's zero-false-positive resolver first, then the jobs-board
    heuristic (which defaults unqualified IC engineering titles to mid)."""
    if not title:
        return "unspecified"
    s = match_seniority(title)
    if s:
        return s
    return _PGPIPE_MAP.get(seniority_of(title), "unspecified")


# --- years-of-experience bands ---------------------------------------------
YOE_BANDS = ("0-1", "1-3", "3-5", "5-8", "8-12", "12+", "unspecified")


def yoe_band(years: float | None) -> str:
    if years is None:
        return "unspecified"
    if years < 1:
        return "0-1"
    if years < 3:
        return "1-3"
    if years < 5:
        return "3-5"
    if years < 8:
        return "5-8"
    if years < 12:
        return "8-12"
    return "12+"


# --- description signals ----------------------------------------------------
_EDU = [
    ("phd", re.compile(r"\b(ph\.?\s?d\.?|doctorate|doctoral degree)", re.I)),
    ("master", re.compile(r"\b(master'?s|m\.sc\.?|msc|mba|m\.eng|master of)\b", re.I)),
    (
        "bachelor",
        re.compile(
            r"\b(bachelor'?s?|b\.sc\.?|bsc|b\.tech|btech|b\.e\.|undergraduate degree|"
            r"ba/bs|bs/ba|b\.s\. degree|four[- ]year degree|4[- ]year degree|college degree)",
            re.I,
        ),
    ),
    ("high_school", re.compile(r"\b(high school( diploma)?|ged\b|secondary school)", re.I)),
]
EDUCATION_ORDER = ("high_school", "bachelor", "master", "phd", "none_stated")


def classify_education(text: str) -> str:
    """Lowest degree the posting mentions ("Bachelor's or Master's" -> bachelor)."""
    found = [lvl for lvl, rx in _EDU if rx.search(text)]
    if not found:
        return "none_stated"
    return min(found, key=EDUCATION_ORDER.index)


_VISA_NO = re.compile(
    r"(not|unable to|cannot|can't|won't|will not|do not|does not|are not able to)\s+"
    r"(be able to\s+)?(provide\s+|offer\s+|support\s+)?(visa\s+|immigration\s+|employment\s+)?"
    r"(sponsor|sponsorship)|sponsorship (is )?not (available|provided|offered|possible)|"
    r"without (the need for )?(current or future )?(visa |employer )?sponsorship|"
    r"no (visa )?sponsorship",
    re.I,
)
_VISA_YES = re.compile(
    r"(visa|immigration|work permit) (sponsorship|support) (is )?(available|provided|offered)|"
    r"(will|can|we) sponsor|sponsorship (is )?available|offer(s|ing)? visa sponsorship|"
    r"provide(s)? visa sponsorship|relocation and visa|visa support",
    re.I,
)


def classify_visa(text: str) -> str:
    if _VISA_NO.search(text):
        return "not_offered"
    if _VISA_YES.search(text):
        return "offered"
    return "not_mentioned"


_EQUITY = re.compile(
    r"\b(stock options?|rsus?\b|restricted stock|employee stock|esop\b|"
    r"equity (grant|package|compensation|award|stake|component)|"
    r"(competitive|meaningful|generous|significant|startup) equity|equity in the company)",
    re.I,
)
_CLEARANCE = re.compile(
    r"\b(security clearance|ts/sci|top secret|secret clearance|active clearance|"
    r"clearance (is )?required|sc clearance|dv clearance|nv1|nv2|baseline clearance)",
    re.I,
)


def has_equity(text: str) -> bool:
    return bool(_EQUITY.search(text))


def needs_clearance(text: str) -> bool:
    return bool(_CLEARANCE.search(text))


# --- cleaned title ----------------------------------------------------------
_TITLE_DROP = re.compile(
    r"\((.*?)\)|\[(.*?)\]|\s[-–|@]\s.*$|,.*$|\b(senior|sr\.?|junior|jr\.?|staff|principal|"
    r"lead|intern(ship)?|entry[- ]level|mid[- ]level|associate|graduate|new grad|"
    r"i{1,3}|iv|v|l\d|level \d|[1-5])\b|\b(f/m/d|m/f/d|m/w/d|w/m/d|h/f|f/h)\b",
    re.I,
)


def clean_title(title: str | None) -> str | None:
    """Group variants: 'Senior Software Engineer II, Payments' -> 'Software Engineer'."""
    if not title:
        return None
    t = _TITLE_DROP.sub(" ", title)
    t = re.sub(r"[^\w&+#/ ]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) < 2:
        return None
    return t[:60].title()
