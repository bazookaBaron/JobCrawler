"""Regenerate the trimmed crawler/data/{companies,boards}.csv from an upstream
jobseek checkout, keeping ~500 well-known companies whose every board uses a
RICH (full-data) HTTP monitor and needs no browser.

Why "rich only": the single-process Postgres pipeline in src/pgpipe/ calls each
monitor's discover() directly and stores what it returns. Rich monitors
(greenhouse, ashby, lever, recruitee, gem, oracle_hcm, amazon, ...) return full
DiscoveredJob rows (title/locations/employment_type/dept/url) with no scraper
and no description fetch. URL-only monitors (workday, smartrecruiters, personio,
sitemap, dom, ...) would need jobseek's scraper pipeline, which we dropped.

Run:
    # from a fresh upstream clone at $JOBSEEK:
    cp "$JOBSEEK/apps/crawler/data/companies.csv" crawler/data/companies.csv.upstream
    cp "$JOBSEEK/apps/crawler/data/boards.csv"    crawler/data/boards.csv.upstream
    cd crawler/data && python ../../tools/trim_csvs.py

It reads *.csv.upstream and overwrites the trimmed *.csv in the cwd.

RICH_MONITORS / the browser rules mirror upstream
src/core/monitors/__init__.py at the cloned SHA. Re-derive on upstream bump:
    uv run --no-sync python -c "import src.core.monitors as m; print(sorted(m.api_monitor_types()))"
"""
from __future__ import annotations

import csv
import json
import sys

TARGET = 500

# frozenset(m.name for m in _REGISTRY if m.rich) at SHA 73179987
RICH_MONITORS = {
    "accenture", "adp", "almacareer", "amazon", "ashby", "bamboohr", "beehire",
    "beisen", "brassring", "cnstaff", "comeet", "cornerstone", "curately",
    "cvwarehouse", "darwinbox", "dayforce", "deel", "dvinci", "earcu", "gem",
    "greenhouse", "headhunter", "hibob", "hirehive", "hireology", "infor",
    "inline", "inploi", "jarvi", "jobstreet", "jobylon", "keka", "kipt",
    "lever", "linkedin", "manatal", "mokahr", "oracle_hcm", "pageup", "paycom",
    "paylocity", "pinpoint", "prospective", "recruitee", "recruiter_co_kr",
    "rss", "seamlesshiring", "traffit", "turbohire", "typify", "ukg", "unifr",
    "unisante", "welcometothejungle",
}
# monitor_needs_browser: always-browser families
BROWSER_MONITORS = {"accenture", "brassring", "candidatus", "darwinbox", "dayforce", "njoyn"}

# +10 score: brand-name companies we specifically want if eligible.
CURATED = {
    "anthropic", "openai", "cohere", "mistral-ai", "perplexity-ai", "x-ai",
    "scale-ai", "contextual-ai", "together-ai", "huggingface", "runway",
    "luma-ai", "cerebras-systems", "groq", "sambanova", "modal", "baseten",
    "anyscale", "fal", "cursor", "sierra", "harvey", "glean", "pinecone",
    "weaviate", "qdrant", "elevenlabs", "hume-ai", "writer", "stability-ai",
    "assemblyai", "deepgram", "lambda",
    "databricks", "snowflake", "confluent", "cockroach-labs", "clickhouse",
    "starburst", "dbt-labs", "fivetran", "materialize", "redpanda-data",
    "singlestore", "temporal", "temporal-technologies", "planetscale", "neon",
    "supabase", "vercel", "netlify", "render", "railway", "hashicorp",
    "datadog", "cloudflare", "fastly", "grafana-labs", "timescale", "prisma",
    "sentry", "gitlab", "docker", "hasura", "chronosphere", "teleport",
    "aiven", "ngrok", "fermyon", "tailscale", "warpstream", "prefect",
    "dagster-labs", "astronomer", "monte-carlo", "cribl", "airbyte",
    "stripe", "adyen", "block", "brex", "ramp", "mercury", "plaid", "wise",
    "revolut", "monzo", "n26", "nubank", "klarna", "affirm", "sofi",
    "robinhood", "coinbase", "kraken", "circle", "gemini", "anchorage-digital",
    "fireblocks", "blockchain-com", "chainalysis", "alchemy", "deel",
    "rippling", "gusto", "checkout-com", "gocardless", "airwallex", "mollie",
    "marqeta", "modern-treasury", "column", "increase", "bitso", "chime",
    "jane-street", "optiver", "imc", "hudson-river-trading", "drw",
    "jump-trading", "tower-research-capital", "worldquant", "two-sigma",
    "point72", "man-group", "aqr", "g-research", "gsa-capital", "pdt-partners",
    "squarepoint-capital", "the-voleon-group", "vatic-labs", "eclipse-trading",
    "akuna-capital", "belvedere-trading", "3red-partners", "alphagrep-securities",
    "aquatic-capital-management", "bluecrest-capital-management", "virtu-financial",
    "flow-traders", "schonfeld", "engineers-gate", "headlands-research",
    "radix-trading-experienced-job-board", "quantlab", "five-rings",
    "old-mission-capital", "wolverine-trading", "chicago-trading-company",
    "jpmorgan", "amazon", "reddit", "discord", "notion", "figma", "airtable",
    "linear", "loom", "miro", "canva", "grammarly", "1password", "dropbox",
    "box", "asana", "monday-com", "calendly", "zapier", "retool", "postman",
    "instacart", "doordash", "gopuff", "faire", "gong", "clari", "outreach",
    "amplitude", "mixpanel", "segment", "launchdarkly", "split", "statsig",
    "posthog", "algolia", "meilisearch", "typesense", "elastic",
    "hackerone", "snyk", "wiz", "lacework", "okta", "auth0", "duo-security",
    "crowdstrike", "sentinelone", "abnormal-security", "material-security",
    "socket", "chainguard", "sysdig", "aqua-security",
    "nvidia", "arm", "sifive", "rivos", "tenstorrent",
    "roblox", "unity", "epic-games", "riot-games", "scopely",
    "the-browser-company", "raycast", "warp", "zed-industries", "replit",
    "codeium", "tabnine", "sourcegraph", "weights-biases", "wandb", "comet-ml",
    "labelbox", "surge-ai", "invisible-technologies", "turing",
    "flexport", "samsara", "verkada", "gecko-robotics", "figure",
    "physical-intelligence", "skild-ai", "covariant", "chef-robotics",
    "waymo", "cruise", "zoox", "aurora", "nuro", "applied-intuition",
    "kodiak-robotics", "wayve", "helion-energy", "commonwealth-fusion-systems",
    "form-energy", "boston-metal", "twelve",
    "anduril", "shield-ai", "hadrian", "saronic", "castelion",
    "spacex", "relativity-space", "stoke-space",
}

IND_STRONG = {"1", "2"}                       # Technology, Financial Services
IND_OK = {"6", "15", "16", "18", "20", "21"}  # media/gaming, aero, auto, biotech, robotics, cyber


def _cfg(raw: str | None) -> dict:
    raw = (raw or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def monitor_needs_browser(name: str, cfg: dict) -> bool:
    if name in BROWSER_MONITORS:
        return True
    if name == "api_sniffer":
        return (not cfg.get("api_url")) or bool(cfg.get("browser"))
    if name in ("dom", "inline"):
        return bool(cfg.get("render"))
    if name == "nextdata":
        return bool(
            cfg.get("render") or cfg.get("actions")
            or cfg.get("source") == "browser" or cfg.get("browser_expression")
        )
    return False


# Monitors this fork can ingest as full rows: upstream's rich set PLUS
# 'workday' — jobseek's workday monitor is URL-only, but src/pgpipe/
# monitors_workday.py hits the Workday CXS API directly and returns full rows.
LOCAL_RICH = RICH_MONITORS | {"workday"}


def board_is_http_rich(row: dict) -> bool:
    mt = row["monitor_type"]
    return mt in LOCAL_RICH and not monitor_needs_browser(mt, _cfg(row.get("monitor_config")))


def main() -> None:
    companies = list(csv.DictReader(open("companies.csv.upstream", encoding="utf-8")))
    boards = list(csv.DictReader(open("boards.csv.upstream", encoding="utf-8")))
    comp_fields = list(companies[0].keys())
    board_fields = list(boards[0].keys())
    comp_by_slug = {c["slug"]: c for c in companies}

    boards_by_company: dict[str, list[dict]] = {}
    for b in boards:
        boards_by_company.setdefault(b["company_slug"], []).append(b)

    eligible: list[str] = []
    for slug, rows in boards_by_company.items():
        if slug not in comp_by_slug:
            continue
        if rows and all(board_is_http_rich(r) for r in rows):
            eligible.append(slug)
    eligible_set = set(eligible)

    def score(slug: str) -> tuple:
        c = comp_by_slug[slug]
        extras = c.get("extras") or ""
        ind = (c.get("industry") or "").strip()
        rows = boards_by_company[slug]
        s = 0
        if slug in CURATED:
            s += 10
        if "wikidataId" in extras:
            s += 5
        if "sameAs" in extras:
            s += 2
        if ind in IND_STRONG:
            s += 3
        elif ind in IND_OK:
            s += 1
        if any(r["monitor_type"] in ("greenhouse", "ashby", "lever") for r in rows):
            s += 1
        if len(rows) == 1:
            s += 1
        return (-s, slug)

    ranked = sorted(eligible, key=score)
    kept = set(ranked[:TARGET])
    kept |= {s for s in CURATED if s in eligible_set}

    trimmed_companies = sorted(
        (c for c in companies if c["slug"] in kept), key=lambda r: r["slug"]
    )
    trimmed_boards = sorted(
        (b for b in boards if b["company_slug"] in kept),
        key=lambda r: (r["company_slug"], r["board_slug"]),
    )

    with open("companies.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=comp_fields)
        w.writeheader()
        w.writerows(trimmed_companies)
    with open("boards.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=board_fields)
        w.writeheader()
        w.writerows(trimmed_boards)

    mt_counts: dict[str, int] = {}
    for b in trimmed_boards:
        mt_counts[b["monitor_type"]] = mt_counts.get(b["monitor_type"], 0) + 1

    print(f"eligible (rich + non-browser, all boards): {len(eligible)}")
    print(f"kept companies : {len(kept)}")
    print(f"kept boards    : {len(trimmed_boards)}")
    print(f"monitor mix    : {dict(sorted(mt_counts.items(), key=lambda x: -x[1]))}")
    curated_missing = sorted(
        s for s in CURATED
        if s in comp_by_slug and s not in eligible_set
    )
    print()
    print(f"curated names in upstream but NOT rich-eligible ({len(curated_missing)}):")
    for s in curated_missing:
        types = sorted({r["monitor_type"] for r in boards_by_company.get(s, [])})
        print(f"  {s}: {types}")
    print()
    absent = sorted(s for s in CURATED if s not in comp_by_slug)
    print(f"curated names absent from upstream companies.csv ({len(absent)}): {absent}")


if __name__ == "__main__":
    sys.exit(main())
