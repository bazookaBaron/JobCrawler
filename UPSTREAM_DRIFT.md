# Upstream drift — job-crawler vs. jobseek

Derived from the **crawler only** of
[`colophon-group/jobseek`](https://github.com/colophon-group/jobseek).

| | |
|---|---|
| Upstream repo | `https://github.com/colophon-group/jobseek` |
| Upstream commit (`--depth 1`) | **`73179987ad34d9bdb480c5845c0c24af5f862691`** |
| Cloned | 2026-08-30 |
| Upstream path taken | `apps/crawler/` → `./crawler/` |
| Python | `3.13` |

This fork keeps **only** jobseek's monitor modules (`src/core/monitors/*`) and
`DiscoveredJob`. Everything operational — the Redis queue, the distributed
worker pool, scrapers, description fetch, R2, Typesense, enrichment, Alembic —
is bypassed by a new single-process package, **`src/pgpipe/`**.

---

## 1. New code (not in upstream): `crawler/src/pgpipe/`

| file | purpose |
|---|---|
| `cli.py` | `jobs` entrypoint: `migrate / purge / sync / run / close-stale / prune / hard-delete / stats` |
| `config.py` | env-only settings (`LOCAL_DATABASE_URL`, `PGPIPE_*`). No pydantic, no Redis. |
| `db.py` | asyncpg pool; `search_path = <schema>,public`; applies `schema.sql` |
| `schema.sql` | compact schema in a **dedicated `crawler` Postgres schema** (idempotent); `seniority` column; `jobs_analytics()` RPC; `service_role` grants |
| `csv_sync.py` | `data/*.csv` → `crawler.company` / `crawler.job_board` |
| `queue.py` | `crawler.crawl_queue` — `SELECT … FOR UPDATE SKIP LOCKED`, 30-min stale-claim reclaim |
| `throttle.py` | in-process per-host politeness gate + global semaphore (replaces `redis_capacity.py`) |
| `http.py` | plain `httpx.AsyncClient` (browser-ish UA); skips `src.shared.http` proxy/SSRF stack |
| `tech_filter.py` | `is_tech_role(title, department)` — ALLOW/BLOCK regex lists; `PGPIPE_TECH_ONLY` (default 1) |
| `seniority.py` | `seniority_of(title)` → intern/junior/mid/senior/staff/principal/unknown |
| `monitors_workday.py` | compact **rich** Workday CXS-API monitor (jobseek's is URL-only) |
| `ingest.py` | dispatch monitor (incl. Workday) → map `DiscoveredJob` → tech-filter → seniority-tag → compact upsert |
| `runner.py` | orchestrates `run`: reclaim → enqueue → N workers → close-stale → prune → `crawl_run` row (with kept/dropped counts in `notes`) |

`jobs` is registered in `pyproject.toml [project.scripts]` (see
`patches/02-*.diff`). `uv.lock` is unchanged (no new dependencies —
`asyncpg`, `httpx`, `python-dotenv` were already there).

---

## 1b. Round-2 additions (tech filter, seniority, Workday, analytics RPC)

- **Tech-only filter** — `src/pgpipe/tech_filter.py::is_tech_role(title, department)`.
  ALLOW list (software engineer, sde/swe, backend/frontend/full-stack, sre,
  data/ml/security engineer, applied/research scientist, quant developer, …)
  and a BLOCK list that wins even on an ALLOW match (sales/solutions/support
  engineer, account, recruit, marketing, financial, collections, relationship
  manager, branch). Applied in `ingest.crawl_board` **before upsert** when
  `PGPIPE_TECH_ONLY` ≠ 0 (default on). Per-run kept/dropped counts go into
  `crawl_run.notes` and the `jobs run` JSON (`postings_dropped_nontech`).
- **`seniority` column** on `crawler.job_posting` (`text NOT NULL DEFAULT
  'unknown'`, indexed). Derived at ingest by `src/pgpipe/seniority.py::
  seniority_of(title)` — most-specific-first so "Senior Staff Engineer" →
  `principal`, "New Grad SWE Intern" → `intern`. A bare "Software Engineer"
  with no level cue → `unknown` (per the spec's `else unknown`).
- **Workday** — `src/pgpipe/monitors_workday.py`. jobseek's
  `src/core/monitors/workday.py` is URL-only (needs a scraper); this hits the
  Workday CXS search API directly
  (`POST …/wday/cxs/{tenant}/{site}/jobs`, paginate by 20 to `total`, cap
  2000) and returns full `DiscoveredJob` rows with `source='workday'`.
  `ingest._discover` routes `monitor_type == 'workday'` here.
  `tools/trim_csvs.py` adds `workday` to the eligible monitor set
  (`LOCAL_RICH`), so the trimmed CSVs now include 27 Workday boards
  (nvidia, crowdstrike, snyk, checkout-com, g-research, paypal, adobe, …).
  `_cfg` resolves tenant/dc/site from any of jobseek's config key spellings
  (`company`/`tenant`, `wd_instance`/`dc`, `site`/`board`/`site_id`/…) and
  falls back to parsing the `board_url`.
- **`jobs purge`** — `TRUNCATE crawler.job_posting RESTART IDENTITY` +
  `TRUNCATE crawler.crawl_queue RESTART IDENTITY` for a clean re-scrape.
  Companies/boards are untouched.
- **`crawler.jobs_analytics()`** — STABLE SQL function in `schema.sql`
  returning one JSON object for the webapp analytics tab
  (`total`, `companies_with_jobs`, `by_seniority[7]`, `by_work_type[4]`,
  `by_source[]`, `top_companies[12]`, `posted_by_day[21]`, `mid_share_pct`),
  all over `status='open'`. `schema.sql` also `GRANT EXECUTE ON ALL FUNCTIONS
  … TO service_role` (+ default privileges). Called as
  `supabaseAdmin.schema('crawler').rpc('jobs_analytics')` — the `crawler`
  schema must be added under Supabase → Settings → API → **Exposed schemas**.
- **`search/jobs_search.{sql,js}`** — `search_jobs()` / `list_jobs()` gained a
  `p_seniority text[]` filter and return the `seniority` column; JS adds a
  `jobsAnalytics(db)` wrapper. (Signatures changed → the file `DROP FUNCTION`s
  the old ones first.)

---

## 2. Redis removed

**Not an in-place rewrite of `src/redis_queue.py`.** That module (39 KB, ~35
functions, Lua scripts, inflight leases, circuit breakers, dead-letters) exists
to coordinate *multiple distributed browser/simple workers*. A single-process
cron needs none of it, so `pgpipe` implements the small slice that matters:

| jobseek (Redis) | this fork (Postgres) |
|---|---|
| `redis_queue.claim_work()` (Lua ZSET pop + inflight lease) | `queue.claim_one()` — `SELECT id … WHERE status='pending' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1`, then `UPDATE … status='claimed'` |
| inflight lease + reaper (`reap_expired`) | `queue.reclaim_stale()` — `UPDATE crawl_queue SET status='pending' WHERE status='claimed' AND claimed_at < now() - interval '30 min'`, run at the start of every `jobs run` |
| `enqueue_monitors()` / tiered ready queues | `queue.enqueue_due()` — one `pending` row per enabled board; partial unique index `crawl_queue_live_board_uniq` makes it race-free |
| `redis_capacity.py` per-domain token bucket | `throttle.Politeness` — per-host `asyncio.Lock` + "next OK time" + jitter; ATS hosts 0.6 s, others 2.0 s; global `Semaphore(PGPIPE_CONCURRENCY)` |
| host / provider circuit breakers | dropped — `job_board.consecutive_failures` + queue `attempts`/`max_attempts` back-off instead |

Config change: `src/config.py` — `redis_url`, `upstash_redis_rest_url`,
`upstash_redis_rest_token` deleted (`patches/01-config-remove-redis.diff`).

**Dead code, kept but never invoked** (deleting them cascades through
`cli.py`/`sync.py`/`exporter.py`/`processing/*` for no benefit to `jobs`):
`src/redis_queue.py`, `src/redis_capacity.py`, `src/workers/pipeline.py`,
`src/shared/redis.py`, `src/exporter.py`, and the `crawler` / `ws` / `labeller`
entrypoints. They will raise if run (missing `settings.redis_url`).

---

## 3. Compact schema — no descriptions

All objects in schema **`crawler`** (chosen because the live collision check
could not be run from this environment — a dedicated schema is collision-proof
regardless of what the webapp uses; override with `PGPIPE_SCHEMA`).

`crawler.job_posting` columns: `id, company_slug, board_slug, source,
external_id, url, title, titles (jsonb), location, locations (jsonb),
location_type, employment_type, department, date_posted, first_seen_at,
last_seen_at, status` + a generated `search_tsv`. **No** salary / experience /
occupation / seniority / technology / enrichment / R2 / location-id arrays, and
**no `descriptions` table**.

`ingest.py` never reads `DiscoveredJob.description` and never calls a scraper,
so no description HTML is fetched or stored (rich monitors return it inline;
it is simply dropped). "last time seen live" = **`last_seen_at`**, set to
`now()` on every upsert (`ingest._UPSERT`).

Other new tables: `crawler.company`, `crawler.job_board`, `crawler.crawl_queue`,
`crawler.crawl_run` (per-run log).

---

## 4. Retention / cleanup

| when | action | where |
|---|---|---|
| end of every `jobs run` | `UPDATE … SET status='closed' WHERE last_seen_at < now() - interval '3 days' AND status <> 'closed'` | `cleanup.close_stale` |
| end of every `jobs run` | keep newest **400** per company: `DELETE … WHERE id IN (SELECT id FROM (… row_number() OVER (PARTITION BY company_slug ORDER BY first_seen_at DESC)) WHERE rn > 400)` | `cleanup.prune_per_company` |
| daily workflow `cleanup-stale-jobs.yml` | `DELETE FROM crawler.job_posting WHERE last_seen_at < now() - interval '5 days'` + a prune pass | `cleanup.hard_delete_stale` |

Thresholds overridable: `PGPIPE_CLOSE_AFTER_DAYS`, `PGPIPE_PER_COMPANY_CAP`,
`PGPIPE_DELETE_AFTER_DAYS`.

---

## 5. CSV trim — ~500 companies

`tools/trim_csvs.py` selection (from `*.csv.upstream`):

- **Eligible** = company has ≥1 board and *every* board uses a monitor in
  `LOCAL_RICH` = upstream's rich registry (greenhouse, ashby, lever, recruitee,
  gem, pinpoint, oracle_hcm, amazon, rss, deel, jobylon, almacareer, …) **plus
  `workday`** (rich here via `src/pgpipe/monitors_workday.py`), and not
  browser-backed. 4197 companies qualify upstream.
- **Ranked** by: curated brand list (+10), `wikidataId` in extras (+5),
  `sameAs` (+2), industry ∈ {Technology, Financial Services} (+3) or
  {media, aerospace, automotive, biotech, robotics, cyber} (+1),
  greenhouse/ashby/lever (+1), single-board (+1). Top 500 kept, plus any
  curated pick that is eligible.

Result: **500 companies / 516 boards**. Monitor mix:
`greenhouse 310, ashby 94, lever 35, workday 27, rss 16, pinpoint 12,
recruitee 7, gem 3, almacareer 3, oracle_hcm 3, jobylon 2, amazon 1, deel 1,
recruiter_co_kr 1, inline 1`.

### Excluded — need jobseek's scraper pipeline (re-add later)

URL-only monitors were dropped because they need a scraper to get any job data.
Named targets lost this way: `google` (sitemap), `two-sigma` / `d-e-shaw-group`
(dom), `wise` (smartrecruiters), `huggingface` (workable), `rippling`
(rippling), `circle` / `canva` / `aiven` (sitemap), `revolut` (nextdata),
`castelion` (dom). **Workday is now supported** — `checkout-com`, `g-research`,
`nvidia`, `crowdstrike`, `snyk` (and 22 more Workday boards) are back in.

### Excluded — Playwright/browser (from the earlier phase, still excluded)

`citadel`, `citadel-securities`, `balyasny-asset-management`, `revolut`,
`susquehanna-international-group` — `render:true` scrapers or
`api_sniffer` with `browser:true`.

### Data files removed from `crawler/data/`

`company_descriptions.csv`, `industries.csv`, `occupations.csv`,
`occupation_domains.csv`, `seniority.csv`, `technologies.csv`, the `*.svg`s,
`labeller_optout.txt`, `images/` — this fork does no taxonomy resolution.
Also removed earlier: `Dockerfile`, `docker-compose.yml`, `deploy*.sh`,
`alerts.yaml`, `alloy.river`, `grafana-dashboard.json`, `runtime-cost/`,
`murmur/`, `ws-package/`.

---

## 6. GitHub Actions

- **`.github/workflows/crawl-jobs.yml`** (cron `0 */6 * * *`): checkout →
  `setup-uv` (py 3.13) → `uv sync --frozen` → `jobs migrate` → `jobs sync` →
  `jobs run --minutes 20` (which also closes 3-day-stale + prunes to 400) →
  step-summary from the JSON each command prints + `jobs stats`. `set -euo
  pipefail` → any crawl failure fails the job.
- **`.github/workflows/cleanup-stale-jobs.yml`** (cron `17 4 * * *`):
  `jobs hard-delete --days 5` + `jobs prune`, deleted-count to the summary.
- Only secret: **`LOCAL_DATABASE_URL`**. No `REDIS_URL`.

---

## 7. Verification status

**Verified against the live shared Supabase DB (round 2):** `jobs migrate`
(schema + `seniority` column + `jobs_analytics()` + grants applied);
`jobs sync` (500 companies / 516 boards incl. 27 Workday); `jobs stats`;
`crawler.jobs_analytics()` returns the exact contract shape; a read-only
projection of `tech_filter` + `seniority` over the existing rows.

**Blocked for the agent, must be run by the user:** `jobs purge` (TRUNCATE —
the sandbox blocks bulk-destructive DB ops). Run the clean re-scrape yourself:
`jobs migrate && jobs purge && jobs sync && jobs run --minutes 20`.
Also add `crawler` to Supabase → Settings → API → **Exposed schemas** so the
webapp's `.schema('crawler').rpc('jobs_analytics')` resolves.

**Verified offline:** `uv sync` + `uv lock --check`; `jobs --help` + every
subparser; `import src.pgpipe.*` + `src.core.monitors`; `is_tech_role` /
`seniority_of` unit checks; Workday `_cfg` + posting parse over all 5 upstream
config spellings; `ruff` + `py_compile` clean; both workflow YAMLs;
`search/jobs_search.{sql,js}`.

---

## 8. Re-syncing with a newer upstream

```bash
git clone --depth 1 https://github.com/colophon-group/jobseek /tmp/jobseek-new
NEW_SHA=$(git -C /tmp/jobseek-new rev-parse HEAD)

# monitors + shared helpers (review the diff — pgpipe depends on these)
rsync -a --delete /tmp/jobseek-new/apps/crawler/src/core/  crawler/src/core/
rsync -a          /tmp/jobseek-new/apps/crawler/src/shared/ crawler/src/shared/

# re-check that our rich/browser assumptions still hold, then rebuild CSVs
cd crawler && uv run --no-sync python -c \
  "import src.core.monitors as m; print(sorted(m.api_monitor_types()))"
cp /tmp/jobseek-new/apps/crawler/data/companies.csv data/companies.csv.upstream
cp /tmp/jobseek-new/apps/crawler/data/boards.csv    data/boards.csv.upstream
( cd data && python ../../tools/trim_csvs.py )

# re-apply patches/01 + patches/02 if src/config.py or pyproject.toml moved,
# then bump the SHA at the top of this file to $NEW_SHA.
```
