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
| `cli.py` | `jobs` entrypoint: `migrate / sync / run / close-stale / prune / hard-delete / stats` |
| `config.py` | env-only settings (`LOCAL_DATABASE_URL`, `PGPIPE_*`). No pydantic, no Redis. |
| `db.py` | asyncpg pool; `search_path = <schema>,public`; applies `schema.sql` |
| `schema.sql` | compact schema in a **dedicated `crawler` Postgres schema** (idempotent) |
| `csv_sync.py` | `data/*.csv` → `crawler.company` / `crawler.job_board` |
| `queue.py` | `crawler.crawl_queue` — `SELECT … FOR UPDATE SKIP LOCKED`, 30-min stale-claim reclaim |
| `throttle.py` | in-process per-host politeness gate + global semaphore (replaces `redis_capacity.py`) |
| `http.py` | plain `httpx.AsyncClient` (browser-ish UA); skips `src.shared.http` proxy/SSRF stack |
| `ingest.py` | call `get_discoverer(monitor_type)(board, http)` → map `DiscoveredJob` → compact upsert |
| `runner.py` | orchestrates `run`: reclaim → enqueue → N workers → close-stale → prune → `crawl_run` row |

`jobs` is registered in `pyproject.toml [project.scripts]` (see
`patches/02-*.diff`). `uv.lock` is unchanged (no new dependencies —
`asyncpg`, `httpx`, `python-dotenv` were already there).

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

- **Eligible** = company has ≥1 board and *every* board uses a **rich** monitor
  (`m.rich` in upstream's registry: greenhouse, ashby, lever, recruitee, gem,
  pinpoint, oracle_hcm, amazon, rss, deel, jobylon, almacareer, …) that is
  **not** browser-backed. 4036 companies qualify upstream.
- **Ranked** by: curated brand list (+10), `wikidataId` in extras (+5),
  `sameAs` (+2), industry ∈ {Technology, Financial Services} (+3) or
  {media, aerospace, automotive, biotech, robotics, cyber} (+1),
  greenhouse/ashby/lever (+1), single-board (+1). Top 500 kept, plus any
  curated pick that is eligible.

Result: **500 companies / 516 boards**. Monitor mix:
`greenhouse 333, ashby 98, lever 35, rss 16, pinpoint 12, recruitee 7, gem 3,
almacareer 3, oracle_hcm 3, jobylon 2, amazon 1, deel 1, recruiter_co_kr 1,
inline 1`.

### Excluded — need jobseek's scraper pipeline (re-add later)

URL-only monitors were dropped because they need a scraper to get any job data.
Named targets lost this way: `google` (sitemap), `two-sigma` / `d-e-shaw-group`
(dom), `checkout-com` / `g-research` / `nvidia` / `crowdstrike` / `snyk`
(workday), `wise` (smartrecruiters), `huggingface` (workable), `rippling`
(rippling), `circle` / `canva` / `aiven` (sitemap), `revolut` (nextdata),
`castelion` (dom).

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

## 7. NOT verified from this environment

The sandbox blocked every outbound DB connection and every script making
outbound HTTP, so the following were **not run** and must be done by the user
(commands in `RUN_LOCAL.md`):

- the `information_schema.tables` collision check on the shared Supabase DB
  (mitigated by using the dedicated `crawler` schema unconditionally);
- `jobs migrate` / `jobs sync` / `jobs run` against the live DB;
- a real crawl pass, sample rows, and DB-size delta.

What **was** verified offline: `uv sync` + `uv lock --check`; `jobs --help` and
every subparser; `import src.pgpipe.*` + `src.core.monitors` (54 rich monitors);
`DiscoveredJob → compact row` mapping; `ruff` + `py_compile` clean on
`src/pgpipe/`; both workflow YAMLs parse; `search/jobs_search.{sql,js}`.

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
