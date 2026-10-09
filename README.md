# job-crawler

A trimmed, **$0-budget** job-posting crawler, adapted from the crawler of
[`colophon-group/jobseek`](https://github.com/colophon-group/jobseek)
(commit `73179987ad34d9bdb480c5845c0c24af5f862691`).

- **Compute** — GitHub Actions cron. Single process, runs to completion, exits.
  No server, no worker, no paid platform.
- **Queue** — **Postgres** (`crawler.crawl_queue`, `SELECT … FOR UPDATE SKIP
  LOCKED`). **No Redis / Upstash.**
- **Database** — Supabase Postgres (free tier), everything in a dedicated
  **`crawler`** schema so it never collides with the webapp tables.
- **Scope** — ~500 well-known companies (backend/distributed-systems, fintech,
  quant, GenAI, infra, security), ~516 boards, **rich HTTP monitors only**
  (Greenhouse, Ashby, Lever, Recruitee, Gem, Oracle HCM, Amazon, **Workday** via
  a compact CXS-API monitor). No browser, no scrapers, **no descriptions stored**.
- **Tech-only** — a title/department filter (`PGPIPE_TECH_ONLY`, default on)
  keeps engineering / applied-science / quant-dev roles and drops the rest.
  Every posting is tagged with a `seniority` bucket
  (intern/junior/mid/senior/staff/principal/unknown).
- **Search + analytics** — Postgres FTS (`search/jobs_search.sql`) and a
  `crawler.jobs_analytics()` RPC for the webapp analytics tab. No Typesense.

Nothing here touches the `resume-optimizer` repo.

---

## Layout

```
job-crawler/
├── crawler/
│   ├── src/pgpipe/            NEW — the entire Redis-free pipeline (jobs CLI)
│   ├── src/report/            NEW — daily market report (separate job, aggregates only)
│   ├── src/core/monitors/     reused verbatim from jobseek (the only reused code)
│   ├── src/ ...               rest of jobseek, inert (see UPSTREAM_DRIFT.md §2)
│   ├── data/companies.csv     ~500 companies (trimmed)
│   ├── data/boards.csv        ~516 rich-monitor boards (trimmed)
│   └── pyproject.toml         + `jobs = "src.pgpipe.cli:main"`
├── .github/workflows/
│   ├── crawl-jobs.yml         cron every 6h: migrate -> sync -> run (+close+prune)
│   ├── cleanup-stale-jobs.yml cron daily: hard-delete postings unseen 5d
│   └── market-report.yml      cron daily: market report -> report_daily / report_weekly
├── search/jobs_search.sql     FTS functions on crawler.job_posting
├── search/jobs_search.js      node-postgres / supabase-js wrappers
├── tools/trim_csvs.py         regenerates the trimmed CSVs from upstream
├── tools/build_report_data.py regenerates crawler/data/report/ + taxonomies
├── patches/                   the two diffs vs upstream (config, pyproject)
├── RUN_LOCAL.md               exact first-run commands
└── UPSTREAM_DRIFT.md          every deviation from upstream
```

---

## The `jobs` CLI

```
uv run --no-sync jobs migrate                  schema + seniority col + jobs_analytics() (idempotent)
uv run --no-sync jobs purge                    TRUNCATE job_posting + reset crawl_queue (clean re-scrape)
uv run --no-sync jobs sync                     data/*.csv -> crawler.company / job_board
uv run --no-sync jobs run [--minutes 20]       one bounded crawl pass
uv run --no-sync jobs close-stale [--days 3]   status='closed' for postings unseen N days
uv run --no-sync jobs prune [--cap 400]        keep newest N postings per company
uv run --no-sync jobs hard-delete [--days 5]   DELETE postings unseen N days
uv run --no-sync jobs stats                    row counts
```

`jobs run` = reclaim stale claims → enqueue every enabled board → N workers
claim + fetch + **tech-filter + seniority-tag** + upsert → close-stale → prune.
Clean re-scrape: `jobs migrate && jobs purge && jobs sync && jobs run`. See `RUN_LOCAL.md`.

---

## Jobs table (`crawler.job_posting`)

| column | type | notes |
|---|---|---|
| `id` | bigint identity | PK |
| `company_slug` | text | FK `crawler.company` |
| `board_slug` | text | FK `crawler.job_board` |
| `source` | text | monitor type, e.g. `greenhouse` |
| `external_id` | text | provider-stable id (`source_identity` / requisition id), nullable |
| `url` | text | apply / canonical URL — **unique with `company_slug`** |
| `title` | text | primary title |
| `titles` | jsonb | all localized titles, `[]` default |
| `location` | text | primary location string |
| `locations` | jsonb | all location strings, `[]` default |
| `location_type` | text | `remote` \| `hybrid` \| `onsite` \| null |
| `employment_type` | text | `full_time` \| `part_time` \| `contract` \| `internship` \| … |
| `department` | text | nullable |
| `date_posted` | text | provider-reported, as given, nullable |
| `first_seen_at` | timestamptz | set once on insert |
| `last_seen_at` | timestamptz | **bumped to `now()` on every upsert** — "seen live" |
| `status` | text | `open` \| `closed` (closed after 3 d unseen) |
| `search_tsv` | tsvector | generated from title+company+department+location; GIN indexed |

Indexes: PK; `unique(company_slug, url)`; `(company_slug)`, `(board_slug)`,
`(status)`, `(last_seen_at desc)`, `(company_slug, first_seen_at desc)`,
partial `(company_slug, external_id)`, GIN `(search_tsv)`.

---

## Market report (separate job)

`.github/workflows/market-report.yml` runs daily at 01:13 UTC and builds a
**report only** of the global job market — no job rows are stored and nothing
appears in the webapp. It is fully independent of `crawl-jobs`: its own board
list (`crawler/data/report/`, ~4.7k companies, all industries and roles), its
own code (`crawler/src/report/`), its own schemas.

```
crawl  (6 parallel shards)   fetch boards WITH descriptions -> derive per-job
                             attributes in memory -> shard parquet (runner disk)
build                        merge shards -> dedupe -> FX to USD -> compare with
                             yesterday -> rollup -> sections -> one transaction:
                               report_daily.report   overwrite the single row
                               report_weekly.report  fold the day into its ISO week
```

| table | rows | what's in it |
|---|---|---|
| `report_daily.report` | always 1 (`id = 1`) | today's report + day-over-day and vs-last-week deltas |
| `report_weekly.report` | 1 per ISO week | the week compiled from its daily reports: per-day averages for stocks (jobs, companies), totals for flows (new, removed), salary percentiles from the merged daily histograms, deltas vs previous week / 4 / 12 weeks; `status` = `in_progress` until the next week starts, then `final` |

Per job (derived in memory, then dropped): profile (occupation) and job family,
seniority, years of experience, salary (structured or parsed from the
description; annualised; converted to USD at that day's ECB rate), country /
region / city, work mode, employment type, technologies, education, visa
sponsorship, equity, clearance, language, posting age, company industry / size /
age, ATS source.

Report sections (JSONB, chart-ready `[{bucket, label, jobs, share_pct, new,
removed, remote_pct, salary_n, salary_disclosure_pct, salary_avg_usd,
salary_median_usd, salary_p10..p90_usd, yoe_median, d_jobs, d_jobs_pct, ...}]`):

| column | contents |
|---|---|
| `kpi`, `kpi_deltas` | headline numbers; change vs previous day / week |
| `profiles` | job families, profiles, top titles, seniority; risers/fallers (absolute and %), appeared/disappeared, for profiles, titles, countries, cities, technologies, companies, industries; profile × seniority / work mode, family × industry / company size / education |
| `salary` | overall; salary by every core dimension; histograms ($10k bins) overall and per family / seniority / YOE / region / country / profile; premiums (remote vs onsite per profile, city / technology / industry / company-size premium indexes vs the same profile's median); local-currency medians by country and country × family; disclosure rates by country / ATS / company |
| `experience` | YOE bands and exact years, salary curve by years, profile × YOE, family × YOE, seniority × YOE, country × YOE, entry-level share |
| `geography` | regions, countries, top cities, multi-location jobs, profile × country, family × country, country × seniority |
| `skills` | technologies, technology × family / YOE / country, technologies that appear together |
| `companies` | top hiring companies, industries, company size and age |
| `attributes` | work mode, employment type, education, visa, equity, clearance, language, ATS, posting age, how long removed jobs were open |
| `coverage`, `data_quality` | boards ok/failed per ATS, coverage %, salary parse rates, thin-sample flags, FX used |

Every salary figure carries its sample size (`salary_n`) and `salary_thin` when
it rests on fewer than 30 salaries. Percentiles come from 1% log-scale
histograms (4% in two-way tables), accurate to about ±0.5%.

Read it with the service-role key (RLS on, no public policies), selecting the
report columns — `rollup`, `baseline`, `job_state` and `prev_job_state` are
machinery for the weekly fold and day-over-day comparisons.

```bash
cd crawler
uv run --no-sync python -m src.report.cli crawl --shard 0 --shards 6 --out ../out
uv run --no-sync python -m src.report.cli build --in ../out --shards 6 [--dry-run --out r.json]
uv run --no-sync python -m src.report.cli show
```

Manual test run: Actions → market-report → *Run workflow* with
`board_limit = 10` and `dry_run = true` — the report JSON is uploaded as an
artifact and nothing is written to Supabase.

---

## Deploying

This repo has **no git remote** yet. To run it on GitHub Actions:

1. Create a GitHub repo and add it as a remote:
   ```bash
   git remote add origin git@github.com:<you>/job-crawler.git
   git push -u origin main
   ```
2. Repo → **Settings → Secrets and variables → Actions → New repository secret**:
   `LOCAL_DATABASE_URL` = your Supabase URI (session pooler, port 5432,
   `?sslmode=require`). That is the only secret. **No `REDIS_URL`.**
3. Actions tab → run **crawl-jobs** once via *Run workflow*, then
   **cleanup-stale-jobs** once. After that both run on cron
   (crawl every 6 h, cleanup daily 04:17 UTC).
4. Apply search helpers once: `psql "$LOCAL_DATABASE_URL" -f search/jobs_search.sql`.

See `UPSTREAM_DRIFT.md` for exactly what changed vs jobseek and how to pull a
newer upstream.
