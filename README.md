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
│   ├── src/core/monitors/     reused verbatim from jobseek (the only reused code)
│   ├── src/ ...               rest of jobseek, inert (see UPSTREAM_DRIFT.md §2)
│   ├── data/companies.csv     ~500 companies (trimmed)
│   ├── data/boards.csv        ~516 rich-monitor boards (trimmed)
│   └── pyproject.toml         + `jobs = "src.pgpipe.cli:main"`
├── .github/workflows/
│   ├── crawl-jobs.yml         cron every 6h: migrate -> sync -> run (+close+prune)
│   └── cleanup-stale-jobs.yml cron daily: hard-delete postings unseen 5d
├── search/jobs_search.sql     FTS functions on crawler.job_posting
├── search/jobs_search.js      node-postgres / supabase-js wrappers
├── tools/trim_csvs.py         regenerates the trimmed CSVs from upstream
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
