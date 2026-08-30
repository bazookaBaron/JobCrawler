# RUN_LOCAL — first run

Prereqs: `uv` (`pipx install uv`), a Supabase project (free tier). **No Redis.**
Python 3.13 is fetched by `uv`. All commands run from **`crawler/`**.

---

## 0. Credentials

```bash
cp ../.env.local.example crawler/.env.local
# edit crawler/.env.local -> set LOCAL_DATABASE_URL to the Supabase URI
# (Session pooler, port 5432, ?sslmode=require). That is the only required var.
```

`crawler/.env.local` is git-ignored and auto-loaded by `src/pgpipe/config.py`.

---

## 1. Install

```bash
cd crawler
uv sync
uv run --no-sync jobs --help          # migrate | sync | run | close-stale | prune | hard-delete | stats
```

---

## 2. Create the schema

```bash
uv run --no-sync jobs migrate
```

Creates schema **`crawler`** and its tables (`company`, `job_board`,
`crawl_queue`, `job_posting`, `crawl_run`) + the `job_posting.seniority`
column, the `search_tsv` generated column + GIN index, the
`crawler.jobs_analytics()` RPC, and `GRANT … TO service_role`. Idempotent —
safe to re-run. Nothing is created in `public`, so it cannot collide with the
webapp's tables.

> For the webapp's `supabaseAdmin.schema('crawler').rpc('jobs_analytics')` to
> resolve, add **`crawler`** under Supabase → Settings → API → *Exposed schemas*.

### 2b. Clean re-scrape

`jobs run` only *upserts* (it never deletes rows that stopped matching a
filter). To rebuild the table tech-only from scratch:

```bash
uv run --no-sync jobs purge      # TRUNCATE crawler.job_posting + reset crawl_queue
```

Full clean cycle: `jobs migrate && jobs purge && jobs sync && jobs run --minutes 20`.

> Optional sanity check that nothing in `public` clashes:
> ```sql
> SELECT table_name FROM information_schema.tables
> WHERE table_schema='public'
>   AND table_name IN ('company','job_board','job_posting','crawl_queue','descriptions');
> ```
> Expect **zero rows**. (If your webapp genuinely owns a `company`/`job_posting`
> in `public` it does not matter — this fork only ever touches schema `crawler`.)

---

## 3. Load companies + boards

```bash
uv run --no-sync jobs sync
# -> {"sync": {"companies": 500, "boards": 516, "boards_disabled_total": 0}}
```

---

## 4. One crawl pass

```bash
uv run --no-sync jobs run --minutes 20
```

What it does, in order: reclaim `claimed` queue rows older than 30 min →
enqueue every enabled board → up to `PGPIPE_CONCURRENCY` (12) workers
`claim (FOR UPDATE SKIP LOCKED)` + fetch → **tech-filter** (`PGPIPE_TECH_ONLY`,
default on) → **seniority-tag** → upsert → `close-stale` (3 d) →
`prune` (400/company) → write a `crawl_run` row. Prints a JSON summary:

```json
{"boards_ok": 511, "boards_failed": 15, "postings_upserted": 15434,
 "postings_dropped_nontech": 55781, "postings_pruned": 4367,
 "boards_remaining_pending": 0, "elapsed_s": 450.3, ...}
```

(Real numbers from the first live run: 516 boards, ~7.5 min, ~22 % of fetched
postings kept as tech roles.) Re-run any time — it is incremental (upsert on
`(company_slug, url)`, `last_seen_at` bumped). `crawl_run.notes` records the
kept/dropped split.

---

## 5. Verify

```bash
uv run --no-sync jobs stats
```

```sql
-- newest 10 jobs
SELECT company_slug, title, seniority, location, location_type, url, source,
       first_seen_at, last_seen_at, status
FROM crawler.job_posting
ORDER BY first_seen_at DESC
LIMIT 10;

-- seniority distribution
SELECT seniority, count(*) FROM crawler.job_posting
WHERE status='open' GROUP BY 1 ORDER BY 2 DESC;

-- the whole analytics-tab payload
SELECT crawler.jobs_analytics();

-- workday roles
SELECT company_slug, title, location, url FROM crawler.job_posting
WHERE source='workday' ORDER BY first_seen_at DESC LIMIT 10;

-- per-company counts
SELECT company_slug, count(*) FROM crawler.job_posting
WHERE status='open' GROUP BY 1 ORDER BY 2 DESC LIMIT 20;

-- board health
SELECT count(*) FILTER (WHERE last_success_at IS NOT NULL) AS ok,
       count(*) FILTER (WHERE last_error IS NOT NULL)      AS errored,
       count(*) FILTER (WHERE NOT is_enabled)              AS disabled_gone
FROM crawler.job_board;
```

---

## 6. Search

```bash
psql "$LOCAL_DATABASE_URL" -f ../search/jobs_search.sql   # once
```

adds `crawler.search_jobs()`, `crawler.list_jobs()`, `crawler.job_counts_by_company()`.
Use `../search/jobs_search.js` from a Node backend, or call directly:

```sql
SELECT id, company_slug, title, rank, total_count
FROM crawler.search_jobs('distributed systems rust', NULL, NULL, 'open', 10, 0);
```

---

## Manual cleanup (also automated in the two workflows)

```bash
uv run --no-sync jobs close-stale --days 3     # status='closed' when unseen 3 d
uv run --no-sync jobs prune --cap 400          # newest 400 per company
uv run --no-sync jobs hard-delete --days 5     # DELETE when unseen 5 d
```

---

## Expected failure modes (normal)

| symptom | meaning | action |
|---|---|---|
| `asyncpg … password authentication failed` / `ConnectionRefused` | wrong `LOCAL_DATABASE_URL` | use the Supabase **session pooler** URI, port 5432, `?sslmode=require` |
| a board logs `HTTPStatusError 403/404` | company moved / removed its board | recorded in `job_board.last_error`; `BoardGoneError` (e.g. Greenhouse 404) auto-sets `is_enabled=false` |
| `429` on greenhouse/ashby | politeness too aggressive | raise `PGPIPE_DELAY_ATS` or lower `PGPIPE_CONCURRENCY` |
| `boards_remaining_pending > 0` after a pass | 20 min didn't drain 516 first-time boards | run `jobs run` again (queue resumes) |
| `boards_failed` in the tens | expected — some ATS tokens in the CSV are stale | check `SELECT board_slug,last_error FROM crawler.job_board WHERE last_error IS NOT NULL` |

Rough healthy first pass: **~440-490 of 516 boards succeed**, tens of thousands
of `job_posting` rows (Amazon/greenhouse-heavy companies dominate), a few MB of
table + index growth.
