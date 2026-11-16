-- pgpipe schema — compact, Redis-free job crawler storage.
-- Idempotent: safe to run on every `jobs migrate`. All objects live in a
-- dedicated schema (default "crawler") so they never collide with the
-- webapp tables that share this Supabase database.
--
-- The schema name is injected by src/pgpipe/db.py as {{SCHEMA}} (an
-- asyncpg-safe identifier). Do not hard-code it here.

CREATE SCHEMA IF NOT EXISTS {{SCHEMA}};

-- --- companies (from data/companies.csv) ----------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.company (
    slug         text PRIMARY KEY,
    name         text NOT NULL,
    website      text,
    industry     smallint,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- company_type: hand-curated classification from data/companies.csv (see
-- src/pgpipe/csv_sync.py). 'unknown' is a safe default the downstream
-- (webapp) weighting can treat as low-priority-but-not-excluded.
ALTER TABLE {{SCHEMA}}.company
    ADD COLUMN IF NOT EXISTS company_type text NOT NULL DEFAULT 'unknown'
    CHECK (company_type IN ('product', 'startup', 'service', 'unknown'));

-- --- boards (from data/boards.csv) --------------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.job_board (
    board_slug            text PRIMARY KEY,
    company_slug          text NOT NULL REFERENCES {{SCHEMA}}.company(slug) ON DELETE CASCADE,
    board_url             text NOT NULL,
    monitor_type          text NOT NULL,
    monitor_config        jsonb NOT NULL DEFAULT '{}'::jsonb,
    is_enabled            boolean NOT NULL DEFAULT true,
    last_attempt_at       timestamptz,
    last_success_at       timestamptz,
    last_error            text,
    consecutive_failures  int NOT NULL DEFAULT 0,
    last_job_count        int,
    updated_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS job_board_company_idx ON {{SCHEMA}}.job_board(company_slug);
-- crawl_run.id of the last run in which this board was crawled SUCCESSFULLY.
ALTER TABLE {{SCHEMA}}.job_board ADD COLUMN IF NOT EXISTS last_success_run_id bigint;

-- --- work queue (Postgres replaces Upstash Redis) ----------------------
-- One row per board per crawl cycle. Claimed with
--   SELECT ... FOR UPDATE SKIP LOCKED
-- so multiple workers (even multiple concurrent Actions runs) never double
-- process a board. Stale 'claimed' rows are reset at the start of each run.
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.crawl_queue (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    board_slug    text NOT NULL REFERENCES {{SCHEMA}}.job_board(board_slug) ON DELETE CASCADE,
    company_slug  text NOT NULL,
    status        text NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'claimed', 'done', 'error')),
    attempts      int NOT NULL DEFAULT 0,
    enqueued_at   timestamptz NOT NULL DEFAULT now(),
    claimed_at    timestamptz,
    claimed_by    text,
    finished_at   timestamptz,
    last_error    text
);
-- at most one live (pending/claimed) task per board
CREATE UNIQUE INDEX IF NOT EXISTS crawl_queue_live_board_uniq
    ON {{SCHEMA}}.crawl_queue(board_slug)
    WHERE status IN ('pending', 'claimed');
CREATE INDEX IF NOT EXISTS crawl_queue_pending_idx
    ON {{SCHEMA}}.crawl_queue(id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS crawl_queue_claimed_idx
    ON {{SCHEMA}}.crawl_queue(claimed_at) WHERE status = 'claimed';

-- --- job postings (compact — NO description body) ---------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.job_posting (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_slug    text NOT NULL REFERENCES {{SCHEMA}}.company(slug) ON DELETE CASCADE,
    board_slug      text REFERENCES {{SCHEMA}}.job_board(board_slug) ON DELETE SET NULL,
    source          text NOT NULL,               -- monitor type, e.g. 'greenhouse'
    external_id     text,                        -- provider-stable id / source_identity
    url             text NOT NULL,               -- apply / canonical posting URL
    title           text,                        -- primary (first) title
    titles          jsonb NOT NULL DEFAULT '[]'::jsonb,   -- all localized titles
    location        text,                        -- primary location string
    locations       jsonb NOT NULL DEFAULT '[]'::jsonb,   -- all location strings
    location_type   text,                        -- remote | hybrid | onsite | null
    employment_type text,                        -- full_time | part_time | contract | intern | ...
    department      text,
    date_posted     text,                        -- provider-reported, as given (may be null)
    first_seen_at   timestamptz NOT NULL DEFAULT now(),
    last_seen_at    timestamptz NOT NULL DEFAULT now(),
    status          text NOT NULL DEFAULT 'open' -- open | closed
                    CHECK (status IN ('open', 'closed')),
    UNIQUE (company_slug, url)
);
CREATE INDEX IF NOT EXISTS job_posting_company_idx   ON {{SCHEMA}}.job_posting(company_slug);
CREATE INDEX IF NOT EXISTS job_posting_board_idx     ON {{SCHEMA}}.job_posting(board_slug);
CREATE INDEX IF NOT EXISTS job_posting_status_idx    ON {{SCHEMA}}.job_posting(status);
CREATE INDEX IF NOT EXISTS job_posting_last_seen_idx ON {{SCHEMA}}.job_posting(last_seen_at DESC);
-- Composite (company_slug, first_seen_at) index — also serves the webapp's
-- "new postings per subscribed company since watermark" notification query
-- (a simple inequality on first_seen_at uses this fine regardless of the
-- DESC sort direction), so no separate ASC variant is added.
CREATE INDEX IF NOT EXISTS job_posting_first_seen_idx
    ON {{SCHEMA}}.job_posting(company_slug, first_seen_at DESC);
CREATE INDEX IF NOT EXISTS job_posting_ext_idx
    ON {{SCHEMA}}.job_posting(company_slug, external_id) WHERE external_id IS NOT NULL;

-- seniority bucket derived from the title at ingest (src/pgpipe/seniority.py):
-- intern | junior | mid | senior | staff | principal | unknown
ALTER TABLE {{SCHEMA}}.job_posting
    ADD COLUMN IF NOT EXISTS seniority text NOT NULL DEFAULT 'unknown';
CREATE INDEX IF NOT EXISTS job_posting_seniority_idx ON {{SCHEMA}}.job_posting(seniority);

-- country bucket derived offline (no geocoding API) from location/locations
-- at ingest (src/pgpipe/country.py): 'US' | 'IN' | 'remote' | 'other'.
-- The CHECK constraint is (re)defined idempotently in the v2 section below so
-- databases created before 'remote' existed get upgraded.
ALTER TABLE {{SCHEMA}}.job_posting
    ADD COLUMN IF NOT EXISTS country text;
-- The webapp filters `WHERE country IN ('US','IN')` on every jobs-board
-- request; partial index on the open set keeps that fast.
CREATE INDEX IF NOT EXISTS job_posting_country_idx
    ON {{SCHEMA}}.job_posting(country) WHERE status = 'open';

-- Real posting timestamp, parsed offline from the raw provider `date_posted`
-- string at ingest (src/pgpipe/posted_at.py; dateutil handles the mixed
-- ISO-8601 / RFC-2822 formats different ATSs return). Deliberately distinct
-- from first_seen_at: first_seen_at is "when WE discovered this row" (used
-- for storage retention/cleanup), posted_at is "when the job was actually
-- posted" (used by the webapp's 24h freshness gate) — a listing we only just
-- crawled for the first time can still be weeks old. Falls back to now() at
-- insert time (COALESCE in the upsert) when the source gives no date at all,
-- and never regresses to null on a later crawl pass that omits it. UNLIKE
-- country, this DOES need a backfill for pre-existing rows: date_posted text
-- for currently-open postings goes back to 2019, so 1-day churn alone won't
-- correct it — run `jobs backfill-posted-at` once after this migration.
ALTER TABLE {{SCHEMA}}.job_posting
    ADD COLUMN IF NOT EXISTS posted_at timestamptz,
    ADD COLUMN IF NOT EXISTS closed_at timestamptz;
CREATE INDEX IF NOT EXISTS job_posting_posted_at_idx
    ON {{SCHEMA}}.job_posting(posted_at DESC, id DESC) WHERE status = 'open';

-- --- simple full-text search over title + company (no description) ----
ALTER TABLE {{SCHEMA}}.job_posting
    ADD COLUMN IF NOT EXISTS search_tsv tsvector
    GENERATED ALWAYS AS (
        to_tsvector('english',
            coalesce(title, '') || ' ' || coalesce(company_slug, '') || ' ' ||
            coalesce(department, '') || ' ' || coalesce(location, ''))
    ) STORED;
CREATE INDEX IF NOT EXISTS job_posting_search_idx
    ON {{SCHEMA}}.job_posting USING gin (search_tsv);

-- --- webapp read-path indexes --------------------------------------
-- The Vloombox app (frontend/server.js, /api/jobs*) reads this table with:
--   WHERE status='open' AND posted_at >= <cutoff> ORDER BY posted_at DESC, id DESC
--   (first_seen_at is still used for the SEO "content freshness" signal on
--   /jobs/company/:slug — that's crawl activity, not posting age, so it kept
--   its own index below rather than switching to posted_at)
--   distinct `source` over the open set                (source facet)
--   company_slug / location / title  ILIKE '%...%'      (filters + /api/jobs/facets)
-- The crawler's own writes don't need these; they're purely for the reader.
-- Wrapped in a DO block so `jobs migrate` can't abort if pg_trgm's opclass
-- isn't on this connection's search_path (Supabase installs it in `extensions`).
-- The always-safe partial btree indexes still get created in that case; run the
-- *_trgm_idx statements once from the Supabase SQL editor if the NOTICE fires.
DO $webapp_idx$
BEGIN
    BEGIN
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'pg_trgm unavailable (%): trigram indexes skipped', SQLERRM;
    END;

    EXECUTE $q$
        CREATE INDEX IF NOT EXISTS job_posting_open_recent_idx
            ON {{SCHEMA}}.job_posting (first_seen_at DESC, id DESC)
            WHERE status = 'open'
    $q$;
    EXECUTE $q$
        CREATE INDEX IF NOT EXISTS job_posting_open_source_idx
            ON {{SCHEMA}}.job_posting (source)
            WHERE status = 'open' AND source IS NOT NULL
    $q$;

    BEGIN
        EXECUTE $q$ CREATE INDEX IF NOT EXISTS job_posting_company_trgm_idx
            ON {{SCHEMA}}.job_posting USING gin (company_slug gin_trgm_ops) $q$;
        EXECUTE $q$ CREATE INDEX IF NOT EXISTS job_posting_location_trgm_idx
            ON {{SCHEMA}}.job_posting USING gin (location gin_trgm_ops) $q$;
        EXECUTE $q$ CREATE INDEX IF NOT EXISTS job_posting_title_trgm_idx
            ON {{SCHEMA}}.job_posting USING gin (title gin_trgm_ops) $q$;
    EXCEPTION WHEN undefined_object OR undefined_function THEN
        RAISE NOTICE 'pg_trgm opclass not on search_path — create job_posting_*_trgm_idx manually in the Supabase SQL editor';
    END;
END
$webapp_idx$;

-- --- run log (one row per `jobs run`) --------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.crawl_run (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz,
    boards_total   int,
    boards_ok      int,
    boards_failed  int,
    postings_upserted int,
    postings_closed   int,
    postings_pruned   int,
    notes          text
);

-- --- analytics RPC (webapp analytics tab) --------------------------------
-- One JSON object, all counts over status='open'. Called by the webapp as
--   supabaseAdmin.schema('crawler').rpc('jobs_analytics')
-- (the 'crawler' schema must be in Supabase -> Settings -> API -> Exposed schemas).
CREATE OR REPLACE FUNCTION {{SCHEMA}}.jobs_analytics()
RETURNS json
LANGUAGE sql
STABLE
AS $fn$
WITH j AS (
    SELECT company_slug, source, seniority, location_type, first_seen_at
    FROM {{SCHEMA}}.job_posting
    WHERE status = 'open'
),
tot AS (
    SELECT count(*)::int AS total,
           count(DISTINCT company_slug)::int AS companies_with_jobs
    FROM j
),
sen AS (
    SELECT b.bucket, b.ord, coalesce(c.n, 0)::int AS count
    FROM (VALUES ('intern',1),('junior',2),('mid',3),('senior',4),
                 ('staff',5),('principal',6),('unknown',7)) AS b(bucket, ord)
    LEFT JOIN (
        SELECT coalesce(seniority, 'unknown') AS bucket, count(*) AS n
        FROM j GROUP BY 1
    ) c ON c.bucket = b.bucket
),
wt AS (
    SELECT b.type, b.ord, coalesce(c.n, 0)::int AS count
    FROM (VALUES ('remote',1),('hybrid',2),('onsite',3),('unspecified',4)) AS b(type, ord)
    LEFT JOIN (
        SELECT coalesce(location_type, 'unspecified') AS type, count(*) AS n
        FROM j GROUP BY 1
    ) c ON c.type = b.type
),
src AS (
    SELECT source, count(*)::int AS count
    FROM j GROUP BY source
),
tc AS (
    SELECT j.company_slug AS slug, co.name, count(*)::int AS count
    FROM j
    LEFT JOIN {{SCHEMA}}.company co ON co.slug = j.company_slug
    GROUP BY j.company_slug, co.name
    ORDER BY count(*) DESC, j.company_slug
    LIMIT 12
),
pbd AS (
    SELECT g.d::date AS day, coalesce(c.n, 0)::int AS count
    FROM generate_series(current_date - 20, current_date, interval '1 day') AS g(d)
    LEFT JOIN (
        SELECT first_seen_at::date AS day, count(*) AS n FROM j GROUP BY 1
    ) c ON c.day = g.d::date
)
SELECT json_build_object(
    'total', (SELECT total FROM tot),
    'companies_with_jobs', (SELECT companies_with_jobs FROM tot),
    'by_seniority', (
        SELECT coalesce(json_agg(
            json_build_object('bucket', bucket, 'count', count) ORDER BY ord
        ), '[]'::json) FROM sen
    ),
    'by_work_type', (
        SELECT coalesce(json_agg(
            json_build_object('type', type, 'count', count) ORDER BY ord
        ), '[]'::json) FROM wt
    ),
    'by_source', (
        SELECT coalesce(json_agg(
            json_build_object('source', source, 'count', count)
            ORDER BY count DESC, source
        ), '[]'::json) FROM src
    ),
    'top_companies', (
        SELECT coalesce(json_agg(
            json_build_object('slug', slug, 'name', name, 'count', count)
            ORDER BY count DESC, slug
        ), '[]'::json) FROM tc
    ),
    'posted_by_day', (
        SELECT coalesce(json_agg(
            json_build_object('day', to_char(day, 'YYYY-MM-DD'), 'count', count)
            ORDER BY day
        ), '[]'::json) FROM pbd
    ),
    'mid_share_pct', (
        SELECT coalesce(
            round(100.0 * (SELECT count FROM sen WHERE bucket = 'mid')
                  / nullif((SELECT total FROM tot), 0), 1),
            0
        )
    )
);
$fn$;

-- --- market snapshot RPC (daily analytics capture) -----------------------
-- One JSON object, single pass over job_posting (+ a join to company for
-- by_company_type / top_companies). Called by `jobs snapshot-market`
-- (src/pgpipe/cli.py) from the cleanup-stale-jobs workflow, BEFORE the
-- hard-delete step. Closed rows are kept for 7 days; this table is the
-- long-term record of a day's activity.
CREATE OR REPLACE FUNCTION {{SCHEMA}}.compute_market_snapshot()
RETURNS json
LANGUAGE sql
STABLE
AS $fn$
WITH open_jobs AS (
    SELECT id, seniority, location_type, country, company_slug
    FROM {{SCHEMA}}.job_posting
    WHERE status = 'open'
),
totals AS (
    SELECT
        (SELECT count(*) FROM {{SCHEMA}}.job_posting WHERE status = 'open') AS total_open,
        -- posted_at (real posting date), NOT first_seen_at (our discovery
        -- time) — at 1-day retention, first_seen_at is within 24h for
        -- virtually every row in the table by construction, which would make
        -- this metric almost always equal total_open and useless as a
        -- distinct "hiring velocity" signal.
        (SELECT count(*) FROM {{SCHEMA}}.job_posting
            WHERE posted_at > now() - interval '24 hours') AS new_postings,
        (SELECT count(*) FROM {{SCHEMA}}.job_posting
            WHERE status = 'closed'
              AND coalesce(closed_at, last_seen_at) > now() - interval '24 hours'
        ) AS closed_postings
),
sen AS (
    SELECT coalesce(seniority, 'unknown') AS bucket, count(*) AS n
    FROM open_jobs GROUP BY 1
),
wt AS (
    SELECT coalesce(location_type, 'unspecified') AS type, count(*) AS n
    FROM open_jobs GROUP BY 1
),
ctry AS (
    SELECT coalesce(country, 'other') AS country, count(*) AS n
    FROM open_jobs GROUP BY 1
),
ctype AS (
    SELECT coalesce(co.company_type, 'unknown') AS company_type, count(*) AS n
    FROM open_jobs oj
    LEFT JOIN {{SCHEMA}}.company co ON co.slug = oj.company_slug
    GROUP BY 1
),
top_co AS (
    SELECT coalesce(co.name, jp.company_slug) AS company, count(*) AS new_postings
    FROM {{SCHEMA}}.job_posting jp
    LEFT JOIN {{SCHEMA}}.company co ON co.slug = jp.company_slug
    WHERE jp.posted_at > now() - interval '24 hours'
    GROUP BY coalesce(co.name, jp.company_slug)
    ORDER BY count(*) DESC, company
    LIMIT 10
)
SELECT json_build_object(
    'total_open', (SELECT total_open FROM totals),
    'new_postings', (SELECT new_postings FROM totals),
    'closed_postings', (SELECT closed_postings FROM totals),
    'by_seniority', (SELECT coalesce(json_object_agg(bucket, n), '{}'::json) FROM sen),
    'by_work_type', (SELECT coalesce(json_object_agg(type, n), '{}'::json) FROM wt),
    'by_country', (SELECT coalesce(json_object_agg(country, n), '{}'::json) FROM ctry),
    'by_company_type', (SELECT coalesce(json_object_agg(company_type, n), '{}'::json) FROM ctype),
    'top_companies', (
        SELECT coalesce(json_agg(
            json_build_object('company', company, 'new_postings', new_postings)
        ), '[]'::json) FROM top_co
    )
);
$fn$;

-- --- market snapshot table (public schema — separate webapp workstream
-- reads this; the crawler owns creating + writing it) ---------------------
CREATE TABLE IF NOT EXISTS public.jobs_market_snapshot_daily (
    snapshot_date     date PRIMARY KEY,
    total_open        bigint NOT NULL,
    new_postings      bigint NOT NULL,
    closed_postings   bigint NOT NULL,
    by_seniority      jsonb NOT NULL DEFAULT '{}',
    by_work_type      jsonb NOT NULL DEFAULT '{}',
    by_country        jsonb NOT NULL DEFAULT '{}',
    by_company_type   jsonb NOT NULL DEFAULT '{}',
    top_companies     jsonb NOT NULL DEFAULT '[]',
    created_at        timestamptz NOT NULL DEFAULT now()
);
-- Read-only for the webapp. RLS is enabled (every other public.* table in
-- this project has it on) with a public SELECT policy since snapshot
-- aggregates carry no sensitive/per-user data; writes only ever happen from
-- `jobs snapshot-market` using the service_role connection, which bypasses RLS.
ALTER TABLE public.jobs_market_snapshot_daily ENABLE ROW LEVEL SECURITY;
DO $snapshot_policy$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'public' AND tablename = 'jobs_market_snapshot_daily'
          AND policyname = 'jobs_market_snapshot_daily_public_read'
    ) THEN
        CREATE POLICY jobs_market_snapshot_daily_public_read
            ON public.jobs_market_snapshot_daily FOR SELECT
            USING (true);
    END IF;
END
$snapshot_policy$;
GRANT SELECT ON public.jobs_market_snapshot_daily TO anon, authenticated, service_role;

-- =========================================================================
-- v2: stable ids, feed, dedupe, LinkedIn-ready columns, roles.
-- Everything below is idempotent (safe on every `jobs migrate`).
-- =========================================================================

-- --- companies: allow auto-insert of LinkedIn-discovered companies ----------
-- name used to be NOT NULL with no default; a BEFORE INSERT trigger derives a
-- display name from the slug so `INSERT INTO company (slug)` works.
ALTER TABLE {{SCHEMA}}.company ALTER COLUMN name SET DEFAULT '';
CREATE OR REPLACE FUNCTION {{SCHEMA}}.company_fill_defaults() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    IF NEW.name IS NULL OR btrim(NEW.name) = '' THEN
        NEW.name := initcap(replace(replace(NEW.slug, '-', ' '), '_', ' '));
    END IF;
    IF NEW.company_type IS NULL THEN NEW.company_type := 'unknown'; END IF;
    RETURN NEW;
END
$fn$;
DROP TRIGGER IF EXISTS company_fill_defaults_trg ON {{SCHEMA}}.company;
CREATE TRIGGER company_fill_defaults_trg BEFORE INSERT ON {{SCHEMA}}.company
    FOR EACH ROW EXECUTE FUNCTION {{SCHEMA}}.company_fill_defaults();

-- --- job_posting: new columns -------------------------------------------------
ALTER TABLE {{SCHEMA}}.job_posting
    ADD COLUMN IF NOT EXISTS linkedin_job_id  text,
    ADD COLUMN IF NOT EXISTS apply_url        text,
    ADD COLUMN IF NOT EXISTS alt_urls         jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS company_name     text,
    ADD COLUMN IF NOT EXISTS company_key      text,
    ADD COLUMN IF NOT EXISTS dedupe_key       text,
    ADD COLUMN IF NOT EXISTS reposted_at      timestamptz,
    ADD COLUMN IF NOT EXISTS repost_count     int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS duplicate_of     bigint,
    ADD COLUMN IF NOT EXISTS eff_at           timestamptz,
    ADD COLUMN IF NOT EXISTS closed_at        timestamptz,
    ADD COLUMN IF NOT EXISTS last_seen_run_id bigint;

DO $fk$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'job_posting_duplicate_of_fk'
                     AND conrelid = '{{SCHEMA}}.job_posting'::regclass) THEN
        ALTER TABLE {{SCHEMA}}.job_posting
            ADD CONSTRAINT job_posting_duplicate_of_fk
            FOREIGN KEY (duplicate_of) REFERENCES {{SCHEMA}}.job_posting(id) ON DELETE SET NULL;
    END IF;
END
$fk$;

-- country: add 'remote'
DO $ctry$
DECLARE def text;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO def FROM pg_constraint
     WHERE conname = 'job_posting_country_check'
       AND conrelid = '{{SCHEMA}}.job_posting'::regclass;
    IF def IS NULL OR def NOT LIKE '%remote%' THEN
        ALTER TABLE {{SCHEMA}}.job_posting DROP CONSTRAINT IF EXISTS job_posting_country_check;
        ALTER TABLE {{SCHEMA}}.job_posting ADD CONSTRAINT job_posting_country_check
            CHECK (country IS NULL OR country IN ('US', 'IN', 'remote', 'other'));
    END IF;
END
$ctry$;

-- --- key functions (mirrored in src/pgpipe/keys.py) ---------------------------
CREATE OR REPLACE FUNCTION {{SCHEMA}}.norm_company_key(p_name text) RETURNS text
LANGUAGE sql IMMUTABLE AS $fn$
    SELECT CASE WHEN p_name IS NULL THEN '' ELSE
        coalesce(
            nullif(regexp_replace(
                regexp_replace(lower(p_name),
                    '\m(inc|llc|ltd|pvt|private|limited|technologies|corp)\M', '', 'g'),
                '[^a-z0-9]', '', 'g'), ''),
            regexp_replace(lower(p_name), '[^a-z0-9]', '', 'g'))
    END
$fn$;

CREATE OR REPLACE FUNCTION {{SCHEMA}}.norm_title(p_title text) RETURNS text
LANGUAGE sql IMMUTABLE AS $fn$
    SELECT btrim(regexp_replace(lower(coalesce(p_title, '')), '[^a-z0-9]+', ' ', 'g'))
$fn$;

CREATE OR REPLACE FUNCTION {{SCHEMA}}.norm_city(p_location text) RETURNS text
LANGUAGE sql IMMUTABLE AS $fn$
    SELECT regexp_replace(lower(split_part(coalesce(p_location, ''), ',', 1)), '[^a-z0-9]', '', 'g')
$fn$;

-- BEFORE INSERT/UPDATE: fill company_name/company_key/dedupe_key/eff_at.
CREATE OR REPLACE FUNCTION {{SCHEMA}}.job_posting_derive() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    IF NEW.company_name IS NULL OR btrim(NEW.company_name) = '' THEN
        SELECT c.name INTO NEW.company_name FROM {{SCHEMA}}.company c WHERE c.slug = NEW.company_slug;
    END IF;
    NEW.company_key := coalesce(
        nullif({{SCHEMA}}.norm_company_key(coalesce(NEW.company_name, NEW.company_slug)), ''),
        NEW.company_slug);
    NEW.dedupe_key := md5(
        NEW.company_key || '|' || {{SCHEMA}}.norm_title(NEW.title) || '|' ||
        {{SCHEMA}}.norm_city(NEW.location));
    NEW.eff_at := coalesce(
        GREATEST(NEW.posted_at, coalesce(NEW.reposted_at, NEW.posted_at)),
        NEW.reposted_at, NEW.first_seen_at);
    RETURN NEW;
END
$fn$;
DROP TRIGGER IF EXISTS job_posting_derive_trg ON {{SCHEMA}}.job_posting;
CREATE TRIGGER job_posting_derive_trg BEFORE INSERT OR UPDATE ON {{SCHEMA}}.job_posting
    FOR EACH ROW EXECUTE FUNCTION {{SCHEMA}}.job_posting_derive();

-- --- backfill of the new columns (cheap no-op once done) ----------------------
UPDATE {{SCHEMA}}.job_posting SET posted_at = first_seen_at WHERE posted_at IS NULL;
UPDATE {{SCHEMA}}.job_posting SET closed_at = last_seen_at
    WHERE status = 'closed' AND closed_at IS NULL;
-- touching a row fires job_posting_derive_trg, which fills the derived keys
UPDATE {{SCHEMA}}.job_posting SET title = title
    WHERE dedupe_key IS NULL OR eff_at IS NULL OR company_key IS NULL;

-- --- unique indexes -----------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS job_posting_linkedin_uidx
    ON {{SCHEMA}}.job_posting (linkedin_job_id) WHERE linkedin_job_id IS NOT NULL;

-- Provider id. Scoped per company_slug because ids like Workday requisition
-- numbers are only unique within a tenant. Pre-existing duplicates are removed
-- (oldest id kept) the one time the index is created.
DO $uidx$
BEGIN
    IF to_regclass('{{SCHEMA}}.job_posting_src_ext_uidx') IS NULL THEN
        DELETE FROM {{SCHEMA}}.job_posting a
        USING {{SCHEMA}}.job_posting b
        WHERE a.external_id IS NOT NULL
          AND a.source = b.source AND a.company_slug = b.company_slug
          AND a.external_id = b.external_id AND a.id > b.id;
        CREATE UNIQUE INDEX job_posting_src_ext_uidx
            ON {{SCHEMA}}.job_posting (source, company_slug, external_id)
            WHERE external_id IS NOT NULL;
    END IF;
END
$uidx$;

CREATE INDEX IF NOT EXISTS job_posting_dedupe_idx
    ON {{SCHEMA}}.job_posting (dedupe_key) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS job_posting_eff_at_idx
    ON {{SCHEMA}}.job_posting (eff_at DESC, id DESC) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS job_posting_closed_idx
    ON {{SCHEMA}}.job_posting (closed_at) WHERE status = 'closed';
CREATE INDEX IF NOT EXISTS job_posting_dupof_idx
    ON {{SCHEMA}}.job_posting (duplicate_of) WHERE duplicate_of IS NOT NULL;
CREATE INDEX IF NOT EXISTS job_posting_board_run_idx
    ON {{SCHEMA}}.job_posting (board_slug, last_seen_run_id) WHERE status = 'open';

-- --- board health (BoardGoneError / persistent failures) ----------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.board_health (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    board_slug  text NOT NULL,
    run_id      bigint,
    event       text NOT NULL CHECK (event IN ('gone', 'failing', 'recovered')),
    detail      text,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS board_health_board_idx
    ON {{SCHEMA}}.board_health (board_slug, created_at DESC);
CREATE INDEX IF NOT EXISTS board_health_run_idx ON {{SCHEMA}}.board_health (run_id);

-- --- greedy company-cooldown ordering ------------------------------------------
-- p_codes: dense integer company codes, input ALREADY sorted newest-first.
-- Returns the 1-based input positions in placed order (first p_limit of them).
-- A company just placed cannot be placed again until p_gap other placements
-- happened; if only cooling companies remain, the one whose LAST placement is
-- earliest goes next. Mirror: src/pgpipe/feedref.py::cooldown_order.
CREATE OR REPLACE FUNCTION {{SCHEMA}}.cooldown_order(
    p_codes int[], p_gap int DEFAULT 9, p_limit int DEFAULT NULL)
RETURNS int[]
LANGUAGE plpgsql IMMUTABLE AS $fn$
DECLARE
    n int := coalesce(array_length(p_codes, 1), 0);
    lim int;
    used boolean[];
    res int[] := ARRAY[]::int[];
    win int[] := ARRAY[]::int[];
    first_unused int := 1;
    i int;
    k int;
    pick int;
    wl int;
BEGIN
    IF n = 0 THEN RETURN res; END IF;
    lim := least(n, coalesce(p_limit, n));
    used := array_fill(false, ARRAY[n]);
    WHILE coalesce(array_length(res, 1), 0) < lim LOOP
        WHILE first_unused <= n AND used[first_unused] LOOP
            first_unused := first_unused + 1;
        END LOOP;
        pick := 0;
        i := first_unused;
        WHILE i <= n LOOP
            IF NOT used[i] AND NOT (p_codes[i] = ANY (win)) THEN
                pick := i; EXIT;
            END IF;
            i := i + 1;
        END LOOP;
        IF pick = 0 THEN
            wl := coalesce(array_length(win, 1), 0);
            FOR k IN 1..wl LOOP
                IF k < wl AND win[k] = ANY (win[k + 1:wl]) THEN CONTINUE; END IF;
                i := first_unused;
                WHILE i <= n LOOP
                    IF NOT used[i] AND p_codes[i] = win[k] THEN pick := i; EXIT; END IF;
                    i := i + 1;
                END LOOP;
                EXIT WHEN pick <> 0;
            END LOOP;
        END IF;
        used[pick] := true;
        res := res || pick;
        win := win || p_codes[pick];
        IF array_length(win, 1) > p_gap THEN
            win := win[2:array_length(win, 1)];
        END IF;
    END LOOP;
    RETURN res;
END
$fn$;

-- --- the feed table --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.job_feed (
    feed_position  bigint NOT NULL,
    id             bigint NOT NULL,
    company_slug   text NOT NULL,
    company_key    text,
    company_name   text,
    company_type   text,
    title          text,
    url            text NOT NULL,
    apply_url      text,
    alt_urls       jsonb NOT NULL DEFAULT '[]'::jsonb,
    location       text,
    location_type  text,
    country        text,
    seniority      text,
    source         text,
    posted_at      timestamptz,
    reposted_at    timestamptz,
    repost_count   int NOT NULL DEFAULT 0,
    eff_at         timestamptz,
    first_seen_at  timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS job_feed_position_uidx ON {{SCHEMA}}.job_feed (feed_position);
CREATE INDEX IF NOT EXISTS job_feed_id_idx        ON {{SCHEMA}}.job_feed (id);
CREATE INDEX IF NOT EXISTS job_feed_country_idx   ON {{SCHEMA}}.job_feed (country);
CREATE INDEX IF NOT EXISTS job_feed_seniority_idx ON {{SCHEMA}}.job_feed (seniority);
CREATE INDEX IF NOT EXISTS job_feed_source_idx    ON {{SCHEMA}}.job_feed (source);
CREATE INDEX IF NOT EXISTS job_feed_eff_at_idx    ON {{SCHEMA}}.job_feed (eff_at DESC, id DESC);
-- company hub page: job_feed WHERE company_slug = $1 ORDER BY eff_at DESC
CREATE INDEX IF NOT EXISTS job_feed_company_eff_idx
    ON {{SCHEMA}}.job_feed (company_slug, eff_at DESC);

DO $feed_idx$
BEGIN
    BEGIN
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'pg_trgm unavailable (%): job_feed trigram indexes skipped', SQLERRM;
        RETURN;
    END;
    BEGIN
        EXECUTE $q$ CREATE INDEX IF NOT EXISTS job_feed_title_trgm_idx
            ON {{SCHEMA}}.job_feed USING gin (title gin_trgm_ops) $q$;
        EXECUTE $q$ CREATE INDEX IF NOT EXISTS job_feed_company_trgm_idx
            ON {{SCHEMA}}.job_feed USING gin (company_name gin_trgm_ops) $q$;
        EXECUTE $q$ CREATE INDEX IF NOT EXISTS job_feed_location_trgm_idx
            ON {{SCHEMA}}.job_feed USING gin (location gin_trgm_ops) $q$;
    EXCEPTION WHEN undefined_object OR undefined_function THEN
        RAISE NOTICE 'pg_trgm opclass not on search_path — create job_feed_*_trgm_idx manually';
    END;
END
$feed_idx$;

-- --- build_feed(): cross-source dedupe + rebuild job_feed ----------------------
-- SECURITY DEFINER so both service_role and crawler_writer can call it; the
-- search_path is pinned. Returns the number of rows placed in job_feed.
CREATE OR REPLACE FUNCTION {{SCHEMA}}.build_feed() RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = {{SCHEMA}}, public AS $fn$
DECLARE
    ids   bigint[];
    codes int[];
    ord   int[];
    n     int;
BEGIN
    -- (1a) un-mark duplicates whose ATS twin is gone
    UPDATE job_posting li SET duplicate_of = NULL
     WHERE li.duplicate_of IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM job_posting a
                        WHERE a.id = li.duplicate_of AND a.status = 'open');

    -- (1b) LinkedIn rows duplicating an open non-LinkedIn row
    UPDATE job_posting li SET duplicate_of = ats.id
      FROM (SELECT DISTINCT ON (dedupe_key) id, dedupe_key
              FROM job_posting
             WHERE status = 'open' AND source <> 'linkedin' AND dedupe_key IS NOT NULL
             ORDER BY dedupe_key, eff_at DESC NULLS LAST, id) ats
     WHERE li.source = 'linkedin' AND li.status = 'open'
       AND li.dedupe_key = ats.dedupe_key
       AND li.duplicate_of IS DISTINCT FROM ats.id;

    -- (1c) ATS rows carry the LinkedIn urls in alt_urls (non-LinkedIn entries kept)
    UPDATE job_posting ats SET alt_urls = x.new_alt
      FROM (
        SELECT a.id,
               coalesce((SELECT jsonb_agg(e) FROM jsonb_array_elements(a.alt_urls) e
                          WHERE e->>'source' IS DISTINCT FROM 'linkedin'), '[]'::jsonb)
               || coalesce((SELECT jsonb_agg(jsonb_build_object('url', li.url, 'source', 'linkedin')
                                             ORDER BY li.id)
                              FROM job_posting li
                             WHERE li.duplicate_of = a.id AND li.status = 'open'), '[]'::jsonb)
               AS new_alt
          FROM job_posting a
         WHERE a.source <> 'linkedin'
           AND (EXISTS (SELECT 1 FROM job_posting li
                         WHERE li.duplicate_of = a.id AND li.status = 'open')
                OR a.alt_urls @> '[{"source":"linkedin"}]'::jsonb)
      ) x
     WHERE ats.id = x.id AND ats.alt_urls IS DISTINCT FROM x.new_alt;

    -- (2) rebuild job_feed from the last 24h of open, non-duplicate rows
    SELECT array_agg(s.id ORDER BY s.eff_at DESC, s.id DESC),
           array_agg(s.code ORDER BY s.eff_at DESC, s.id DESC)
      INTO ids, codes
      FROM (SELECT jp.id, jp.eff_at,
                   dense_rank() OVER (ORDER BY coalesce(jp.company_key, jp.company_slug)) AS code
              FROM job_posting jp
             WHERE jp.status = 'open' AND jp.duplicate_of IS NULL
               AND jp.eff_at >= now() - interval '24 hours') s;

    DELETE FROM job_feed;
    IF ids IS NULL THEN RETURN 0; END IF;

    ord := cooldown_order(codes, 9, NULL);

    INSERT INTO job_feed (feed_position, id, company_slug, company_key, company_name,
                          company_type, title, url, apply_url, alt_urls, location,
                          location_type, country, seniority, source, posted_at,
                          reposted_at, repost_count, eff_at, first_seen_at)
    SELECT u.rn, jp.id, jp.company_slug, jp.company_key,
           coalesce(jp.company_name, c.name, jp.company_slug),
           coalesce(c.company_type, 'unknown'),
           jp.title, jp.url, jp.apply_url, jp.alt_urls, jp.location,
           jp.location_type, jp.country, jp.seniority, jp.source, jp.posted_at,
           jp.reposted_at, jp.repost_count, jp.eff_at, jp.first_seen_at
      FROM unnest(ord) WITH ORDINALITY AS u(ix, rn)
      JOIN job_posting jp ON jp.id = ids[u.ix]
      LEFT JOIN company c ON c.slug = jp.company_slug;

    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END
$fn$;

-- --- feed_page(): filtered + re-interleaved page of job_feed ------------------
-- Positions returned are positions within the FILTERED interleave
-- (p_offset + 1 ..). NULL param = no filter. p_location may hold several
-- locations joined with '|' (OR-matched, ILIKE). total_count = filtered rows.
CREATE OR REPLACE FUNCTION {{SCHEMA}}.feed_page(
    p_q text, p_company text, p_location text, p_location_type text,
    p_seniority text[], p_source text[], p_country text[], p_company_type text[],
    p_hours int, p_offset int, p_limit int)
RETURNS TABLE (
    feed_position bigint, id bigint, company_slug text, company_key text,
    company_name text, company_type text, title text, url text, apply_url text,
    alt_urls jsonb, location text, location_type text, country text, seniority text,
    source text, posted_at timestamptz, reposted_at timestamptz, repost_count int,
    eff_at timestamptz, first_seen_at timestamptz, total_count bigint)
LANGUAGE plpgsql STABLE AS $fn$
#variable_conflict use_column
DECLARE
    ids   bigint[];
    codes int[];
    ord   int[];
    tot   bigint;
    off_  int := greatest(coalesce(p_offset, 0), 0);
    lim_  int := least(greatest(coalesce(p_limit, 50), 0), 500);
    q_pat text := CASE WHEN coalesce(p_q, '') = '' THEN NULL ELSE
        '%' || replace(replace(replace(p_q, '\', '\\'), '%', '\%'), '_', '\_') || '%' END;
    c_pat text := CASE WHEN coalesce(p_company, '') = '' THEN NULL ELSE
        '%' || replace(replace(replace(p_company, '\', '\\'), '%', '\%'), '_', '\_') || '%' END;
    l_pats text[] := CASE WHEN coalesce(p_location, '') = '' THEN NULL ELSE
        ARRAY(SELECT '%' || replace(replace(replace(btrim(x), '\', '\\'), '%', '\%'), '_', '\_') || '%'
                FROM unnest(string_to_array(p_location, '|')) x
               WHERE btrim(x) <> '') END;
BEGIN
    IF l_pats IS NOT NULL AND array_length(l_pats, 1) IS NULL THEN l_pats := NULL; END IF;

    SELECT array_agg(s.id ORDER BY s.eff_at DESC, s.id DESC),
           array_agg(s.code ORDER BY s.eff_at DESC, s.id DESC),
           count(*)
      INTO ids, codes, tot
      FROM (SELECT f.id, f.eff_at,
                   dense_rank() OVER (ORDER BY coalesce(f.company_key, f.company_slug)) AS code
              FROM {{SCHEMA}}.job_feed f
             WHERE (q_pat IS NULL OR f.title ILIKE q_pat OR f.company_name ILIKE q_pat)
               AND (c_pat IS NULL OR f.company_name ILIKE c_pat
                    OR f.company_slug = lower(p_company))
               AND (l_pats IS NULL OR f.location ILIKE ANY (l_pats))
               AND (coalesce(p_location_type, '') = '' OR f.location_type = p_location_type)
               AND (p_seniority IS NULL OR f.seniority = ANY (p_seniority))
               AND (p_source IS NULL OR f.source = ANY (p_source))
               AND (p_country IS NULL OR f.country = ANY (p_country))
               AND (p_company_type IS NULL OR f.company_type = ANY (p_company_type))
               AND (p_hours IS NULL OR f.eff_at >= now() - make_interval(hours => p_hours))
           ) s;

    IF ids IS NULL THEN RETURN; END IF;
    ord := {{SCHEMA}}.cooldown_order(codes, 9, off_ + lim_);

    RETURN QUERY
    SELECT (off_ + u.rn)::bigint, f.id, f.company_slug, f.company_key, f.company_name,
           f.company_type, f.title, f.url, f.apply_url, f.alt_urls, f.location,
           f.location_type, f.country, f.seniority, f.source, f.posted_at,
           f.reposted_at, f.repost_count, f.eff_at, f.first_seen_at, tot
      FROM unnest(ord[off_ + 1 : off_ + lim_]) WITH ORDINALITY AS u(ix, rn)
      JOIN {{SCHEMA}}.job_feed f ON f.id = ids[u.ix]
     ORDER BY u.rn;
END
$fn$;

-- --- roles + grants for the new objects ---------------------------------------
-- crawler_writer: least-privilege role for the scheduled workflows / Oracle VM.
-- NOLOGIN here; create a login user that inherits it (see README):
--   CREATE ROLE crawler_ci LOGIN PASSWORD '...' IN ROLE crawler_writer;
DO $role$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'crawler_writer') THEN
        CREATE ROLE crawler_writer NOLOGIN;
    END IF;
END
$role$;

GRANT USAGE ON SCHEMA {{SCHEMA}} TO crawler_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{SCHEMA}} TO crawler_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {{SCHEMA}} TO crawler_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA {{SCHEMA}}
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO crawler_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA {{SCHEMA}}
    GRANT USAGE, SELECT ON SEQUENCES TO crawler_writer;
-- the daily market snapshot is the one write the crawler does outside its schema
GRANT SELECT, INSERT, UPDATE ON public.jobs_market_snapshot_daily TO crawler_writer;

-- function execute rights: nothing public; only the intended roles.
REVOKE ALL ON FUNCTION {{SCHEMA}}.build_feed() FROM PUBLIC;
REVOKE ALL ON FUNCTION {{SCHEMA}}.cooldown_order(int[], int, int) FROM PUBLIC;
REVOKE ALL ON FUNCTION {{SCHEMA}}.feed_page(text, text, text, text, text[], text[], text[], text[], int, int, int) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION {{SCHEMA}}.build_feed() TO crawler_writer;
GRANT EXECUTE ON FUNCTION {{SCHEMA}}.cooldown_order(int[], int, int) TO crawler_writer;
DO $anon$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
        REVOKE ALL ON FUNCTION {{SCHEMA}}.build_feed() FROM anon;
        REVOKE ALL ON FUNCTION {{SCHEMA}}.feed_page(text, text, text, text, text[], text[], text[], text[], int, int, int) FROM anon;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        REVOKE ALL ON FUNCTION {{SCHEMA}}.build_feed() FROM authenticated;
        REVOKE ALL ON FUNCTION {{SCHEMA}}.feed_page(text, text, text, text, text[], text[], text[], text[], int, int, int) FROM authenticated;
    END IF;
END
$anon$;

-- --- API access -----------------------------------------------------
-- The webapp reads this schema ONLY through its Express server, which uses
-- the Supabase service_role key. Grant that role read-only access. (The
-- browser never queries these tables directly, so anon/authenticated get
-- nothing and no RLS is needed here.)
GRANT USAGE ON SCHEMA {{SCHEMA}} TO service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA {{SCHEMA}} TO service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {{SCHEMA}} TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA {{SCHEMA}} GRANT SELECT ON TABLES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA {{SCHEMA}} GRANT EXECUTE ON FUNCTIONS TO service_role;
