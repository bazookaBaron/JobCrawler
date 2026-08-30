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
CREATE INDEX IF NOT EXISTS job_posting_first_seen_idx
    ON {{SCHEMA}}.job_posting(company_slug, first_seen_at DESC);
CREATE INDEX IF NOT EXISTS job_posting_ext_idx
    ON {{SCHEMA}}.job_posting(company_slug, external_id) WHERE external_id IS NOT NULL;

-- seniority bucket derived from the title at ingest (src/pgpipe/seniority.py):
-- intern | junior | mid | senior | staff | principal | unknown
ALTER TABLE {{SCHEMA}}.job_posting
    ADD COLUMN IF NOT EXISTS seniority text NOT NULL DEFAULT 'unknown';
CREATE INDEX IF NOT EXISTS job_posting_seniority_idx ON {{SCHEMA}}.job_posting(seniority);

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
