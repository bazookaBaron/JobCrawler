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
