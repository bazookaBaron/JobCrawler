-- ===========================================================================
-- job-crawler  —  Postgres full-text search over the compact jobs table
-- ===========================================================================
-- Runs on the same Supabase database the webapp uses. Everything lives in the
-- dedicated "crawler" schema so it never collides with webapp tables.
--
-- The table + the generated search_tsv column + GIN index are created by
-- `jobs migrate` (src/pgpipe/schema.sql). This file only adds query functions.
--
--   crawler.job_posting(
--     id bigint, company_slug text, board_slug text, source text,
--     external_id text, url text, title text, titles jsonb,
--     location text, locations jsonb, location_type text,
--     employment_type text, department text, date_posted text,
--     first_seen_at timestamptz, last_seen_at timestamptz, status text,
--     search_tsv tsvector  -- GENERATED from title+company+department+location
--   )
--
-- Apply:  psql "$LOCAL_DATABASE_URL" -f search/jobs_search.sql
--   (or paste into the Supabase SQL editor)
-- ===========================================================================

SET search_path TO crawler, public;

-- ---------------------------------------------------------------------------
-- search_jobs()  —  FTS over title + company + department + location, with
-- optional company / location-type / status filters and LIMIT/OFFSET paging.
--
--   p_query      : natural language ('' or NULL => browse newest, no text rank)
--   p_company    : text[] of company slugs      (NULL/'{}' = any)
--   p_location   : text[] of location_type      ('remote'|'hybrid'|'onsite')
--   p_status     : 'open' (default) | 'closed' | 'any'
--   p_limit / p_offset : pagination (defaults 20 / 0)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION crawler.search_jobs(
    p_query    text    DEFAULT NULL,
    p_company  text[]  DEFAULT NULL,
    p_location text[]  DEFAULT NULL,
    p_status   text    DEFAULT 'open',
    p_limit    int     DEFAULT 20,
    p_offset   int     DEFAULT 0
)
RETURNS TABLE (
    id              bigint,
    company_slug    text,
    title           text,
    titles          jsonb,
    location        text,
    location_type   text,
    employment_type text,
    department      text,
    url             text,
    source          text,
    external_id     text,
    date_posted     text,
    first_seen_at   timestamptz,
    last_seen_at    timestamptz,
    status          text,
    rank            real,
    total_count     bigint
)
LANGUAGE sql STABLE
AS $$
    WITH q AS (
        SELECT CASE
                 WHEN p_query IS NULL OR btrim(p_query) = '' THEN NULL
                 ELSE websearch_to_tsquery('english', p_query)
               END AS ts
    ),
    base AS (
        SELECT jp.id, jp.company_slug, jp.title, jp.titles, jp.location,
               jp.location_type, jp.employment_type, jp.department, jp.url,
               jp.source, jp.external_id, jp.date_posted,
               jp.first_seen_at, jp.last_seen_at, jp.status,
               CASE WHEN (SELECT ts FROM q) IS NULL THEN 0::real
                    ELSE ts_rank_cd(jp.search_tsv, (SELECT ts FROM q)) END AS rank
        FROM crawler.job_posting jp
        WHERE (p_status = 'any' OR jp.status = p_status)
          AND (p_company IS NULL OR array_length(p_company, 1) IS NULL
               OR jp.company_slug = ANY (p_company))
          AND (p_location IS NULL OR array_length(p_location, 1) IS NULL
               OR jp.location_type = ANY (p_location))
          AND ((SELECT ts FROM q) IS NULL
               OR jp.search_tsv @@ (SELECT ts FROM q))
    )
    SELECT base.*, count(*) OVER () AS total_count
    FROM base
    ORDER BY rank DESC, first_seen_at DESC, id DESC
    LIMIT  greatest(p_limit, 0)
    OFFSET greatest(p_offset, 0);
$$;


-- ---------------------------------------------------------------------------
-- list_jobs()  —  no text query; filter + newest-first, keyset pagination.
-- Pass the previous page's last row as (p_before_seen, p_before_id); NULLs
-- for page 1.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION crawler.list_jobs(
    p_company     text[]  DEFAULT NULL,
    p_location    text[]  DEFAULT NULL,
    p_status      text    DEFAULT 'open',
    p_before_seen timestamptz DEFAULT NULL,
    p_before_id   bigint  DEFAULT NULL,
    p_limit       int     DEFAULT 20
)
RETURNS TABLE (
    id              bigint,
    company_slug    text,
    title           text,
    location        text,
    location_type   text,
    employment_type text,
    department      text,
    url             text,
    source          text,
    first_seen_at   timestamptz,
    last_seen_at    timestamptz,
    status          text
)
LANGUAGE sql STABLE
AS $$
    SELECT jp.id, jp.company_slug, jp.title, jp.location, jp.location_type,
           jp.employment_type, jp.department, jp.url, jp.source,
           jp.first_seen_at, jp.last_seen_at, jp.status
    FROM crawler.job_posting jp
    WHERE (p_status = 'any' OR jp.status = p_status)
      AND (p_company  IS NULL OR array_length(p_company, 1)  IS NULL
           OR jp.company_slug = ANY (p_company))
      AND (p_location IS NULL OR array_length(p_location, 1) IS NULL
           OR jp.location_type = ANY (p_location))
      AND (p_before_seen IS NULL
           OR (jp.first_seen_at, jp.id) < (p_before_seen, p_before_id))
    ORDER BY jp.first_seen_at DESC, jp.id DESC
    LIMIT greatest(p_limit, 0);
$$;


-- ---------------------------------------------------------------------------
-- job_counts_by_company()  —  small helper for a UI sidebar / facet.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION crawler.job_counts_by_company(p_status text DEFAULT 'open')
RETURNS TABLE (company_slug text, company_name text, n bigint)
LANGUAGE sql STABLE
AS $$
    SELECT jp.company_slug, c.name, count(*)
    FROM crawler.job_posting jp
    JOIN crawler.company c ON c.slug = jp.company_slug
    WHERE (p_status = 'any' OR jp.status = p_status)
    GROUP BY jp.company_slug, c.name
    ORDER BY count(*) DESC;
$$;


-- ---------------------------------------------------------------------------
-- Quick checks
-- ---------------------------------------------------------------------------
-- SELECT id, company_slug, title, rank, total_count
-- FROM crawler.search_jobs('distributed systems rust', NULL, NULL, 'open', 10, 0);
--
-- SELECT id, company_slug, title
-- FROM crawler.search_jobs('payments infra', ARRAY['stripe','adyen'], ARRAY['remote']);
--
-- SELECT * FROM crawler.list_jobs(ARRAY['anthropic'], NULL, 'open', NULL, NULL, 25);
-- ===========================================================================
