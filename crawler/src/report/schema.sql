-- Market report storage — two NEW schemas, independent of `crawler` / `public`.
-- Idempotent: applied by `report build` on every run.
--
--   report_daily.report   exactly ONE row (id = 1), overwritten every day
--   report_weekly.report  ONE row per ISO week, folded from each daily run
--
-- Report-only data: no job rows, no URLs, no posting text. RLS is on with no
-- policies, so anon/authenticated (the webapp's browser keys) can read
-- nothing; only the service_role connection used by the workflow can.

CREATE SCHEMA IF NOT EXISTS report_daily;
CREATE SCHEMA IF NOT EXISTS report_weekly;

-- --- daily: single row ------------------------------------------------------
CREATE TABLE IF NOT EXISTS report_daily.report (
    id                     smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    report_date            date NOT NULL,
    iso_week               text NOT NULL,
    generated_at           timestamptz NOT NULL DEFAULT now(),
    status                 text NOT NULL CHECK (status IN ('ok', 'partial')),
    code_version           text,

    -- headline numbers (also inside `kpi`, here as columns for easy SQL)
    total_jobs             integer NOT NULL,
    unique_roles           integer NOT NULL,
    companies_hiring       integer NOT NULL,
    countries              integer NOT NULL,
    new_jobs               integer,            -- NULL on the very first (baseline) run
    removed_jobs           integer,
    remote_pct             numeric(5,2),
    hybrid_pct             numeric(5,2),
    onsite_pct             numeric(5,2),
    salary_disclosure_pct  numeric(5,2),
    salary_avg_usd         numeric(12,0),
    salary_median_usd      numeric(12,0),
    salary_p10_usd         numeric(12,0),
    salary_p25_usd         numeric(12,0),
    salary_p75_usd         numeric(12,0),
    salary_p90_usd         numeric(12,0),
    yoe_median             numeric(4,1),
    visa_offered_pct       numeric(5,2),

    -- report sections (chart-ready JSON; see src/report/sections.py)
    kpi                    jsonb NOT NULL,
    kpi_deltas             jsonb NOT NULL,     -- vs previous day, vs previous week
    coverage               jsonb NOT NULL,
    fx_rates               jsonb NOT NULL,
    profiles               jsonb NOT NULL,
    salary                 jsonb NOT NULL,
    experience             jsonb NOT NULL,
    geography              jsonb NOT NULL,
    skills                 jsonb NOT NULL,
    companies              jsonb NOT NULL,
    attributes             jsonb NOT NULL,
    data_quality           jsonb NOT NULL,

    -- machinery for the weekly fold and day-over-day comparisons. Not report
    -- content: select the columns above when reading the report.
    rollup                 jsonb NOT NULL,     -- additive building blocks for the week
    baseline               jsonb,              -- previous day's compact figures
    job_state              bytea,              -- zlib(hashed job keys) as of report_date
    prev_job_state         bytea               -- same, as of the previous report date
);

-- --- weekly: one row per ISO week ---------------------------------------------
CREATE TABLE IF NOT EXISTS report_weekly.report (
    iso_week               text PRIMARY KEY,   -- e.g. 2026-W41
    week_start             date NOT NULL,      -- Monday
    week_end               date NOT NULL,      -- Sunday
    status                 text NOT NULL CHECK (status IN ('in_progress', 'final')),
    days_covered           smallint NOT NULL,
    report_dates           date[] NOT NULL,
    first_run_at           timestamptz NOT NULL DEFAULT now(),
    last_run_at            timestamptz NOT NULL DEFAULT now(),
    avg_coverage_pct       numeric(5,2),
    code_version           text,

    -- headline numbers: *_avg = average per covered day, *_sum = week total
    total_jobs_avg         numeric(12,1) NOT NULL,
    unique_roles_avg       numeric(12,1) NOT NULL,
    companies_hiring_avg   numeric(12,1) NOT NULL,
    new_jobs_sum           integer,
    removed_jobs_sum       integer,
    remote_pct             numeric(5,2),
    hybrid_pct             numeric(5,2),
    onsite_pct             numeric(5,2),
    salary_disclosure_pct  numeric(5,2),
    salary_avg_usd         numeric(12,0),
    salary_median_usd      numeric(12,0),
    salary_p10_usd         numeric(12,0),
    salary_p25_usd         numeric(12,0),
    salary_p75_usd         numeric(12,0),
    salary_p90_usd         numeric(12,0),
    new_listing_salary_median_usd numeric(12,0),
    yoe_median             numeric(4,1),
    visa_offered_pct       numeric(5,2),

    daily_series           jsonb NOT NULL,     -- one headline point per covered day
    kpi                    jsonb NOT NULL,
    kpi_deltas             jsonb NOT NULL,     -- vs previous week, 4 and 12 weeks ago
    coverage               jsonb NOT NULL,
    profiles               jsonb NOT NULL,
    salary                 jsonb NOT NULL,
    experience             jsonb NOT NULL,
    geography              jsonb NOT NULL,
    skills                 jsonb NOT NULL,
    companies              jsonb NOT NULL,
    attributes             jsonb NOT NULL,
    data_quality           jsonb NOT NULL,

    -- running sum of the daily rollups while in_progress; NULL once final
    rollup                 jsonb
);
CREATE INDEX IF NOT EXISTS report_week_start_idx ON report_weekly.report (week_start DESC);

-- --- access ------------------------------------------------------------------
ALTER TABLE report_daily.report ENABLE ROW LEVEL SECURITY;
ALTER TABLE report_weekly.report ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON ALL TABLES IN SCHEMA report_daily FROM anon, authenticated;
REVOKE ALL ON ALL TABLES IN SCHEMA report_weekly FROM anon, authenticated;
GRANT USAGE ON SCHEMA report_daily, report_weekly TO service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA report_daily TO service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA report_weekly TO service_role;
