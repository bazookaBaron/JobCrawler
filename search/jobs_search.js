// ===========================================================================
// job-crawler  —  search helpers (plain node-postgres)
// ===========================================================================
//   npm i pg
//
// One-time setup: `jobs migrate` creates crawler.job_posting (incl. the
// seniority column + generated search_tsv + GIN index) and the
// crawler.jobs_analytics() RPC. Then apply search/jobs_search.sql once to add
// the crawler.search_jobs() / list_jobs() / job_counts_by_company() functions.
//
// Every function takes `db` = a pg.Pool or pg.Client (anything with
// .query(text, params)). It never opens its own connection.
// ===========================================================================

'use strict';

/**
 * Full-text search over title + company + department + location, with optional
 * company / location-type / status filters and LIMIT/OFFSET pagination.
 *
 * @param {{query: Function}} db
 * @param {object}  opts
 * @param {string} [opts.q]              natural-language query
 * @param {string[]} [opts.companies]    company slugs
 * @param {string[]} [opts.locationTypes] "remote" | "hybrid" | "onsite"
 * @param {string[]} [opts.seniorities]  intern|junior|mid|senior|staff|principal|unknown
 * @param {'open'|'closed'|'any'} [opts.status='open']
 * @param {number} [opts.limit=20]
 * @param {number} [opts.offset=0]
 * @returns {Promise<{rows: JobRow[], total: number, limit: number,
 *                    offset: number, nextOffset: number|null}>}
 */
async function searchJobs(db, opts = {}) {
  const {
    q = null, companies = null, locationTypes = null, seniorities = null,
    status = 'open', limit = 20, offset = 0,
  } = opts;

  const safeLimit = clampInt(limit, 1, 100, 20);
  const safeOffset = clampInt(offset, 0, 1_000_000, 0);

  const { rows } = await db.query(
    'SELECT * FROM crawler.search_jobs($1, $2, $3, $4, $5, $6, $7)',
    [emptyToNull(q), nullIfEmpty(companies), nullIfEmpty(locationTypes),
     nullIfEmpty(seniorities), status, safeLimit, safeOffset],
  );

  const total = rows.length ? Number(rows[0].total_count) : 0;
  for (const r of rows) delete r.total_count;
  const nextOffset = safeOffset + rows.length < total ? safeOffset + safeLimit : null;
  return { rows, total, limit: safeLimit, offset: safeOffset, nextOffset };
}

/**
 * No text query — filter + newest-first, keyset pagination.
 * @param {{query: Function}} db
 * @param {object} opts
 * @param {string[]} [opts.companies]
 * @param {string[]} [opts.locationTypes]
 * @param {string[]} [opts.seniorities]
 * @param {'open'|'closed'|'any'} [opts.status='open']
 * @param {{first_seen_at: string, id: number}} [opts.after]
 * @param {number} [opts.limit=20]
 * @returns {Promise<{rows: any[], after: {first_seen_at: string, id: number}|null}>}
 */
async function listJobs(db, opts = {}) {
  const {
    companies = null, locationTypes = null, seniorities = null, status = 'open',
    after = null, limit = 20,
  } = opts;
  const safeLimit = clampInt(limit, 1, 100, 20);

  const { rows } = await db.query(
    'SELECT * FROM crawler.list_jobs($1, $2, $3, $4, $5, $6, $7)',
    [nullIfEmpty(companies), nullIfEmpty(locationTypes), nullIfEmpty(seniorities), status,
     after ? after.first_seen_at : null, after ? after.id : null, safeLimit],
  );

  const last = rows.length === safeLimit ? rows[rows.length - 1] : null;
  return { rows, after: last ? { first_seen_at: last.first_seen_at, id: last.id } : null };
}

/** { company_slug, company_name, n } rows, busiest first. */
async function jobCountsByCompany(db, status = 'open') {
  const { rows } = await db.query(
    'SELECT * FROM crawler.job_counts_by_company($1)', [status],
  );
  return rows;
}

/**
 * The webapp analytics-tab payload (one JSON object). Wraps
 * crawler.jobs_analytics(). See src/pgpipe/schema.sql for the exact shape:
 * { total, companies_with_jobs, by_seniority[], by_work_type[], by_source[],
 *   top_companies[], posted_by_day[], mid_share_pct }
 */
async function jobsAnalytics(db) {
  const { rows } = await db.query('SELECT crawler.jobs_analytics() AS a');
  return rows[0].a;
}

// --- helpers -------------------------------------------------------------
function emptyToNull(s) {
  return s == null || String(s).trim() === '' ? null : String(s);
}
function nullIfEmpty(a) {
  return Array.isArray(a) && a.length ? a : null;
}
function clampInt(v, lo, hi, dflt) {
  const n = Number.parseInt(v, 10);
  return Number.isNaN(n) ? dflt : Math.min(hi, Math.max(lo, n));
}

module.exports = { searchJobs, listJobs, jobCountsByCompany, jobsAnalytics };

// ===========================================================================
// Example — node-postgres
// ===========================================================================
// const { Pool } = require('pg');
// const { searchJobs } = require('./search/jobs_search');
// const pool = new Pool({ connectionString: process.env.LOCAL_DATABASE_URL });
//
// const page1 = await searchJobs(pool, {
//   q: 'low latency c++ trading',
//   companies: ['jane-street', 'optiver', 'imc', 'hudson-river-trading'],
//   limit: 20,
// });
// console.log(page1.total, page1.rows.map(r => `${r.company_slug}  ${r.title}`));
//
// ===========================================================================
// Example — supabase-js (RPC; note the "crawler" schema)
// ===========================================================================
// const supabase = createClient(URL, SERVICE_ROLE_KEY, { db: { schema: 'crawler' } });
// const { data } = await supabase.rpc('search_jobs', {
//   p_query: 'machine learning', p_company: ['anthropic', 'openai'],
//   p_location: null, p_seniority: ['senior', 'staff'],
//   p_status: 'open', p_limit: 20, p_offset: 0,
// });
// // data[0].total_count = pre-pagination total.
//
// // analytics tab:
// const { data: a } = await supabase.rpc('jobs_analytics');   // one JSON object
// ===========================================================================
