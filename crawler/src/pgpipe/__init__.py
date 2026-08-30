"""pgpipe — a single-process, Postgres-only job crawl pipeline.

Replaces jobseek's Redis queue + distributed worker pool (src/redis_queue.py,
src/redis_capacity.py, src/workers/pipeline.py) for a $0 GitHub Actions cron:

  jobs migrate     apply the compact schema (dedicated Postgres schema)
  jobs sync        data/*.csv  ->  <schema>.company / <schema>.job_board
  jobs run         reclaim stale claims -> enqueue due boards -> crawl -> retain
  jobs close-stale mark postings not seen in N days as closed
  jobs prune       keep only the newest N postings per company
  jobs hard-delete DELETE postings not seen in N days (daily cleanup workflow)
  jobs stats       print row counts

Only jobseek's monitor modules (src/core/monitors/*) and src/shared/http are
reused — everything else in this package is new. See UPSTREAM_DRIFT.md.
"""
