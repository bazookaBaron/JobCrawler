"""Market report — a separate, report-only pipeline (no job rows are stored).

    crawl (sharded)  ->  per-shard parquet of derived job attributes (runner disk)
    build            ->  merge shards, compute the daily report + mergeable
                         rollup, overwrite report_daily.report (1 row) and fold
                         the day into report_weekly.report (1 row per ISO week)

Nothing here reads or writes the `crawler` schema used by the jobs board.
"""
