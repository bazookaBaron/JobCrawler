"""DB size guard. Supabase free tier is 500 MB; once pg_database_size() passes
MAX_DB_MB the pipeline stops INSERTing new rows (existing rows keep getting
last_seen_at refreshed so nothing already stored churns out early) and logs
loudly. The size is re-read at most every `ttl` seconds."""
from __future__ import annotations

import sys
import time

import asyncpg

from src.pgpipe.config import settings


def over_limit(size_mb: float, max_mb: float) -> bool:
    return max_mb > 0 and size_mb > max_mb


class SizeGuard:
    def __init__(self, pool: asyncpg.Pool, max_mb: float | None = None, ttl: float = 30.0) -> None:
        self._pool = pool
        self.max_mb = settings.max_db_mb if max_mb is None else float(max_mb)
        self._ttl = ttl
        self._checked = 0.0
        self.size_mb = 0.0
        self.blocked = False
        self._warned = False

    async def refresh(self) -> bool:
        """Re-read the DB size if the cached value is stale; returns True when
        inserts of NEW rows must be suppressed."""
        now = time.monotonic()
        if now - self._checked >= self._ttl:
            self._checked = now
            size = await self._pool.fetchval("SELECT pg_database_size(current_database())")
            self.size_mb = float(size or 0) / 1048576.0
            self.blocked = over_limit(self.size_mb, self.max_mb)
            if self.blocked and not self._warned:
                self._warned = True
                print(
                    f"!!! DB SIZE GUARD: database is {self.size_mb:.1f} MB > MAX_DB_MB="
                    f"{self.max_mb:.0f} MB. NOT inserting new rows (updating last_seen_at "
                    f"only) until space is freed. Cut boards or shorten retention.",
                    file=sys.stderr, flush=True,
                )
        return self.blocked
