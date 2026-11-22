"""asyncpg pool + schema bootstrap for pgpipe."""
from __future__ import annotations

import asyncio
import random
import sys

import asyncpg

from src.pgpipe.config import SCHEMA_SQL, settings

# Errors that mean "the DB / pooler is momentarily out of connection slots or
# dropped us" (Supabase session-mode pooler caps clients hard; 8 matrix shards +
# linkedin + finalize starting together exceed it). Always worth a retry.
TRANSIENT_DB_ERRORS = (
    asyncpg.TooManyConnectionsError,
    asyncpg.CannotConnectNowError,
    asyncpg.ConnectionDoesNotExistError,
    asyncpg.InterfaceError,
    asyncpg.PostgresConnectionError,
    ConnectionError,
    OSError,
    asyncio.TimeoutError,
)


async def with_retry(fn, *, attempts: int = 6, base: float = 2.0, cap: float = 45.0, what: str = "db"):
    """Await fn() (a zero-arg coroutine factory), retrying transient connection
    errors with jittered exponential backoff."""
    for i in range(attempts):
        try:
            return await fn()
        except TRANSIENT_DB_ERRORS as exc:
            if i == attempts - 1:
                raise
            delay = min(cap, base * (2 ** i)) * random.uniform(0.6, 1.4)
            print(f"[{what}] transient {type(exc).__name__}: retry {i + 1}/{attempts - 1} in {delay:.0f}s",
                  file=sys.stderr, flush=True)
            await asyncio.sleep(delay)


async def connect() -> asyncpg.Pool:
    """Open a small pool against LOCAL_DATABASE_URL with search_path set to
    <schema>,public so unqualified table names resolve to our schema."""
    schema = settings.validated_schema()
    dsn = settings.require_db()
    return await with_retry(
        lambda: asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=settings.pool_max,  # small: 8 matrix shards share the DB's connection cap
            statement_cache_size=0,          # Supabase pooler friendly
            command_timeout=120,
            server_settings={
                "application_name": f"pgpipe:{settings.worker_id}",
                "search_path": f"{schema},public",
            },
        ),
        attempts=8, what="connect",
    )


async def apply_schema(pool: asyncpg.Pool) -> None:
    """Run schema.sql (idempotent). {{SCHEMA}} -> validated identifier."""
    schema = settings.validated_schema()
    sql = SCHEMA_SQL.read_text(encoding="utf-8").replace("{{SCHEMA}}", schema)
    async with pool.acquire() as conn:
        await conn.execute(sql)
