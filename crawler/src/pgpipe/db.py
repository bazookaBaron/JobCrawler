"""asyncpg pool + schema bootstrap for pgpipe."""
from __future__ import annotations

import asyncpg

from src.pgpipe.config import SCHEMA_SQL, settings


async def connect() -> asyncpg.Pool:
    """Open a small pool against LOCAL_DATABASE_URL with search_path set to
    <schema>,public so unqualified table names resolve to our schema."""
    schema = settings.validated_schema()
    dsn = settings.require_db()
    return await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=max(4, settings.concurrency + 2),
        statement_cache_size=0,          # Supabase pooler friendly
        command_timeout=120,
        server_settings={
            "application_name": f"pgpipe:{settings.worker_id}",
            "search_path": f"{schema},public",
        },
    )


async def apply_schema(pool: asyncpg.Pool) -> None:
    """Run schema.sql (idempotent). {{SCHEMA}} -> validated identifier."""
    schema = settings.validated_schema()
    sql = SCHEMA_SQL.read_text(encoding="utf-8").replace("{{SCHEMA}}", schema)
    async with pool.acquire() as conn:
        await conn.execute(sql)
