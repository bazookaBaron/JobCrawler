# patches/

Diffs applied to upstream jobseek (`apps/crawler/`) at commit
`73179987ad34d9bdb480c5845c0c24af5f862691`. Everything else in `crawler/src/`
is byte-for-byte upstream; all new logic is in `crawler/src/pgpipe/` (a new
package, not a patch).

| file | what |
|---|---|
| `01-config-remove-redis.diff` | `src/config.py` — delete `redis_url`, `upstash_redis_rest_url`, `upstash_redis_rest_token`. `redis_max_connections` kept only so the now-dead `src/redis_queue.py` still imports. |
| `02-pyproject-add-jobs-entrypoint.diff` | `pyproject.toml` — add `jobs = "src.pgpipe.cli:main"` to `[project.scripts]`. No dependency changes; `uv.lock` unchanged. |

Re-apply after an upstream sync:

```bash
cd crawler
git apply --3way ../patches/01-config-remove-redis.diff
git apply --3way ../patches/02-pyproject-add-jobs-entrypoint.diff
```

## Why Redis wasn't rewritten in place

`src/redis_queue.py` (39 KB, Lua scripts, inflight leases, circuit breakers,
dead-letters) and `src/workers/pipeline.py` exist to coordinate *many
distributed workers*. A $0 single-process cron needs none of that. `pgpipe`
implements the small slice that matters — a `FOR UPDATE SKIP LOCKED` queue with
a 30-minute stale-claim reclaim, and an in-process politeness gate — as ~700
lines of new, testable code. The upstream Redis modules are left inert (see
`UPSTREAM_DRIFT.md` §2).
