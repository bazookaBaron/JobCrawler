"""In-process politeness — replaces jobseek's Redis per-domain rate limiter
(src/redis_capacity.py). No shared state, no distributed workers: a simple
per-host "min gap between requests" gate plus a global concurrency semaphore.
"""
from __future__ import annotations

import asyncio
import random
import time
from urllib.parse import urlparse

from src.pgpipe.config import settings

# ATS API hosts are shared by many boards; throttle the provider, not the board.
_ATS_HOST_HINTS = (
    "greenhouse.io", "ashbyhq.com", "lever.co", "recruitee.com", "gem.com",
    "pinpointhq.com", "oraclecloud.com", "amazon.jobs", "workable.com",
    "jobvite.com", "myworkdayjobs.com", "bamboohr.com", "jazzhr.com",
    "smartrecruiters.com", "personio.de", "teamtailor.com", "jobylon.com",
)


def _host(url: str) -> str:
    return (urlparse(url).hostname or "").lower()


def _is_ats(host: str) -> bool:
    return any(host == h or host.endswith("." + h) for h in _ATS_HOST_HINTS)


class Politeness:
    """One instance per run. `async with pol.slot(url):` yields once it is
    polite to hit that URL's host, respecting the min per-host gap."""

    def __init__(self) -> None:
        self._sem = asyncio.Semaphore(max(1, settings.concurrency))
        self._host_locks: dict[str, asyncio.Lock] = {}
        self._next_ok: dict[str, float] = {}

    def _lock(self, host: str) -> asyncio.Lock:
        lk = self._host_locks.get(host)
        if lk is None:
            lk = self._host_locks[host] = asyncio.Lock()
        return lk

    def slot(self, url: str) -> _Slot:
        return _Slot(self, url)


class _Slot:
    def __init__(self, pol: Politeness, url: str) -> None:
        self._pol = pol
        self._host = _host(url)
        self._delay = (
            settings.delay_ats_seconds if _is_ats(self._host)
            else settings.delay_other_seconds
        )

    async def __aenter__(self) -> None:
        await self._pol._sem.acquire()
        lock = self._pol._lock(self._host)
        await lock.acquire()
        try:
            now = time.monotonic()
            wait = self._pol._next_ok.get(self._host, 0.0) - now
            if wait > 0:
                await asyncio.sleep(wait)
            jitter = random.uniform(0, settings.delay_jitter_seconds)
            self._pol._next_ok[self._host] = time.monotonic() + self._delay + jitter
        finally:
            lock.release()

    async def __aexit__(self, *exc: object) -> None:
        self._pol._sem.release()
