"""Pure-Python reference of the greedy company-cooldown interleave used by
`{schema}.cooldown_order()` / `build_feed()` / `feed_page()` in schema.sql.

Input: company keys, ALREADY sorted newest-first. Walk the list placing jobs;
a company that was just placed cannot be placed again until `gap` (9) other
placements happened. If every remaining job belongs to a cooling company,
place the oldest-waiting one (the company whose last placement is earliest).
Returns the 0-based input indexes in placed order. The SQL mirrors this
step-for-step (see tests/pgpipe/test_feed_reference.py + test_feed_sql.py).
"""
from __future__ import annotations

from typing import Hashable, Sequence


def cooldown_order(
    keys: Sequence[Hashable], gap: int = 9, limit: int | None = None
) -> list[int]:
    n = len(keys)
    lim = n if limit is None else min(n, limit)
    used = [False] * n
    out: list[int] = []
    win: list[Hashable] = []  # last `gap` placed keys, oldest first
    first_unused = 0
    while len(out) < lim:
        while first_unused < n and used[first_unused]:
            first_unused += 1
        pick = -1
        for i in range(first_unused, n):
            if not used[i] and keys[i] not in win:
                pick = i
                break
        if pick < 0:
            # everything left is cooling: oldest-waiting = earliest LAST placement
            for k, wk in enumerate(win):
                if wk in win[k + 1:]:
                    continue  # a later placement of the same key exists
                for i in range(first_unused, n):
                    if not used[i] and keys[i] == wk:
                        pick = i
                        break
                if pick >= 0:
                    break
        used[pick] = True
        out.append(pick)
        win.append(keys[pick])
        if len(win) > gap:
            win.pop(0)
    return out
