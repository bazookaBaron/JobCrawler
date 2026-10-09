"""Day-over-day job identity, stored as one compressed blob in the daily row.

Per open job we keep (hashed key, board hash, first day) — 16 bytes, no
attributes. Comparing today's crawl with yesterday's blob gives exact new and
removed counts and how long removed jobs were open. Jobs on boards that FAILED
today are carried forward unchanged, so a flaky board doesn't show up as a
wave of removals today and a wave of "new" jobs tomorrow.
"""

from __future__ import annotations

import struct
import zlib
from array import array
from dataclasses import dataclass

_MAGIC = b"RS1"


def board_hash(board_slug: str) -> int:
    return zlib.crc32(board_slug.encode()) & 0x7FFFFFFF


@dataclass
class JobState:
    keys: array  # 'q'
    boards: array  # 'i'
    first_day: array  # 'i'

    def __len__(self) -> int:
        return len(self.keys)


def encode(state: JobState) -> bytes:
    n = len(state.keys)
    body = state.keys.tobytes() + state.boards.tobytes() + state.first_day.tobytes()
    return zlib.compress(_MAGIC + struct.pack("<I", n) + body, 6)


def decode(blob: bytes | None) -> JobState | None:
    if not blob:
        return None
    raw = zlib.decompress(bytes(blob))
    if raw[:3] != _MAGIC:
        return None
    (n,) = struct.unpack("<I", raw[3:7])
    off = 7
    keys = array("q")
    keys.frombytes(raw[off : off + 8 * n])
    off += 8 * n
    boards = array("i")
    boards.frombytes(raw[off : off + 4 * n])
    off += 4 * n
    first = array("i")
    first.frombytes(raw[off : off + 4 * n])
    return JobState(keys, boards, first)


# removed-job age, exact day counts capped at this value (mergeable histogram)
MAX_DAYS_OPEN = 365


@dataclass
class Comparison:
    is_new: list[bool]  # aligned with today's keys
    removed: int | None  # None on the baseline run
    removed_days: dict[str, int]  # {days_open: count}
    carried: int  # jobs kept because their board failed today
    new_state: JobState


def compare(
    prev: JobState | None,
    keys: list[int],
    boards: list[int],
    posted_days: list[int | None],
    failed_boards: set[int],
    today: int,
) -> Comparison:
    if prev is None:
        # Baseline run: no yesterday, so nothing is "new" (and nothing removed).
        is_new = [False] * len(keys)
        first = array("i", [min(today, pd) if pd is not None else today for pd in posted_days])
        return Comparison(
            is_new, None, {}, 0, JobState(array("q", keys), array("i", boards), first)
        )

    prev_first = dict(zip(prev.keys, prev.first_day, strict=True))
    today_set = set(keys)
    is_new = [k not in prev_first for k in keys]
    first = array(
        "i",
        [
            prev_first.get(k, min(today, pd) if pd is not None else today)
            for k, pd in zip(keys, posted_days, strict=True)
        ],
    )

    removed = 0
    removed_days: dict[str, int] = {}
    carry_k, carry_b, carry_f = array("q"), array("i"), array("i")
    for k, b, f in zip(prev.keys, prev.boards, prev.first_day, strict=True):
        if k in today_set:
            continue
        if b in failed_boards:
            carry_k.append(k)
            carry_b.append(b)
            carry_f.append(f)
            continue
        removed += 1
        d = str(min(MAX_DAYS_OPEN, max(0, today - f)))
        removed_days[d] = removed_days.get(d, 0) + 1

    new_keys = array("q", keys)
    new_keys.extend(carry_k)
    new_boards = array("i", boards)
    new_boards.extend(carry_b)
    first.extend(carry_f)
    return Comparison(
        is_new, removed, removed_days, len(carry_k), JobState(new_keys, new_boards, first)
    )
