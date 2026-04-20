"""Collect pipeline run results and apply lockout logic."""
from __future__ import annotations

import time
from pathlib import Path
from typing import List, Optional

from pipe_sentinel.lockout import LockoutStore
from pipe_sentinel.runner import RunResult


def store_from_path(path: str | Path) -> LockoutStore:
    return LockoutStore(path=Path(path))


def apply_failures(
    store: LockoutStore,
    results: List[RunResult],
    duration_seconds: float = 300.0,
    threshold: int = 1,
    now: Optional[float] = None,
) -> List[str]:
    """Lock pipelines whose failure count meets the threshold.

    Returns list of pipeline names that were locked.
    """
    t = now if now is not None else time.time()
    from collections import Counter
    failure_counts: Counter = Counter()
    for r in results:
        if not r.success:
            failure_counts[r.pipeline] += 1

    locked: List[str] = []
    for pipeline, count in failure_counts.items():
        if count >= threshold and not store.is_locked(pipeline, t):
            reason = f"{count} failure(s) in current run batch"
            store.lock(pipeline, duration_seconds, reason, now=t)
            locked.append(pipeline)
    return locked


def filter_blocked(
    store: LockoutStore,
    pipelines: list,
    now: Optional[float] = None,
) -> list:
    """Return pipelines that are NOT currently locked."""
    return [p for p in pipelines if not store.is_locked(p.name, now)]
