"""Collect retry budget states from run results."""
from __future__ import annotations

from pathlib import Path
from typing import List

from pipe_sentinel.retry_budget import RetryBudgetConfig, RetryBudgetState, RetryBudgetStore
from pipe_sentinel.runner import RunResult


def store_from_path(path: Path) -> RetryBudgetStore:
    return RetryBudgetStore(path)


def apply_retries(
    results: List[RunResult],
    store: RetryBudgetStore,
    cfg: RetryBudgetConfig,
) -> List[RetryBudgetState]:
    """Record a retry attempt for each failed result and return updated states."""
    updated: List[RetryBudgetState] = []
    for result in results:
        if not result.success:
            state = store.get(result.pipeline_name)
            state.record_attempt()
            updated.append(state)
    store.save()
    return updated


def filter_blocked(
    results: List[RunResult],
    store: RetryBudgetStore,
    cfg: RetryBudgetConfig,
) -> List[RunResult]:
    """Return only results whose retry budget is NOT exhausted."""
    allowed: List[RunResult] = []
    for result in results:
        state = store.get(result.pipeline_name)
        if not state.is_exhausted(cfg):
            allowed.append(result)
    return allowed
