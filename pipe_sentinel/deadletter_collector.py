"""Collect failed RunResults into the dead-letter store."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from pipe_sentinel.deadletter import DeadLetterEntry, DeadLetterStore
from pipe_sentinel.runner import RunResult

_DEFAULT_PATH = Path(".pipe_sentinel_deadletter.json")


def store_from_path(path: Optional[str]) -> DeadLetterStore:
    return DeadLetterStore(path=Path(path) if path else _DEFAULT_PATH)


def collect_failures(
    results: List[RunResult],
    store: DeadLetterStore,
    *,
    attempts: int = 1,
) -> List[DeadLetterEntry]:
    """Push each failed RunResult into the store; return newly added entries."""
    added: List[DeadLetterEntry] = []
    for result in results:
        if not result.success:
            entry = DeadLetterEntry.from_run_result(result, attempts=attempts)
            store.push(entry)
            added.append(entry)
    return added


def purge_recovered(
    results: List[RunResult],
    store: DeadLetterStore,
) -> List[str]:
    """Remove entries whose pipeline has since succeeded; return removed IDs."""
    succeeded_names = {r.pipeline_name for r in results if r.success}
    removed: List[str] = []
    for entry in store.all_entries():
        if entry.pipeline_name in succeeded_names:
            store.remove(entry.entry_id)
            removed.append(entry.entry_id)
    return removed
