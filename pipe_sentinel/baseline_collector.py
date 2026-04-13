"""Convenience layer: load records from the audit DB and run baseline checks."""
from __future__ import annotations

from typing import List

from pipe_sentinel.audit import fetch_recent, AuditRecord
from pipe_sentinel.baseline import check_violations, BaselineViolation


def collect_and_check(
    db_path: str,
    history_limit: int = 200,
    recent_limit: int = 10,
    threshold_multiplier: float = 2.0,
    min_samples: int = 3,
) -> List[BaselineViolation]:
    """Fetch audit records and return any baseline violations.

    *history_limit* rows supply the baseline statistics; the most recent
    *recent_limit* rows are the candidates being evaluated.
    """
    history: List[AuditRecord] = fetch_recent(db_path, limit=history_limit)
    if not history:
        return []
    recent: List[AuditRecord] = history[:recent_limit]
    # Use the full history (including recent) as the baseline population so
    # that the baseline is as representative as possible.
    return check_violations(
        records=history,
        recent=recent,
        threshold_multiplier=threshold_multiplier,
        min_samples=min_samples,
    )
