"""Collect audit records and partition them for regression analysis."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Tuple

from pipe_sentinel.audit import AuditRecord, fetch_recent


def _group_by_pipeline(
    records: List[AuditRecord],
) -> Dict[str, List[AuditRecord]]:
    groups: Dict[str, List[AuditRecord]] = defaultdict(list)
    for rec in records:
        groups[rec.pipeline].append(rec)
    return groups


def collect_regression_groups(
    db_path: str,
    baseline_limit: int = 50,
    current_limit: int = 20,
) -> Dict[str, Tuple[List[AuditRecord], List[AuditRecord]]]:
    """Return a mapping of pipeline -> (baseline_records, current_records).

    *baseline_limit* controls how many historical records to fetch;
    the most recent *current_limit* of those are treated as the current
    window and the remainder as the baseline.
    """
    all_records = fetch_recent(db_path, limit=baseline_limit)
    by_pipeline = _group_by_pipeline(all_records)

    groups = {}
    for name, recs in by_pipeline.items():
        # records are stored newest-first; keep ordering consistent
        recs_sorted = sorted(recs, key=lambda r: r.started_at)
        current = recs_sorted[-current_limit:]
        baseline = recs_sorted[: max(0, len(recs_sorted) - current_limit)]
        if baseline and current:
            groups[name] = (baseline, current)
    return groups
