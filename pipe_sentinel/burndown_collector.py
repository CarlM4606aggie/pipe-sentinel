"""Collect audit records from the DB and produce burndown results."""
from __future__ import annotations

from typing import Dict, List

from pipe_sentinel.audit import AuditRecord, fetch_recent
from pipe_sentinel.burndown import BurndownResult, scan_burndowns


def _group_by_pipeline(
    records: List[AuditRecord],
) -> Dict[str, List[AuditRecord]]:
    groups: Dict[str, List[AuditRecord]] = {}
    for rec in records:
        groups.setdefault(rec.pipeline, []).append(rec)
    return groups


def collect_burndowns(
    db_path: str,
    limit: int = 200,
) -> List[BurndownResult]:
    """Fetch recent audit records and compute burndown for each pipeline."""
    records = fetch_recent(db_path, limit=limit)
    grouped = _group_by_pipeline(records)
    return scan_burndowns(grouped)
