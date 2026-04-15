"""Collect audit records from the DB and build forecast inputs."""
from __future__ import annotations

from typing import Dict, List

from pipe_sentinel.audit import AuditRecord, fetch_recent
from pipe_sentinel.forecast import ForecastResult, scan_forecasts


def _group_by_pipeline(
    records: List[AuditRecord],
) -> Dict[str, List[AuditRecord]]:
    groups: Dict[str, List[AuditRecord]] = {}
    for rec in records:
        groups.setdefault(rec.pipeline_name, []).append(rec)
    return groups


def collect_forecasts(
    db_path: str,
    *,
    limit: int = 100,
    min_samples: int = 4,
) -> List[ForecastResult]:
    """Fetch recent audit records and return per-pipeline forecasts."""
    records = fetch_recent(db_path, limit=limit)
    groups = _group_by_pipeline(records)
    return scan_forecasts(groups, min_samples=min_samples)
