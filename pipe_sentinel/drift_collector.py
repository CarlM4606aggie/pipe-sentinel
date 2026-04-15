"""Collect audit records from the DB and partition them into
historical and recent windows for drift analysis."""
from __future__ import annotations

from typing import Dict, List, Sequence

from pipe_sentinel.audit import AuditRecord, fetch_recent
from pipe_sentinel.drift import DriftResult, scan_drift


def _partition(
    records: List[AuditRecord],
    recent_n: int,
) -> tuple[List[AuditRecord], List[AuditRecord]]:
    """Split *records* (newest-last) into historical and recent slices."""
    if len(records) <= recent_n:
        return records, records
    historical = records[:-recent_n]
    recent = records[-recent_n:]
    return historical, recent


def collect_drift(
    db_path: str,
    pipeline_names: Sequence[str],
    total_lookback: int = 60,
    recent_n: int = 10,
    threshold: float = 0.15,
) -> List[DriftResult]:
    """Fetch records and compute drift for every named pipeline."""
    historical_map: Dict[str, List[AuditRecord]] = {}
    recent_map: Dict[str, List[AuditRecord]] = {}

    for name in pipeline_names:
        records = list(reversed(fetch_recent(db_path, name, total_lookback)))
        hist, rec = _partition(records, recent_n)
        historical_map[name] = hist
        recent_map[name] = rec

    return scan_drift(list(pipeline_names), historical_map, recent_map, threshold)
