"""Collect failure fingerprints from recent audit records."""
from __future__ import annotations

from typing import List, Optional

from pipe_sentinel.audit import fetch_recent, AuditRecord
from pipe_sentinel.fingerprint import FingerprintReport, scan_fingerprints


def _records_to_failures(records: List[AuditRecord]) -> List[dict]:
    return [
        {"pipeline": r.pipeline, "stderr": r.stderr or ""}
        for r in records
        if not r.success
    ]


def collect_fingerprints(
    db_path: str,
    limit: int = 200,
    recurrence_threshold: int = 2,
    pipeline: Optional[str] = None,
) -> FingerprintReport:
    records = fetch_recent(db_path, limit=limit)
    if pipeline:
        records = [r for r in records if r.pipeline == pipeline]
    failures = _records_to_failures(records)
    return scan_fingerprints(
        failures, recurrence_threshold=recurrence_threshold
    )
