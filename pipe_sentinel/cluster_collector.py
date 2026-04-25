"""Collect recent audit records and feed them into the cluster engine."""
from __future__ import annotations

from typing import List, Sequence, Tuple

from pipe_sentinel.audit import AuditRecord, fetch_recent
from pipe_sentinel.cluster import ClusterReport, cluster_failures


def _failed_pairs(
    records: Sequence[AuditRecord],
) -> List[Tuple[str, str]]:
    """Return (pipeline_name, error) for every failed record that has an error."""
    pairs: List[Tuple[str, str]] = []
    for rec in records:
        if rec.status != "success" and rec.error:
            pairs.append((rec.pipeline, rec.error))
    return pairs


def collect_clusters(db_path: str, limit: int = 200) -> ClusterReport:
    """Fetch recent audit records and cluster their failure messages.

    Args:
        db_path: Path to the SQLite audit database.
        limit:   Maximum number of recent records to consider.

    Returns:
        A ClusterReport grouping pipelines by failure fingerprint.
    """
    records = fetch_recent(db_path, limit=limit)
    pairs = _failed_pairs(records)
    return cluster_failures(pairs)
