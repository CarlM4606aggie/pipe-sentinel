"""Collect audit records grouped by pipeline name for metrics."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional

from pipe_sentinel.audit import AuditRecord, fetch_recent
from pipe_sentinel.metrics import MetricsReport, build_metrics_report


def collect_records(
    db_path: str,
    limit: int = 200,
    pipeline_names: Optional[List[str]] = None,
) -> Dict[str, List[AuditRecord]]:
    """Fetch recent audit records and group them by pipeline name."""
    records = fetch_recent(db_path, limit=limit)
    grouped: Dict[str, List[AuditRecord]] = defaultdict(list)
    for record in records:
        if pipeline_names and record.pipeline_name not in pipeline_names:
            continue
        grouped[record.pipeline_name].append(record)
    # Ensure chronological order within each group
    for name in grouped:
        grouped[name].sort(key=lambda r: r.ran_at)
    return dict(grouped)


def collect_metrics(
    db_path: str,
    limit: int = 200,
    window: int = 10,
    degradation_threshold: float = 0.4,
    pipeline_names: Optional[List[str]] = None,
) -> MetricsReport:
    """High-level helper: collect records and build a MetricsReport."""
    grouped = collect_records(db_path, limit=limit, pipeline_names=pipeline_names)
    return build_metrics_report(
        grouped,
        window=window,
        degradation_threshold=degradation_threshold,
    )
