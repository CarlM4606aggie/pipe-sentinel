"""Collect audit records and produce breach results for all pipelines."""
from __future__ import annotations

from typing import List

from pipe_sentinel.audit import AuditRecord, fetch_recent
from pipe_sentinel.breach import BreachResult, scan_breaches
from pipe_sentinel.config import PipelineConfig, SentinelConfig


_DEFAULT_THRESHOLD = 0.5
_DEFAULT_LOOKBACK = 50


def _group_by_pipeline(
    records: List[AuditRecord],
) -> dict[str, list[AuditRecord]]:
    groups: dict[str, list[AuditRecord]] = {}
    for rec in records:
        groups.setdefault(rec.pipeline, []).append(rec)
    return groups


def threshold_for(pipeline: PipelineConfig) -> float:
    """Return the breach threshold for a pipeline (falls back to default)."""
    raw = getattr(pipeline, "breach_threshold", None)
    if raw is None:
        return _DEFAULT_THRESHOLD
    return float(raw)


def collect_breaches(
    db_path: str,
    config: SentinelConfig,
    lookback: int = _DEFAULT_LOOKBACK,
) -> List[BreachResult]:
    """Fetch recent records from the audit DB and evaluate breach status."""
    records = fetch_recent(db_path, n=lookback * len(config.pipelines) or lookback)
    groups = _group_by_pipeline(records)

    pipeline_map = {p.name: p for p in config.pipelines}
    results: List[BreachResult] = []
    for name, recs in sorted(groups.items()):
        pipeline = pipeline_map.get(name)
        threshold = threshold_for(pipeline) if pipeline else _DEFAULT_THRESHOLD
        from pipe_sentinel.breach import detect_breach
        results.append(detect_breach(name, recs, threshold))
    return results
