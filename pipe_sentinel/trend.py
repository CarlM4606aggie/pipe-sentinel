"""Trend detection: compares recent failure rate to historical baseline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipe_sentinel.audit import AuditRecord


@dataclass
class TrendResult:
    pipeline: str
    recent_rate: float
    baseline_rate: float
    delta: float
    worsening: bool
    sample_count: int

    def __str__(self) -> str:
        direction = "↑ worsening" if self.worsening else "↓ stable/improving"
        return (
            f"{self.pipeline}: recent={self.recent_rate:.1%} "
            f"baseline={self.baseline_rate:.1%} delta={self.delta:+.1%} {direction}"
        )


def _failure_rate(records: List[AuditRecord]) -> float:
    if not records:
        return 0.0
    return sum(1 for r in records if r.status != "success") / len(records)


def detect_trend(
    records: List[AuditRecord],
    pipeline: str,
    recent_window: int = 10,
    min_baseline: int = 5,
    worsening_threshold: float = 0.1,
) -> Optional[TrendResult]:
    """Detect if recent failure rate is significantly worse than historical."""
    pipeline_records = [r for r in records if r.pipeline == pipeline]
    if len(pipeline_records) < recent_window + min_baseline:
        return None

    sorted_records = sorted(pipeline_records, key=lambda r: r.timestamp)
    recent = sorted_records[-recent_window:]
    historical = sorted_records[:-recent_window]

    recent_rate = _failure_rate(recent)
    baseline_rate = _failure_rate(historical)
    delta = recent_rate - baseline_rate
    worsening = delta >= worsening_threshold

    return TrendResult(
        pipeline=pipeline,
        recent_rate=recent_rate,
        baseline_rate=baseline_rate,
        delta=delta,
        worsening=worsening,
        sample_count=len(pipeline_records),
    )


def scan_trends(
    records: List[AuditRecord],
    recent_window: int = 10,
    min_baseline: int = 5,
    worsening_threshold: float = 0.1,
) -> List[TrendResult]:
    names = {r.pipeline for r in records}
    results = []
    for name in sorted(names):
        result = detect_trend(
            records, name, recent_window, min_baseline, worsening_threshold
        )
        if result is not None:
            results.append(result)
    return results
