"""Pipeline run metrics aggregation and trend detection."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipe_sentinel.audit import AuditRecord


@dataclass
class TrendPoint:
    pipeline_name: str
    window: int  # number of recent runs considered
    failure_rate: float  # 0.0 – 1.0
    avg_duration_s: float
    is_degrading: bool  # failure_rate above threshold


@dataclass
class MetricsReport:
    points: List[TrendPoint] = field(default_factory=list)

    @property
    def degrading(self) -> List[TrendPoint]:
        return [p for p in self.points if p.is_degrading]

    @property
    def healthy(self) -> List[TrendPoint]:
        return [p for p in self.points if not p.is_degrading]


def _failure_rate(records: List[AuditRecord]) -> float:
    if not records:
        return 0.0
    failed = sum(1 for r in records if r.status != "success")
    return failed / len(records)


def _avg_duration(records: List[AuditRecord]) -> float:
    durations = [r.duration_s for r in records if r.duration_s is not None]
    if not durations:
        return 0.0
    return sum(durations) / len(durations)


def compute_trend(
    name: str,
    records: List[AuditRecord],
    window: int = 10,
    degradation_threshold: float = 0.4,
) -> TrendPoint:
    recent = records[-window:] if len(records) > window else records
    rate = _failure_rate(recent)
    return TrendPoint(
        pipeline_name=name,
        window=len(recent),
        failure_rate=rate,
        avg_duration_s=_avg_duration(recent),
        is_degrading=rate >= degradation_threshold,
    )


def build_metrics_report(
    records_by_pipeline: dict,
    window: int = 10,
    degradation_threshold: float = 0.4,
) -> MetricsReport:
    points = [
        compute_trend(name, recs, window, degradation_threshold)
        for name, recs in records_by_pipeline.items()
    ]
    return MetricsReport(points=points)
