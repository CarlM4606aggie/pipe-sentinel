"""Failure forecast: predict likelihood of failure based on recent trend."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from pipe_sentinel.audit import AuditRecord


@dataclass
class ForecastResult:
    pipeline_name: str
    sample_count: int
    recent_failure_rate: float   # last window
    baseline_failure_rate: float # earlier window
    trend: float                 # recent - baseline  (positive = worsening)
    risk_level: str              # "low" | "medium" | "high"

    def __str__(self) -> str:
        icon = {"low": "✅", "medium": "⚠️", "high": "🔴"}.get(self.risk_level, "?")
        return (
            f"{icon} {self.pipeline_name}: risk={self.risk_level} "
            f"recent={self.recent_failure_rate:.0%} "
            f"baseline={self.baseline_failure_rate:.0%} "
            f"trend={self.trend:+.0%}"
        )


def _failure_rate(records: Sequence[AuditRecord]) -> float:
    if not records:
        return 0.0
    return sum(1 for r in records if r.status != "success") / len(records)


def _risk_level(recent: float, trend: float) -> str:
    if recent >= 0.7 or trend >= 0.3:
        return "high"
    if recent >= 0.4 or trend >= 0.15:
        return "medium"
    return "low"


def forecast_pipeline(
    name: str,
    records: Sequence[AuditRecord],
    *,
    min_samples: int = 4,
) -> ForecastResult | None:
    """Return a ForecastResult or None when there are too few samples."""
    if len(records) < min_samples:
        return None

    mid = len(records) // 2
    baseline_records = records[:mid]
    recent_records = records[mid:]

    baseline_rate = _failure_rate(baseline_records)
    recent_rate = _failure_rate(recent_records)
    trend = recent_rate - baseline_rate

    return ForecastResult(
        pipeline_name=name,
        sample_count=len(records),
        recent_failure_rate=recent_rate,
        baseline_failure_rate=baseline_rate,
        trend=trend,
        risk_level=_risk_level(recent_rate, trend),
    )


def scan_forecasts(
    records_by_pipeline: dict[str, List[AuditRecord]],
    *,
    min_samples: int = 4,
) -> List[ForecastResult]:
    results: List[ForecastResult] = []
    for name, recs in records_by_pipeline.items():
        result = forecast_pipeline(name, recs, min_samples=min_samples)
        if result is not None:
            results.append(result)
    results.sort(key=lambda r: r.recent_failure_rate, reverse=True)
    return results
