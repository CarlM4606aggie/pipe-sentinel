"""Baseline duration tracking for pipeline runs.

Computes expected duration baselines from historical audit records
and flags runs that deviate significantly from the baseline.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipe_sentinel.audit import AuditRecord


@dataclass
class BaselineStats:
    pipeline_name: str
    sample_count: int
    mean_duration: float          # seconds
    std_duration: float           # seconds
    threshold_multiplier: float   # flag if actual > mean + N * std

    @property
    def upper_bound(self) -> float:
        return self.mean_duration + self.threshold_multiplier * self.std_duration


@dataclass
class BaselineViolation:
    pipeline_name: str
    actual_duration: float
    baseline: BaselineStats

    @property
    def excess_seconds(self) -> float:
        return self.actual_duration - self.baseline.upper_bound

    def __str__(self) -> str:
        return (
            f"{self.pipeline_name}: ran {self.actual_duration:.1f}s "
            f"(expected ≤ {self.baseline.upper_bound:.1f}s, "
            f"mean={self.baseline.mean_duration:.1f}s)"
        )


def compute_baseline(
    records: List[AuditRecord],
    pipeline_name: str,
    threshold_multiplier: float = 2.0,
    min_samples: int = 3,
) -> Optional[BaselineStats]:
    """Return a BaselineStats for *pipeline_name* or None if insufficient data."""
    durations = [
        r.duration
        for r in records
        if r.pipeline_name == pipeline_name and r.duration is not None and r.status == "success"
    ]
    if len(durations) < min_samples:
        return None
    mean = sum(durations) / len(durations)
    variance = sum((d - mean) ** 2 for d in durations) / len(durations)
    std = variance ** 0.5
    return BaselineStats(
        pipeline_name=pipeline_name,
        sample_count=len(durations),
        mean_duration=mean,
        std_duration=std,
        threshold_multiplier=threshold_multiplier,
    )


def check_violations(
    records: List[AuditRecord],
    recent: List[AuditRecord],
    threshold_multiplier: float = 2.0,
    min_samples: int = 3,
) -> List[BaselineViolation]:
    """Check *recent* runs against baselines derived from *records*."""
    violations: List[BaselineViolation] = []
    names = {r.pipeline_name for r in recent}
    for name in names:
        baseline = compute_baseline(records, name, threshold_multiplier, min_samples)
        if baseline is None:
            continue
        for rec in recent:
            if rec.pipeline_name != name or rec.duration is None:
                continue
            if rec.duration > baseline.upper_bound:
                violations.append(
                    BaselineViolation(
                        pipeline_name=name,
                        actual_duration=rec.duration,
                        baseline=baseline,
                    )
                )
    return violations
