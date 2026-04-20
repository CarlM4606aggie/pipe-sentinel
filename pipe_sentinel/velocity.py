"""Velocity tracking: detect sudden changes in pipeline run frequency."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from pipe_sentinel.audit import AuditRecord


@dataclass
class VelocityResult:
    pipeline: str
    window_hours: int
    recent_count: int
    baseline_count: int
    ratio: float  # recent / baseline; <1 means slowdown, >1 means speedup
    is_anomalous: bool

    def __str__(self) -> str:
        direction = "speedup" if self.ratio > 1.0 else "slowdown"
        icon = "⚡" if self.ratio > 1.0 else "🐢"
        return (
            f"{icon} {self.pipeline}: {direction} "
            f"(recent={self.recent_count}, baseline={self.baseline_count}, "
            f"ratio={self.ratio:.2f})"
        )


def _count_in_window(records: Sequence[AuditRecord], cutoff_ts: float) -> int:
    return sum(1 for r in records if r.timestamp >= cutoff_ts)


def detect_velocity(
    records: Sequence[AuditRecord],
    pipeline: str,
    window_hours: int = 24,
    baseline_multiplier: int = 3,
    threshold: float = 0.5,
) -> VelocityResult:
    """Compare run count in recent window vs. a longer baseline window.

    A ratio below *threshold* or above ``1 / threshold`` is flagged as anomalous.
    """
    import time

    now = time.time()
    window_secs = window_hours * 3600
    baseline_secs = window_secs * baseline_multiplier

    pipeline_records = [r for r in records if r.pipeline == pipeline]
    recent_count = _count_in_window(pipeline_records, now - window_secs)
    baseline_total = _count_in_window(pipeline_records, now - baseline_secs)
    # Baseline excludes the recent window to form a fair comparison period
    baseline_count = max(baseline_total - recent_count, 0)

    # Normalise to runs-per-window-period for comparison
    if baseline_count == 0:
        ratio = float(recent_count) if recent_count else 1.0
    else:
        ratio = recent_count / (baseline_count / baseline_multiplier)

    is_anomalous = ratio < threshold or ratio > (1.0 / threshold if threshold > 0 else float("inf"))

    return VelocityResult(
        pipeline=pipeline,
        window_hours=window_hours,
        recent_count=recent_count,
        baseline_count=baseline_count,
        ratio=round(ratio, 4),
        is_anomalous=is_anomalous,
    )


def scan_velocity(
    records: Sequence[AuditRecord],
    pipelines: Sequence[str],
    window_hours: int = 24,
    baseline_multiplier: int = 3,
    threshold: float = 0.5,
) -> List[VelocityResult]:
    """Return velocity results for every pipeline in *pipelines*."""
    return [
        detect_velocity(records, p, window_hours, baseline_multiplier, threshold)
        for p in pipelines
    ]
