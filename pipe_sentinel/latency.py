"""Latency tracking: detect pipelines whose recent run durations exceed a
configured threshold compared to their historical baseline."""
from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import List, Optional, Sequence


@dataclass
class LatencyResult:
    pipeline_name: str
    recent_mean: float          # seconds
    baseline_mean: float        # seconds
    ratio: float                # recent / baseline
    threshold: float            # ratio that triggers alert
    is_slow: bool

    def __str__(self) -> str:
        icon = "\u26a0\ufe0f" if self.is_slow else "\u2705"
        return (
            f"{icon} {self.pipeline_name}: "
            f"recent={self.recent_mean:.1f}s  "
            f"baseline={self.baseline_mean:.1f}s  "
            f"ratio={self.ratio:.2f}x"
        )


def _mean_duration(records: Sequence) -> Optional[float]:
    """Return mean duration_seconds from records, ignoring None values."""
    durations = [
        r.duration_seconds
        for r in records
        if getattr(r, "duration_seconds", None) is not None
    ]
    if not durations:
        return None
    return mean(durations)


def detect_latency(
    pipeline_name: str,
    recent_records: Sequence,
    baseline_records: Sequence,
    threshold: float = 1.5,
) -> Optional[LatencyResult]:
    """Compare recent mean duration against baseline mean.

    Returns None when there is insufficient data to make a determination.
    """
    recent_mean = _mean_duration(recent_records)
    baseline_mean = _mean_duration(baseline_records)

    if recent_mean is None or baseline_mean is None or baseline_mean == 0.0:
        return None

    ratio = recent_mean / baseline_mean
    return LatencyResult(
        pipeline_name=pipeline_name,
        recent_mean=recent_mean,
        baseline_mean=baseline_mean,
        ratio=ratio,
        threshold=threshold,
        is_slow=ratio >= threshold,
    )


def scan_latency(
    pipeline_names: Sequence[str],
    all_records: Sequence,
    recent_window: int = 5,
    threshold: float = 1.5,
) -> List[LatencyResult]:
    """Scan all pipelines for latency regressions.

    ``recent_window`` is the number of most-recent records used as the
    'recent' sample; all remaining records form the baseline.
    """
    results: List[LatencyResult] = []
    for name in pipeline_names:
        pipeline_records = [
            r for r in all_records if getattr(r, "pipeline_name", None) == name
        ]
        if len(pipeline_records) <= recent_window:
            continue
        recent = pipeline_records[-recent_window:]
        baseline = pipeline_records[:-recent_window]
        result = detect_latency(name, recent, baseline, threshold)
        if result is not None:
            results.append(result)
    return results
