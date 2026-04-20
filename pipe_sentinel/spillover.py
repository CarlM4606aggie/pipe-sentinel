"""Spillover detection: identify pipelines that consistently exceed their
scheduled duration window, potentially starving downstream pipelines."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence


@dataclass
class SpilloverResult:
    pipeline_name: str
    scheduled_duration: float   # seconds
    actual_duration: float      # seconds (mean of recent runs)
    spillover_seconds: float    # actual - scheduled
    sample_count: int
    is_spilling: bool

    def __str__(self) -> str:
        icon = "⚠" if self.is_spilling else "✓"
        return (
            f"{icon} {self.pipeline_name}: scheduled={self.scheduled_duration:.1f}s "
            f"actual={self.actual_duration:.1f}s "
            f"spillover={self.spillover_seconds:+.1f}s (n={self.sample_count})"
        )


def _mean_duration(records: Sequence) -> Optional[float]:
    durations = [r.duration for r in records if r.duration is not None]
    if not durations:
        return None
    return sum(durations) / len(durations)


def detect_spillover(
    pipeline_name: str,
    scheduled_duration: float,
    records: Sequence,
    min_samples: int = 3,
) -> Optional[SpilloverResult]:
    """Return a SpilloverResult if enough records exist, else None."""
    relevant = [r for r in records if r.pipeline_name == pipeline_name]
    if len(relevant) < min_samples:
        return None
    mean = _mean_duration(relevant)
    if mean is None:
        return None
    spillover = mean - scheduled_duration
    return SpilloverResult(
        pipeline_name=pipeline_name,
        scheduled_duration=scheduled_duration,
        actual_duration=mean,
        spillover_seconds=spillover,
        sample_count=len(relevant),
        is_spilling=spillover > 0,
    )


def scan_spillovers(
    pipelines,
    records: Sequence,
    min_samples: int = 3,
) -> List[SpilloverResult]:
    """Scan all pipelines that have a scheduled_duration configured."""
    results: List[SpilloverResult] = []
    for pipeline in pipelines:
        scheduled = getattr(pipeline, "scheduled_duration", None)
        if scheduled is None:
            continue
        result = detect_spillover(
            pipeline.name, scheduled, records, min_samples=min_samples
        )
        if result is not None:
            results.append(result)
    return results
