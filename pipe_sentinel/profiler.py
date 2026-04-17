"""Runtime profiling: track and compare pipeline execution durations."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipe_sentinel.audit import AuditRecord


@dataclass
class ProfileStats:
    pipeline: str
    sample_count: int
    min_seconds: float
    max_seconds: float
    mean_seconds: float
    p95_seconds: float

    def is_slow(self, threshold: float) -> bool:
        """Return True if p95 exceeds the given threshold in seconds."""
        return self.p95_seconds > threshold

    def __str__(self) -> str:
        return (
            f"{self.pipeline}: mean={self.mean_seconds:.2f}s "
            f"p95={self.p95_seconds:.2f}s "
            f"min={self.min_seconds:.2f}s max={self.max_seconds:.2f}s "
            f"(n={self.sample_count})"
        )


def _percentile(sorted_values: List[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = int(len(sorted_values) * pct)
    idx = min(idx, len(sorted_values) - 1)
    return sorted_values[idx]


def compute_profile(pipeline: str, records: List[AuditRecord]) -> Optional[ProfileStats]:
    """Compute profiling stats for a single pipeline from audit records."""
    durations = sorted(
        r.duration_seconds for r in records
        if r.pipeline == pipeline and r.duration_seconds is not None
    )
    if not durations:
        return None
    return ProfileStats(
        pipeline=pipeline,
        sample_count=len(durations),
        min_seconds=durations[0],
        max_seconds=durations[-1],
        mean_seconds=sum(durations) / len(durations),
        p95_seconds=_percentile(durations, 0.95),
    )


def scan_profiles(
    records: List[AuditRecord],
    pipeline_names: List[str],
) -> List[ProfileStats]:
    """Return ProfileStats for each named pipeline that has data."""
    results = []
    for name in pipeline_names:
        stats = compute_profile(name, records)
        if stats is not None:
            results.append(stats)
    return results
