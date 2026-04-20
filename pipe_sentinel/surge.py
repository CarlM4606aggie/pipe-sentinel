"""Surge detection: identify pipelines with a sudden spike in failure count."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence


@dataclass
class SurgeResult:
    pipeline: str
    recent_failures: int
    baseline_failures: float  # average failures per window in history
    ratio: float              # recent / baseline (inf when baseline == 0)
    is_surging: bool

    def __str__(self) -> str:
        icon = "🔺" if self.is_surging else "✅"
        return (
            f"{icon} {self.pipeline}: "
            f"{self.recent_failures} recent failures "
            f"(baseline {self.baseline_failures:.1f}, ratio {self.ratio:.1f}x)"
        )


def _count_failures(records: Sequence, pipeline: str) -> int:
    return sum(
        1 for r in records if r.pipeline == pipeline and not r.success
    )


def detect_surge(
    pipeline: str,
    recent_records: Sequence,
    history_records: Sequence,
    history_windows: int = 4,
    surge_ratio: float = 3.0,
    min_recent: int = 2,
) -> SurgeResult:
    """Compare failure count in the recent window against historical average."""
    recent_failures = _count_failures(recent_records, pipeline)

    history_failures = _count_failures(history_records, pipeline)
    baseline = history_failures / max(history_windows, 1)

    if baseline == 0:
        ratio = float("inf") if recent_failures >= min_recent else 0.0
    else:
        ratio = recent_failures / baseline

    is_surging = recent_failures >= min_recent and ratio >= surge_ratio

    return SurgeResult(
        pipeline=pipeline,
        recent_failures=recent_failures,
        baseline_failures=round(baseline, 2),
        ratio=round(ratio, 2),
        is_surging=is_surging,
    )


def scan_surges(
    pipelines: Sequence[str],
    recent_records: Sequence,
    history_records: Sequence,
    history_windows: int = 4,
    surge_ratio: float = 3.0,
    min_recent: int = 2,
) -> List[SurgeResult]:
    """Return surge results for every pipeline."""
    return [
        detect_surge(
            p,
            recent_records,
            history_records,
            history_windows=history_windows,
            surge_ratio=surge_ratio,
            min_recent=min_recent,
        )
        for p in pipelines
    ]
