"""Correlation: detect pipelines that tend to fail together."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Sequence, Tuple

from pipe_sentinel.audit import AuditRecord


@dataclass
class CorrelationPair:
    pipeline_a: str
    pipeline_b: str
    co_failures: int
    total_windows: int

    @property
    def rate(self) -> float:
        if self.total_windows == 0:
            return 0.0
        return self.co_failures / self.total_windows

    def __str__(self) -> str:
        pct = self.rate * 100
        return (
            f"{self.pipeline_a} <-> {self.pipeline_b}: "
            f"{self.co_failures}/{self.total_windows} co-failures ({pct:.1f}%)"
        )


@dataclass
class CorrelationReport:
    pairs: List[CorrelationPair] = field(default_factory=list)
    threshold: float = 0.5

    @property
    def significant(self) -> List[CorrelationPair]:
        return [p for p in self.pairs if p.rate >= self.threshold]


def _group_by_window(
    records: Sequence[AuditRecord], window_seconds: int
) -> List[List[AuditRecord]]:
    """Bucket records into fixed-width time windows."""
    if not records:
        return []
    sorted_recs = sorted(records, key=lambda r: r.started_at)
    windows: List[List[AuditRecord]] = []
    current: List[AuditRecord] = []
    base = sorted_recs[0].started_at
    for rec in sorted_recs:
        if rec.started_at - base <= window_seconds:
            current.append(rec)
        else:
            windows.append(current)
            current = [rec]
            base = rec.started_at
    if current:
        windows.append(current)
    return windows


def _failed_names(window: List[AuditRecord]) -> List[str]:
    return [r.pipeline for r in window if r.status == "failure"]


def detect_correlations(
    records: Sequence[AuditRecord],
    window_seconds: int = 300,
    threshold: float = 0.5,
) -> CorrelationReport:
    """Find pipeline pairs that co-fail frequently within time windows."""
    windows = _group_by_window(records, window_seconds)
    co_counts: Dict[Tuple[str, str], int] = {}
    total_windows = len(windows)

    for window in windows:
        failed = sorted(set(_failed_names(window)))
        for i, a in enumerate(failed):
            for b in failed[i + 1 :]:
                key = (a, b)
                co_counts[key] = co_counts.get(key, 0) + 1

    pairs = [
        CorrelationPair(
            pipeline_a=a,
            pipeline_b=b,
            co_failures=count,
            total_windows=total_windows,
        )
        for (a, b), count in sorted(co_counts.items(), key=lambda x: -x[1])
    ]
    return CorrelationReport(pairs=pairs, threshold=threshold)
