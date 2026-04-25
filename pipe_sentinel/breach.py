"""Breach detection: identify pipelines that have exceeded failure thresholds."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from pipe_sentinel.audit import AuditRecord


@dataclass
class BreachResult:
    pipeline: str
    total_runs: int
    failure_count: int
    failure_rate: float
    threshold: float
    breached: bool

    def __str__(self) -> str:
        status = "BREACH" if self.breached else "OK"
        return (
            f"[{status}] {self.pipeline}: "
            f"{self.failure_count}/{self.total_runs} failures "
            f"({self.failure_rate:.1%} vs threshold {self.threshold:.1%})"
        )


def _failure_rate(records: Sequence[AuditRecord]) -> float:
    if not records:
        return 0.0
    failed = sum(1 for r in records if r.status != "success")
    return failed / len(records)


def detect_breach(
    pipeline: str,
    records: Sequence[AuditRecord],
    threshold: float,
) -> BreachResult:
    """Evaluate whether a pipeline has breached its failure-rate threshold."""
    if not 0.0 <= threshold <= 1.0:
        raise ValueError(f"threshold must be in [0, 1], got {threshold}")
    rate = _failure_rate(records)
    failed = sum(1 for r in records if r.status != "success")
    return BreachResult(
        pipeline=pipeline,
        total_runs=len(records),
        failure_count=failed,
        failure_rate=rate,
        threshold=threshold,
        breached=rate > threshold,
    )


def scan_breaches(
    groups: dict[str, list[AuditRecord]],
    threshold: float,
) -> List[BreachResult]:
    """Scan all pipeline record groups and return breach results."""
    return [
        detect_breach(name, records, threshold)
        for name, records in sorted(groups.items())
    ]
