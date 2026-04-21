"""Saturation detection — flags pipelines whose failure rate exceeds a
configurable threshold over a rolling window of recent runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence


@dataclass
class SaturationResult:
    pipeline_name: str
    total: int
    failures: int
    failure_rate: float
    threshold: float
    saturated: bool
    window_hours: int

    def __str__(self) -> str:
        status = "SATURATED" if self.saturated else "ok"
        return (
            f"{self.pipeline_name}: {status} "
            f"({self.failures}/{self.total} failures, "
            f"rate={self.failure_rate:.0%}, threshold={self.threshold:.0%})"
        )


def _failure_rate(records: Sequence) -> float:
    if not records:
        return 0.0
    failed = sum(1 for r in records if not r.success)
    return failed / len(records)


def detect_saturation(
    pipeline_name: str,
    records: Sequence,
    threshold: float = 0.5,
    window_hours: int = 24,
) -> SaturationResult:
    """Evaluate saturation for a single pipeline's records."""
    total = len(records)
    failures = sum(1 for r in records if not r.success)
    rate = _failure_rate(records)
    saturated = total > 0 and rate >= threshold
    return SaturationResult(
        pipeline_name=pipeline_name,
        total=total,
        failures=failures,
        failure_rate=rate,
        threshold=threshold,
        saturated=saturated,
        window_hours=window_hours,
    )


def scan_saturations(
    grouped: dict,
    threshold: float = 0.5,
    window_hours: int = 24,
) -> List[SaturationResult]:
    """Scan all pipelines and return saturation results."""
    results = [
        detect_saturation(name, records, threshold, window_hours)
        for name, records in grouped.items()
    ]
    results.sort(key=lambda r: r.failure_rate, reverse=True)
    return results
