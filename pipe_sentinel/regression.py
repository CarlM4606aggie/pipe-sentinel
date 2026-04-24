"""Regression detection: flag pipelines whose failure rate has worsened
compared to a historical baseline window."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence


@dataclass
class RegressionResult:
    pipeline: str
    baseline_rate: float   # failure rate in the reference window
    current_rate: float    # failure rate in the recent window
    delta: float           # current_rate - baseline_rate
    is_regression: bool

    def __str__(self) -> str:
        direction = "▲" if self.is_regression else "▼"
        return (
            f"{self.pipeline}: baseline={self.baseline_rate:.1%} "
            f"current={self.current_rate:.1%} {direction} delta={self.delta:+.1%}"
        )


def _failure_rate(records: Sequence) -> float:
    if not records:
        return 0.0
    return sum(1 for r in records if not r.success) / len(records)


def detect_regression(
    pipeline: str,
    baseline_records: Sequence,
    current_records: Sequence,
    min_delta: float = 0.10,
) -> RegressionResult:
    """Compare failure rates between two windows.

    A regression is flagged when the current failure rate exceeds the
    baseline failure rate by at least *min_delta* (default 10 pp).
    """
    baseline_rate = _failure_rate(baseline_records)
    current_rate = _failure_rate(current_records)
    delta = current_rate - baseline_rate
    return RegressionResult(
        pipeline=pipeline,
        baseline_rate=baseline_rate,
        current_rate=current_rate,
        delta=delta,
        is_regression=delta >= min_delta,
    )


def scan_regressions(
    groups: dict,
    min_delta: float = 0.10,
) -> List[RegressionResult]:
    """Run regression detection for every pipeline in *groups*.

    *groups* maps pipeline name -> (baseline_records, current_records).
    Returns only pipelines that show a regression.
    """
    results = []
    for name, (baseline, current) in groups.items():
        result = detect_regression(name, baseline, current, min_delta)
        if result.is_regression:
            results.append(result)
    return results
