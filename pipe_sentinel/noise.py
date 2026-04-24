"""Noise detection for pipe-sentinel.

Identifies pipelines that are 'noisy' — frequently alternating between
passing and failing without sustained stable periods.  A noisy pipeline
produces alert fatigue and should be reviewed or suppressed.

A pipeline is considered noisy when its transition rate (state changes
per run) exceeds a configurable threshold over a recent window of runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from pipe_sentinel.audit import AuditRecord

# Default window: last 20 runs per pipeline.
_DEFAULT_WINDOW = 20
# Default threshold: more than 40 % of runs are state transitions.
_DEFAULT_THRESHOLD = 0.40


@dataclass(frozen=True)
class NoiseResult:
    """Noise analysis result for a single pipeline."""

    pipeline_name: str
    total_runs: int
    transitions: int
    transition_rate: float  # transitions / (total_runs - 1), or 0 when < 2 runs
    threshold: float
    is_noisy: bool

    def __str__(self) -> str:  # pragma: no cover
        symbol = "\u26a0\ufe0f" if self.is_noisy else "\u2705"
        return (
            f"{symbol} {self.pipeline_name}: "
            f"{self.transitions} transitions / {self.total_runs} runs "
            f"(rate={self.transition_rate:.0%}, threshold={self.threshold:.0%})"
        )


def _count_transitions(statuses: List[bool]) -> int:
    """Return the number of times the pass/fail status changes between consecutive runs."""
    if len(statuses) < 2:
        return 0
    return sum(
        1 for a, b in zip(statuses, statuses[1:]) if a != b
    )


def _transition_rate(transitions: int, total_runs: int) -> float:
    """Compute transition rate; returns 0.0 when fewer than two runs are available."""
    if total_runs < 2:
        return 0.0
    return transitions / (total_runs - 1)


def detect_noise(
    records: Sequence[AuditRecord],
    pipeline_name: str,
    *,
    window: int = _DEFAULT_WINDOW,
    threshold: float = _DEFAULT_THRESHOLD,
) -> NoiseResult:
    """Analyse recent records for *pipeline_name* and return a :class:`NoiseResult`.

    Parameters
    ----------
    records:
        All audit records for the pipeline, ordered oldest-first.
    pipeline_name:
        The name of the pipeline being evaluated.
    window:
        How many of the most-recent runs to consider.
    threshold:
        Fraction of run-pairs that must be transitions to flag as noisy.
    """
    recent = [r for r in records if r.pipeline_name == pipeline_name][-window:]
    statuses = [r.success for r in recent]
    total = len(statuses)
    transitions = _count_transitions(statuses)
    rate = _transition_rate(transitions, total)
    return NoiseResult(
        pipeline_name=pipeline_name,
        total_runs=total,
        transitions=transitions,
        transition_rate=rate,
        threshold=threshold,
        is_noisy=rate > threshold,
    )


def scan_noise(
    records: Sequence[AuditRecord],
    pipeline_names: Sequence[str],
    *,
    window: int = _DEFAULT_WINDOW,
    threshold: float = _DEFAULT_THRESHOLD,
) -> List[NoiseResult]:
    """Run noise detection across all *pipeline_names*.

    Returns one :class:`NoiseResult` per pipeline, sorted by
    transition rate descending so the noisiest pipelines appear first.
    """
    results = [
        detect_noise(records, name, window=window, threshold=threshold)
        for name in pipeline_names
    ]
    return sorted(results, key=lambda r: r.transition_rate, reverse=True)
