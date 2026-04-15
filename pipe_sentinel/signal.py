"""signal.py – pipeline run signal aggregation.

A Signal summarises the overall health of a single pipeline by combining
its most-recent run result, consecutive-failure count, and whether it is
currently suppressed or paused.  Consumers (CLI, notifier) can query a
SignalReport to decide whether action is needed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipe_sentinel.runner import RunResult


@dataclass
class PipelineSignal:
    """Aggregated signal for one pipeline."""

    pipeline_name: str
    last_result: Optional[RunResult]
    consecutive_failures: int = 0
    suppressed: bool = False
    paused: bool = False

    @property
    def needs_alert(self) -> bool:
        """True when the pipeline has failed and is not suppressed/paused."""
        if self.suppressed or self.paused:
            return False
        if self.last_result is None:
            return False
        return not self.last_result.success

    @property
    def severity(self) -> str:
        """Return 'critical', 'warning', or 'ok' based on failure depth."""
        if not self.needs_alert:
            return "ok"
        if self.consecutive_failures >= 3:
            return "critical"
        return "warning"

    def __str__(self) -> str:
        icon = {"ok": "✓", "warning": "!", "critical": "✗"}.get(self.severity, "?")
        return (
            f"{icon} {self.pipeline_name} "
            f"[{self.severity}] "
            f"failures={self.consecutive_failures} "
            f"suppressed={self.suppressed} "
            f"paused={self.paused}"
        )


@dataclass
class SignalReport:
    """Collection of signals for all monitored pipelines."""

    signals: List[PipelineSignal] = field(default_factory=list)

    @property
    def alerts(self) -> List[PipelineSignal]:
        return [s for s in self.signals if s.needs_alert]

    @property
    def critical(self) -> List[PipelineSignal]:
        return [s for s in self.signals if s.severity == "critical"]

    @property
    def all_ok(self) -> bool:
        return len(self.alerts) == 0

    def summary(self) -> str:
        total = len(self.signals)
        alert_count = len(self.alerts)
        crit_count = len(self.critical)
        return (
            f"SignalReport: {total} pipeline(s), "
            f"{alert_count} alert(s), "
            f"{crit_count} critical"
        )


def build_signal_report(
    results: List[RunResult],
    suppressed_names: Optional[List[str]] = None,
    paused_names: Optional[List[str]] = None,
) -> SignalReport:
    """Build a SignalReport from a list of RunResults.

    consecutive_failures is derived by counting trailing failures in
    *results* (ordered oldest-first) for each pipeline name.
    """
    suppressed_names = suppressed_names or []
    paused_names = paused_names or []

    # Group results by pipeline name (preserve insertion order)
    grouped: dict[str, List[RunResult]] = {}
    for r in results:
        grouped.setdefault(r.pipeline_name, []).append(r)

    signals: List[PipelineSignal] = []
    for name, runs in grouped.items():
        last = runs[-1]
        consec = 0
        for r in reversed(runs):
            if not r.success:
                consec += 1
            else:
                break
        signals.append(
            PipelineSignal(
                pipeline_name=name,
                last_result=last,
                consecutive_failures=consec,
                suppressed=name in suppressed_names,
                paused=name in paused_names,
            )
        )
    return SignalReport(signals=signals)
