"""Triage module: classify pipeline failures by severity and suggested action."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipe_sentinel.runner import RunResult


SEVERITY_CRITICAL = "critical"
SEVERITY_HIGH = "high"
SEVERITY_LOW = "low"

ACTION_PAGE = "page"
ACTION_NOTIFY = "notify"
ACTION_LOG = "log"


@dataclass
class TriageResult:
    pipeline: str
    severity: str
    action: str
    reason: str
    result: RunResult

    def __str__(self) -> str:
        return (
            f"[{self.severity.upper()}] {self.pipeline} "
            f"-> {self.action}: {self.reason}"
        )


def _classify_severity(result: RunResult, consecutive_failures: int) -> str:
    """Determine severity based on exit code, timeout, and failure streak."""
    if result.timed_out:
        return SEVERITY_CRITICAL
    if consecutive_failures >= 3:
        return SEVERITY_CRITICAL
    if consecutive_failures >= 2:
        return SEVERITY_HIGH
    return SEVERITY_LOW


def _choose_action(severity: str) -> str:
    if severity == SEVERITY_CRITICAL:
        return ACTION_PAGE
    if severity == SEVERITY_HIGH:
        return ACTION_NOTIFY
    return ACTION_LOG


def _build_reason(result: RunResult, consecutive_failures: int) -> str:
    if result.timed_out:
        return "pipeline exceeded its timeout limit"
    if consecutive_failures >= 3:
        return f"{consecutive_failures} consecutive failures detected"
    if consecutive_failures >= 2:
        return "repeated failure on last two runs"
    return "single failure, monitoring"


def triage_result(
    result: RunResult,
    consecutive_failures: int = 1,
) -> Optional[TriageResult]:
    """Return a TriageResult for a failed run, or None if the run succeeded."""
    if result.success:
        return None
    severity = _classify_severity(result, consecutive_failures)
    action = _choose_action(severity)
    reason = _build_reason(result, consecutive_failures)
    return TriageResult(
        pipeline=result.pipeline,
        severity=severity,
        action=action,
        reason=reason,
        result=result,
    )


def triage_all(
    results: List[RunResult],
    consecutive_map: Optional[dict] = None,
) -> List[TriageResult]:
    """Triage a list of run results, returning only actionable entries."""
    consecutive_map = consecutive_map or {}
    triaged = []
    for r in results:
        consec = consecutive_map.get(r.pipeline, 1)
        tr = triage_result(r, consec)
        if tr is not None:
            triaged.append(tr)
    return triaged
