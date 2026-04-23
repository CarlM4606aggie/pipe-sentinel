"""Formatting helpers for triage results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.triage import (
    TriageResult,
    SEVERITY_CRITICAL,
    SEVERITY_HIGH,
    ACTION_PAGE,
    ACTION_NOTIFY,
)


_SEVERITY_ICON = {
    SEVERITY_CRITICAL: "🔴",
    SEVERITY_HIGH: "🟠",
    "low": "🟡",
}

_ACTION_LABEL = {
    ACTION_PAGE: "[PAGE ON-CALL]",
    ACTION_NOTIFY: "[NOTIFY TEAM]",
    "log": "[LOG ONLY]",
}


def _icon(severity: str) -> str:
    return _SEVERITY_ICON.get(severity, "⚪")


def format_triage_result(tr: TriageResult) -> str:
    icon = _icon(tr.severity)
    label = _ACTION_LABEL.get(tr.action, tr.action.upper())
    lines = [
        f"{icon} {tr.pipeline}",
        f"  Severity : {tr.severity.upper()}",
        f"  Action   : {label}",
        f"  Reason   : {tr.reason}",
    ]
    if tr.result.stderr:
        snippet = tr.result.stderr.strip().splitlines()[-1][:120]
        lines.append(f"  Last err : {snippet}")
    return "\n".join(lines)


def build_triage_report(results: List[TriageResult]) -> str:
    if not results:
        return "Triage: no failures to report."
    critical = [r for r in results if r.severity == SEVERITY_CRITICAL]
    high = [r for r in results if r.severity == SEVERITY_HIGH]
    low = [r for r in results if r.severity not in (SEVERITY_CRITICAL, SEVERITY_HIGH)]
    sections: List[str] = []
    header = f"Triage Report  ({len(results)} failure(s))"
    sections.append(header)
    sections.append("=" * len(header))
    for group, label in [
        (critical, "Critical"),
        (high, "High"),
        (low, "Low"),
    ]:
        if group:
            sections.append(f"\n--- {label} ---")
            for tr in group:
                sections.append(format_triage_result(tr))
    return "\n".join(sections)


def print_triage_report(results: List[TriageResult]) -> None:
    print(build_triage_report(results))
