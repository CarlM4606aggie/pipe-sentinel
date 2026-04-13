"""Formatting helpers for escalation decisions."""
from __future__ import annotations

from typing import List

from pipe_sentinel.escalation import EscalationDecision

_ESCALATE_ICON = "🚨"
_OK_ICON = "✅"


def format_decision(decision: EscalationDecision) -> str:
    if decision.should_escalate:
        recipients_str = ", ".join(decision.recipients)
        return (
            f"{_ESCALATE_ICON}  {decision.pipeline_name}: "
            f"{decision.consecutive_failures} consecutive failures — "
            f"escalating to {recipients_str}"
        )
    return (
        f"{_OK_ICON}  {decision.pipeline_name}: "
        f"{decision.consecutive_failures} consecutive failures (no escalation)"
    )


def build_escalation_report(decisions: List[EscalationDecision]) -> str:
    if not decisions:
        return "No escalation data available."

    lines = ["=== Escalation Report ==="]
    escalated = [d for d in decisions if d.should_escalate]
    lines.append(f"Pipelines evaluated : {len(decisions)}")
    lines.append(f"Escalated           : {len(escalated)}")
    lines.append("")
    for decision in decisions:
        lines.append(format_decision(decision))
    return "\n".join(lines)


def print_escalation_report(decisions: List[EscalationDecision]) -> None:
    print(build_escalation_report(decisions))
