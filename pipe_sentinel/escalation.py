"""Escalation policy: promote alert to a higher-priority recipient list
after a pipeline has failed consecutively N or more times."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipe_sentinel.config import PipelineConfig
from pipe_sentinel.pipeline_status import PipelineStatus


@dataclass
class EscalationPolicy:
    """Defines when and to whom alerts should be escalated."""
    threshold: int  # consecutive failures required to escalate
    recipients: List[str]  # escalation email addresses


@dataclass
class EscalationDecision:
    pipeline_name: str
    should_escalate: bool
    consecutive_failures: int
    recipients: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        if self.should_escalate:
            return (
                f"[ESCALATE] {self.pipeline_name} — "
                f"{self.consecutive_failures} consecutive failures → "
                f"{', '.join(self.recipients)}"
            )
        return (
            f"[OK] {self.pipeline_name} — "
            f"{self.consecutive_failures} consecutive failures (below threshold)"
        )


def _parse_escalation(pipeline: PipelineConfig) -> Optional[EscalationPolicy]:
    """Extract escalation policy from pipeline extras, if configured."""
    extras = getattr(pipeline, "extras", {}) or {}
    esc = extras.get("escalation")
    if not esc:
        return None
    threshold = int(esc.get("threshold", 3))
    recipients = esc.get("recipients", [])
    if not recipients:
        return None
    return EscalationPolicy(threshold=threshold, recipients=list(recipients))


def evaluate_escalation(
    status: PipelineStatus,
    pipeline: PipelineConfig,
) -> EscalationDecision:
    """Decide whether the pipeline's failure streak warrants escalation."""
    policy = _parse_escalation(pipeline)
    consecutive = status.consecutive_failures

    if policy is None or consecutive < policy.threshold:
        return EscalationDecision(
            pipeline_name=pipeline.name,
            should_escalate=False,
            consecutive_failures=consecutive,
        )

    return EscalationDecision(
        pipeline_name=pipeline.name,
        should_escalate=True,
        consecutive_failures=consecutive,
        recipients=policy.recipients,
    )


def evaluate_all(
    statuses: List[PipelineStatus],
    pipelines: List[PipelineConfig],
) -> List[EscalationDecision]:
    """Evaluate escalation for every pipeline that has a policy defined."""
    pipeline_map = {p.name: p for p in pipelines}
    decisions: List[EscalationDecision] = []
    for status in statuses:
        pipeline = pipeline_map.get(status.pipeline_name)
        if pipeline is None:
            continue
        decisions.append(evaluate_escalation(status, pipeline))
    return decisions
