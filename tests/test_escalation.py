"""Tests for escalation policy evaluation and report formatting."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import pytest

from pipe_sentinel.escalation import (
    EscalationDecision,
    EscalationPolicy,
    evaluate_escalation,
    evaluate_all,
)
from pipe_sentinel.escalation_report import (
    format_decision,
    build_escalation_report,
)
from pipe_sentinel.pipeline_status import PipelineStatus


# ---------------------------------------------------------------------------
# Minimal stubs
# ---------------------------------------------------------------------------

@dataclass
class _FakePipeline:
    name: str
    extras: dict = field(default_factory=dict)


def _status(name: str, consecutive: int) -> PipelineStatus:
    return PipelineStatus(
        pipeline_name=name,
        last_status="failure" if consecutive > 0 else "success",
        consecutive_failures=consecutive,
        last_run_ts=None,
        is_healthy=consecutive == 0,
    )


def _pipeline_with_escalation(name: str, threshold: int, recipients: List[str]):
    return _FakePipeline(
        name=name,
        extras={"escalation": {"threshold": threshold, "recipients": recipients}},
    )


def _pipeline_no_escalation(name: str):
    return _FakePipeline(name=name, extras={})


# ---------------------------------------------------------------------------
# evaluate_escalation
# ---------------------------------------------------------------------------

def test_no_policy_never_escalates():
    pipeline = _pipeline_no_escalation("etl_load")
    status = _status("etl_load", consecutive=10)
    decision = evaluate_escalation(status, pipeline)  # type: ignore[arg-type]
    assert decision.should_escalate is False


def test_below_threshold_does_not_escalate():
    pipeline = _pipeline_with_escalation("etl_load", threshold=3, recipients=["ops@example.com"])
    status = _status("etl_load", consecutive=2)
    decision = evaluate_escalation(status, pipeline)  # type: ignore[arg-type]
    assert decision.should_escalate is False
    assert decision.consecutive_failures == 2


def test_at_threshold_escalates():
    pipeline = _pipeline_with_escalation("etl_load", threshold=3, recipients=["ops@example.com"])
    status = _status("etl_load", consecutive=3)
    decision = evaluate_escalation(status, pipeline)  # type: ignore[arg-type]
    assert decision.should_escalate is True
    assert decision.recipients == ["ops@example.com"]


def test_above_threshold_escalates():
    pipeline = _pipeline_with_escalation("etl_load", threshold=2, recipients=["a@b.com", "c@d.com"])
    status = _status("etl_load", consecutive=5)
    decision = evaluate_escalation(status, pipeline)  # type: ignore[arg-type]
    assert decision.should_escalate is True
    assert len(decision.recipients) == 2


def test_empty_recipients_no_escalation():
    pipeline = _FakePipeline(
        name="etl_load",
        extras={"escalation": {"threshold": 1, "recipients": []}},
    )
    status = _status("etl_load", consecutive=5)
    decision = evaluate_escalation(status, pipeline)  # type: ignore[arg-type]
    assert decision.should_escalate is False


# ---------------------------------------------------------------------------
# evaluate_all
# ---------------------------------------------------------------------------

def test_evaluate_all_skips_unknown_pipelines():
    statuses = [_status("ghost", consecutive=5)]
    pipelines = [_pipeline_no_escalation("other")]
    decisions = evaluate_all(statuses, pipelines)  # type: ignore[arg-type]
    assert decisions == []


def test_evaluate_all_returns_one_per_status():
    statuses = [
        _status("a", consecutive=4),
        _status("b", consecutive=1),
    ]
    pipelines = [
        _pipeline_with_escalation("a", threshold=3, recipients=["x@y.com"]),
        _pipeline_no_escalation("b"),
    ]
    decisions = evaluate_all(statuses, pipelines)  # type: ignore[arg-type]
    assert len(decisions) == 2
    assert decisions[0].should_escalate is True
    assert decisions[1].should_escalate is False


# ---------------------------------------------------------------------------
# EscalationDecision.__str__
# ---------------------------------------------------------------------------

def test_str_escalated():
    d = EscalationDecision(
        pipeline_name="etl", should_escalate=True,
        consecutive_failures=4, recipients=["mgr@co.com"]
    )
    assert "ESCALATE" in str(d)
    assert "mgr@co.com" in str(d)


def test_str_not_escalated():
    d = EscalationDecision(
        pipeline_name="etl", should_escalate=False,
        consecutive_failures=1, recipients=[]
    )
    assert "OK" in str(d)


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def test_format_decision_escalated_contains_icon():
    d = EscalationDecision(
        pipeline_name="p", should_escalate=True,
        consecutive_failures=3, recipients=["a@b.com"]
    )
    text = format_decision(d)
    assert "🚨" in text
    assert "a@b.com" in text


def test_format_decision_ok_contains_icon():
    d = EscalationDecision(
        pipeline_name="p", should_escalate=False,
        consecutive_failures=0, recipients=[]
    )
    assert "✅" in format_decision(d)


def test_build_escalation_report_empty():
    report = build_escalation_report([])
    assert "No escalation data" in report


def test_build_escalation_report_counts():
    decisions = [
        EscalationDecision("a", True, 4, ["x@y.com"]),
        EscalationDecision("b", False, 1, []),
    ]
    report = build_escalation_report(decisions)
    assert "Escalated           : 1" in report
    assert "Pipelines evaluated : 2" in report
