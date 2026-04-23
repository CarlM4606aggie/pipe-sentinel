"""Tests for pipe_sentinel.triage and pipe_sentinel.triage_report."""
from __future__ import annotations

import pytest

from pipe_sentinel.runner import RunResult
from pipe_sentinel.triage import (
    triage_result,
    triage_all,
    TriageResult,
    SEVERITY_CRITICAL,
    SEVERITY_HIGH,
    SEVERITY_LOW,
    ACTION_PAGE,
    ACTION_NOTIFY,
    ACTION_LOG,
)
from pipe_sentinel.triage_report import (
    format_triage_result,
    build_triage_report,
)


def _run(pipeline="pipe", success=False, timed_out=False, stderr=""):
    return RunResult(
        pipeline=pipeline,
        success=success,
        exit_code=0 if success else 1,
        stdout="",
        stderr=stderr,
        duration=1.0,
        timed_out=timed_out,
        attempts=1,
    )


# ---------------------------------------------------------------------------
# triage_result
# ---------------------------------------------------------------------------

def test_triage_result_returns_none_for_success():
    assert triage_result(_run(success=True)) is None


def test_triage_result_low_severity_single_failure():
    tr = triage_result(_run(), consecutive_failures=1)
    assert tr is not None
    assert tr.severity == SEVERITY_LOW
    assert tr.action == ACTION_LOG


def test_triage_result_high_severity_two_failures():
    tr = triage_result(_run(), consecutive_failures=2)
    assert tr.severity == SEVERITY_HIGH
    assert tr.action == ACTION_NOTIFY


def test_triage_result_critical_severity_three_failures():
    tr = triage_result(_run(), consecutive_failures=3)
    assert tr.severity == SEVERITY_CRITICAL
    assert tr.action == ACTION_PAGE


def test_triage_result_critical_on_timeout():
    tr = triage_result(_run(timed_out=True), consecutive_failures=1)
    assert tr.severity == SEVERITY_CRITICAL
    assert tr.action == ACTION_PAGE


def test_triage_result_pipeline_name_preserved():
    tr = triage_result(_run(pipeline="my_etl"), consecutive_failures=1)
    assert tr.pipeline == "my_etl"


def test_triage_result_str_contains_pipeline_and_severity():
    tr = triage_result(_run(pipeline="etl"), consecutive_failures=3)
    s = str(tr)
    assert "etl" in s
    assert "CRITICAL" in s


# ---------------------------------------------------------------------------
# triage_all
# ---------------------------------------------------------------------------

def test_triage_all_filters_successes():
    results = [_run(success=True), _run(success=False)]
    triaged = triage_all(results)
    assert len(triaged) == 1


def test_triage_all_uses_consecutive_map():
    results = [_run(pipeline="a"), _run(pipeline="b")]
    triaged = triage_all(results, consecutive_map={"a": 3, "b": 1})
    sev_map = {t.pipeline: t.severity for t in triaged}
    assert sev_map["a"] == SEVERITY_CRITICAL
    assert sev_map["b"] == SEVERITY_LOW


def test_triage_all_empty_input():
    assert triage_all([]) == []


# ---------------------------------------------------------------------------
# triage_report
# ---------------------------------------------------------------------------

def test_format_triage_result_contains_pipeline():
    tr = triage_result(_run(pipeline="loader"), consecutive_failures=2)
    out = format_triage_result(tr)
    assert "loader" in out


def test_format_triage_result_contains_severity():
    tr = triage_result(_run(), consecutive_failures=1)
    out = format_triage_result(tr)
    assert "LOW" in out


def test_format_triage_result_shows_stderr_snippet():
    tr = triage_result(_run(stderr="connection refused"), consecutive_failures=1)
    out = format_triage_result(tr)
    assert "connection refused" in out


def test_build_triage_report_empty():
    out = build_triage_report([])
    assert "no failures" in out.lower()


def test_build_triage_report_shows_count():
    results = [triage_result(_run(pipeline=f"p{i}")) for i in range(3)]
    out = build_triage_report(results)
    assert "3 failure" in out


def test_build_triage_report_sections_ordered():
    critical = triage_result(_run(pipeline="c"), consecutive_failures=5)
    low = triage_result(_run(pipeline="l"), consecutive_failures=1)
    out = build_triage_report([low, critical])
    assert out.index("Critical") < out.index("Low")
