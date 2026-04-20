"""Tests for pipe_sentinel.shadow."""
from __future__ import annotations

import pytest

from pipe_sentinel.runner import RunResult
from pipe_sentinel.shadow import (
    ShadowComparison,
    ShadowReport,
    build_shadow_report,
    compare_results,
    print_shadow_report,
)


def _result(name: str, success: bool, duration: float = 1.0) -> RunResult:
    return RunResult(
        pipeline_name=name,
        success=success,
        returncode=0 if success else 1,
        stdout="",
        stderr="" if success else "error",
        duration=duration,
    )


@pytest.fixture()
def live_ok() -> RunResult:
    return _result("etl_daily", success=True, duration=2.0)


@pytest.fixture()
def shadow_ok() -> RunResult:
    return _result("etl_daily", success=True, duration=2.5)


@pytest.fixture()
def shadow_fail() -> RunResult:
    return _result("etl_daily", success=False, duration=1.0)


def test_outcomes_match_when_both_succeed(live_ok, shadow_ok):
    cmp = compare_results("etl_daily", live_ok, shadow_ok)
    assert cmp.outcomes_match is True


def test_outcomes_diverge_when_shadow_fails(live_ok, shadow_fail):
    cmp = compare_results("etl_daily", live_ok, shadow_fail)
    assert cmp.outcomes_match is False


def test_duration_delta_positive(live_ok, shadow_ok):
    cmp = compare_results("etl_daily", live_ok, shadow_ok)
    assert pytest.approx(cmp.duration_delta, abs=1e-6) == 0.5


def test_duration_delta_negative(live_ok, shadow_fail):
    cmp = compare_results("etl_daily", live_ok, shadow_fail)
    assert pytest.approx(cmp.duration_delta, abs=1e-6) == -1.0


def test_str_contains_match_label(live_ok, shadow_ok):
    cmp = compare_results("etl_daily", live_ok, shadow_ok)
    assert "MATCH" in str(cmp)
    assert "etl_daily" in str(cmp)


def test_str_contains_diverge_label(live_ok, shadow_fail):
    cmp = compare_results("etl_daily", live_ok, shadow_fail)
    assert "DIVERGE" in str(cmp)


def test_shadow_report_all_match(live_ok, shadow_ok):
    cmp = compare_results("etl_daily", live_ok, shadow_ok)
    report = build_shadow_report([cmp])
    assert report.all_match is True
    assert report.divergence_count == 0
    assert report.total == 1


def test_shadow_report_detects_divergence(live_ok, shadow_fail):
    cmp = compare_results("etl_daily", live_ok, shadow_fail)
    report = build_shadow_report([cmp])
    assert report.all_match is False
    assert report.divergence_count == 1
    assert len(report.divergences) == 1


def test_shadow_report_empty():
    report = build_shadow_report([])
    assert report.total == 0
    assert report.all_match is True


def test_print_shadow_report_runs(live_ok, shadow_ok, shadow_fail, capsys):
    c1 = compare_results("pipe_a", live_ok, shadow_ok)
    c2 = compare_results("pipe_b", live_ok, shadow_fail)
    report = build_shadow_report([c1, c2])
    print_shadow_report(report)
    captured = capsys.readouterr().out
    assert "Shadow Report" in captured
    assert "pipe_a" in captured
    assert "pipe_b" in captured
    assert "Divergences" in captured
