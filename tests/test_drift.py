"""Tests for pipe_sentinel.drift and pipe_sentinel.drift_report."""
from __future__ import annotations

from typing import List

import pytest

from pipe_sentinel.drift import (
    DriftResult,
    _success_rate,
    detect_drift,
    scan_drift,
)
from pipe_sentinel.drift_report import (
    build_drift_report,
    format_drift_result,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _Rec:
    """Minimal stand-in for AuditRecord."""
    def __init__(self, status: str):
        self.status = status


def _recs(statuses: List[str]):
    return [_Rec(s) for s in statuses]


# ---------------------------------------------------------------------------
# _success_rate
# ---------------------------------------------------------------------------

def test_success_rate_empty():
    assert _success_rate([]) == 1.0


def test_success_rate_all_success():
    assert _success_rate(_recs(["success", "success"])) == 1.0


def test_success_rate_all_failure():
    assert _success_rate(_recs(["failure", "failure"])) == 0.0


def test_success_rate_mixed():
    assert _success_rate(_recs(["success", "failure", "success", "failure"])) == 0.5


# ---------------------------------------------------------------------------
# detect_drift
# ---------------------------------------------------------------------------

def test_detect_drift_not_drifting_when_rates_equal():
    hist = _recs(["success"] * 10)
    rec = _recs(["success"] * 5)
    result = detect_drift("pipe_a", hist, rec, threshold=0.15)
    assert not result.is_drifting
    assert result.delta == pytest.approx(0.0)


def test_detect_drift_flagged_when_large_drop():
    hist = _recs(["success"] * 10)
    rec = _recs(["failure"] * 5 + ["success"] * 5)
    result = detect_drift("pipe_b", hist, rec, threshold=0.15)
    assert result.is_drifting
    assert result.delta == pytest.approx(-0.5)


def test_detect_drift_not_flagged_when_drop_below_threshold():
    hist = _recs(["success"] * 10)
    rec = _recs(["failure"] * 1 + ["success"] * 9)
    result = detect_drift("pipe_c", hist, rec, threshold=0.15)
    assert not result.is_drifting


def test_detect_drift_stores_pipeline_name():
    result = detect_drift("my_pipe", [], [], threshold=0.1)
    assert result.pipeline_name == "my_pipe"


# ---------------------------------------------------------------------------
# scan_drift
# ---------------------------------------------------------------------------

def test_scan_drift_returns_one_result_per_name():
    names = ["a", "b", "c"]
    results = scan_drift(names, {}, {}, threshold=0.1)
    assert len(results) == 3
    assert {r.pipeline_name for r in results} == set(names)


def test_scan_drift_missing_pipeline_treated_as_perfect():
    results = scan_drift(["ghost"], {}, {}, threshold=0.1)
    assert results[0].historical_rate == 1.0
    assert results[0].recent_rate == 1.0


# ---------------------------------------------------------------------------
# drift_report
# ---------------------------------------------------------------------------

def _make_result(name: str, hist: float, rec: float) -> DriftResult:
    return DriftResult(
        pipeline_name=name,
        historical_rate=hist,
        recent_rate=rec,
        delta=rec - hist,
        threshold=0.15,
    )


def test_format_drift_result_contains_pipeline_name():
    r = _make_result("etl_load", 1.0, 0.6)
    assert "etl_load" in format_drift_result(r)


def test_format_drift_result_shows_warning_icon_when_drifting():
    r = _make_result("etl_load", 1.0, 0.6)
    assert "⚠" in format_drift_result(r)


def test_format_drift_result_shows_ok_icon_when_stable():
    r = _make_result("etl_load", 0.8, 0.8)
    assert "✓" in format_drift_result(r)


def test_build_drift_report_empty():
    report = build_drift_report([])
    assert "no pipelines" in report


def test_build_drift_report_counts_drifting():
    results = [
        _make_result("a", 1.0, 0.5),
        _make_result("b", 1.0, 1.0),
    ]
    report = build_drift_report(results)
    assert "Drifting          : 1" in report


def test_build_drift_report_contains_all_names():
    results = [_make_result("pipe_x", 1.0, 0.8), _make_result("pipe_y", 0.9, 0.9)]
    report = build_drift_report(results)
    assert "pipe_x" in report
    assert "pipe_y" in report
