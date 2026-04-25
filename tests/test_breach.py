"""Tests for pipe_sentinel.breach."""
from __future__ import annotations

import pytest

from pipe_sentinel.breach import (
    BreachResult,
    _failure_rate,
    detect_breach,
    scan_breaches,
)


class _Rec:
    def __init__(self, status: str):
        self.status = status
        self.pipeline = "test"


def _recs(*statuses: str):
    return [_Rec(s) for s in statuses]


# ---------------------------------------------------------------------------
# _failure_rate
# ---------------------------------------------------------------------------

def test_failure_rate_empty():
    assert _failure_rate([]) == 0.0


def test_failure_rate_all_success():
    assert _failure_rate(_recs("success", "success")) == 0.0


def test_failure_rate_all_failed():
    assert _failure_rate(_recs("failure", "failure")) == 1.0


def test_failure_rate_mixed():
    result = _failure_rate(_recs("success", "failure", "failure", "success"))
    assert result == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# detect_breach
# ---------------------------------------------------------------------------

def test_detect_breach_no_breach():
    records = _recs("success", "success", "failure")
    result = detect_breach("pipe_a", records, threshold=0.5)
    assert isinstance(result, BreachResult)
    assert result.pipeline == "pipe_a"
    assert not result.breached


def test_detect_breach_exactly_at_threshold_is_not_breached():
    records = _recs("success", "failure")
    result = detect_breach("pipe_a", records, threshold=0.5)
    assert not result.breached


def test_detect_breach_above_threshold():
    records = _recs("failure", "failure", "success")
    result = detect_breach("pipe_b", records, threshold=0.5)
    assert result.breached
    assert result.failure_count == 2
    assert result.total_runs == 3


def test_detect_breach_empty_records():
    result = detect_breach("pipe_c", [], threshold=0.3)
    assert not result.breached
    assert result.failure_rate == 0.0


def test_detect_breach_invalid_threshold():
    with pytest.raises(ValueError):
        detect_breach("pipe_d", _recs("success"), threshold=1.5)


def test_detect_breach_threshold_zero_any_failure_breaches():
    records = _recs("success", "failure")
    result = detect_breach("pipe_e", records, threshold=0.0)
    assert result.breached


# ---------------------------------------------------------------------------
# scan_breaches
# ---------------------------------------------------------------------------

def test_scan_breaches_returns_one_per_pipeline():
    groups = {
        "alpha": _recs("success", "success"),
        "beta": _recs("failure", "failure", "failure"),
    }
    results = scan_breaches(groups, threshold=0.5)
    assert len(results) == 2


def test_scan_breaches_sorted_by_name():
    groups = {"zzz": _recs("success"), "aaa": _recs("failure")}
    results = scan_breaches(groups, threshold=0.5)
    assert results[0].pipeline == "aaa"
    assert results[1].pipeline == "zzz"


def test_scan_breaches_empty_groups():
    assert scan_breaches({}, threshold=0.5) == []


def test_breach_result_str_breach():
    r = BreachResult(
        pipeline="my_pipe",
        total_runs=10,
        failure_count=8,
        failure_rate=0.8,
        threshold=0.5,
        breached=True,
    )
    assert "BREACH" in str(r)
    assert "my_pipe" in str(r)


def test_breach_result_str_ok():
    r = BreachResult(
        pipeline="my_pipe",
        total_runs=10,
        failure_count=1,
        failure_rate=0.1,
        threshold=0.5,
        breached=False,
    )
    assert "OK" in str(r)
