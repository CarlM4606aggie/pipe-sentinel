"""Tests for pipe_sentinel.regression."""
from __future__ import annotations

import pytest

from pipe_sentinel.regression import (
    RegressionResult,
    _failure_rate,
    detect_regression,
    scan_regressions,
)


class _Rec:
    def __init__(self, success: bool):
        self.success = success


def _recs(pattern: str) -> list:
    """Build records from a string of 'P' (pass) and 'F' (fail)."""
    return [_Rec(c == "P") for c in pattern]


# ── _failure_rate ────────────────────────────────────────────────────────────

def test_failure_rate_empty():
    assert _failure_rate([]) == 0.0


def test_failure_rate_all_success():
    assert _failure_rate(_recs("PPP")) == 0.0


def test_failure_rate_all_failed():
    assert _failure_rate(_recs("FFF")) == 1.0


def test_failure_rate_mixed():
    result = _failure_rate(_recs("PPFF"))
    assert result == pytest.approx(0.5)


# ── detect_regression ────────────────────────────────────────────────────────

def test_no_regression_when_rate_stable():
    baseline = _recs("PPPP")
    current = _recs("PPPP")
    r = detect_regression("pipe", baseline, current)
    assert not r.is_regression
    assert r.delta == pytest.approx(0.0)


def test_regression_when_rate_worsens():
    baseline = _recs("PPPPPPPPPP")   # 0 % failure
    current = _recs("FFFFFFFFF F".replace(" ", ""))  # high failure
    r = detect_regression("pipe", baseline, current, min_delta=0.10)
    assert r.is_regression
    assert r.delta > 0


def test_no_regression_below_min_delta():
    baseline = _recs("PPPPPPPPF")   # ~11 %
    current = _recs("PPPPPPFF")     # 25 %
    r = detect_regression("pipe", baseline, current, min_delta=0.50)
    assert not r.is_regression


def test_regression_result_fields():
    baseline = _recs("PPPP")
    current = _recs("FF")
    r = detect_regression("etl", baseline, current, min_delta=0.10)
    assert r.pipeline == "etl"
    assert r.baseline_rate == pytest.approx(0.0)
    assert r.current_rate == pytest.approx(1.0)
    assert r.delta == pytest.approx(1.0)


def test_str_contains_pipeline_name():
    r = RegressionResult(
        pipeline="my_pipe",
        baseline_rate=0.1,
        current_rate=0.4,
        delta=0.3,
        is_regression=True,
    )
    assert "my_pipe" in str(r)


# ── scan_regressions ─────────────────────────────────────────────────────────

def test_scan_returns_only_regressions():
    groups = {
        "good": (_recs("PPPP"), _recs("PPPP")),
        "bad": (_recs("PPPP"), _recs("FFFF")),
    }
    results = scan_regressions(groups, min_delta=0.10)
    names = [r.pipeline for r in results]
    assert "bad" in names
    assert "good" not in names


def test_scan_empty_groups_returns_empty():
    assert scan_regressions({}) == []


def test_scan_all_stable_returns_empty():
    groups = {
        "a": (_recs("PPPP"), _recs("PPPP")),
        "b": (_recs("FF"), _recs("FF")),
    }
    assert scan_regressions(groups, min_delta=0.10) == []
