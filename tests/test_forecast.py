"""Tests for pipe_sentinel.forecast."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pytest

from pipe_sentinel.forecast import (
    ForecastResult,
    _failure_rate,
    _risk_level,
    forecast_pipeline,
    scan_forecasts,
)


@dataclass
class _Rec:
    pipeline_name: str
    status: str


def _recs(name: str, statuses: List[str]) -> List[_Rec]:
    return [_Rec(pipeline_name=name, status=s) for s in statuses]


# ---------------------------------------------------------------------------
# _failure_rate
# ---------------------------------------------------------------------------

def test_failure_rate_empty():
    assert _failure_rate([]) == 0.0


def test_failure_rate_all_success():
    assert _failure_rate(_recs("p", ["success", "success"])) == 0.0


def test_failure_rate_all_failed():
    assert _failure_rate(_recs("p", ["failure", "failure"])) == 1.0


def test_failure_rate_mixed():
    rate = _failure_rate(_recs("p", ["success", "failure", "failure", "success"]))
    assert rate == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# _risk_level
# ---------------------------------------------------------------------------

def test_risk_level_low():
    assert _risk_level(0.1, 0.05) == "low"


def test_risk_level_medium_by_rate():
    assert _risk_level(0.5, 0.0) == "medium"


def test_risk_level_medium_by_trend():
    assert _risk_level(0.1, 0.2) == "medium"


def test_risk_level_high_by_rate():
    assert _risk_level(0.8, 0.0) == "high"


def test_risk_level_high_by_trend():
    assert _risk_level(0.1, 0.4) == "high"


# ---------------------------------------------------------------------------
# forecast_pipeline
# ---------------------------------------------------------------------------

def test_returns_none_when_insufficient_samples():
    result = forecast_pipeline("etl", _recs("etl", ["success", "failure"]), min_samples=4)
    assert result is None


def test_returns_result_with_correct_name():
    statuses = ["success"] * 4 + ["failure"] * 4
    result = forecast_pipeline("etl", _recs("etl", statuses))
    assert result is not None
    assert result.pipeline_name == "etl"


def test_worsening_trend_detected():
    # baseline: 0 failures, recent: all failures
    statuses = ["success", "success", "failure", "failure"]
    result = forecast_pipeline("etl", _recs("etl", statuses))
    assert result is not None
    assert result.trend > 0


def test_improving_trend_detected():
    statuses = ["failure", "failure", "success", "success"]
    result = forecast_pipeline("etl", _recs("etl", statuses))
    assert result is not None
    assert result.trend < 0


def test_sample_count_matches_input():
    statuses = ["success"] * 8
    result = forecast_pipeline("etl", _recs("etl", statuses))
    assert result is not None
    assert result.sample_count == 8


# ---------------------------------------------------------------------------
# scan_forecasts
# ---------------------------------------------------------------------------

def test_scan_forecasts_skips_insufficient():
    groups = {"short": _recs("short", ["success", "failure"])}
    results = scan_forecasts(groups, min_samples=4)
    assert results == []


def test_scan_forecasts_sorted_by_recent_rate():
    groups = {
        "low_risk": _recs("low_risk", ["success"] * 8),
        "high_risk": _recs("high_risk", ["failure"] * 4 + ["failure"] * 4),
    }
    results = scan_forecasts(groups, min_samples=4)
    assert len(results) == 2
    assert results[0].recent_failure_rate >= results[1].recent_failure_rate


def test_str_contains_pipeline_name():
    statuses = ["success", "success", "failure", "failure"]
    result = forecast_pipeline("my_pipe", _recs("my_pipe", statuses))
    assert result is not None
    assert "my_pipe" in str(result)
