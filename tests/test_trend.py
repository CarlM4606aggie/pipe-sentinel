"""Tests for pipe_sentinel.trend."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pytest

from pipe_sentinel.trend import (
    TrendResult,
    _failure_rate,
    detect_trend,
    scan_trends,
)


@dataclass
class _Rec:
    pipeline: str
    status: str
    timestamp: str


def _recs(pipeline: str, statuses: List[str]) -> List[_Rec]:
    return [
        _Rec(pipeline=pipeline, status=s, timestamp=f"2024-01-{i+1:02d}T00:00:00")
        for i, s in enumerate(statuses)
    ]


def test_failure_rate_empty():
    assert _failure_rate([]) == 0.0


def test_failure_rate_all_success():
    records = _recs("p", ["success"] * 10)
    assert _failure_rate(records) == 0.0


def test_failure_rate_all_failed():
    records = _recs("p", ["failure"] * 4)
    assert _failure_rate(records) == 1.0


def test_failure_rate_mixed():
    records = _recs("p", ["success", "failure", "success", "failure"])
    assert _failure_rate(records) == 0.5


def test_detect_trend_returns_none_insufficient_samples():
    records = _recs("pipe", ["success"] * 10)
    result = detect_trend(records, "pipe", recent_window=10, min_baseline=5)
    assert result is None


def test_detect_trend_worsening():
    # 20 historical successes + 10 recent failures
    historical = _recs("pipe", ["success"] * 20)
    recent = [
        _Rec(pipeline="pipe", status="failure", timestamp=f"2024-02-{i+1:02d}T00:00:00")
        for i in range(10)
    ]
    all_records = historical + recent
    result = detect_trend(all_records, "pipe", recent_window=10, min_baseline=5)
    assert result is not None
    assert result.worsening is True
    assert result.recent_rate == 1.0
    assert result.baseline_rate == 0.0
    assert result.delta == pytest.approx(1.0)


def test_detect_trend_stable():
    records = _recs("pipe", ["success"] * 30)
    result = detect_trend(records, "pipe", recent_window=10, min_baseline=5)
    assert result is not None
    assert result.worsening is False
    assert result.delta == pytest.approx(0.0)


def test_detect_trend_sample_count():
    records = _recs("pipe", ["success"] * 20)
    result = detect_trend(records, "pipe", recent_window=10, min_baseline=5)
    assert result.sample_count == 20


def test_scan_trends_multiple_pipelines():
    a_hist = _recs("a", ["success"] * 20)
    a_recent = [
        _Rec(pipeline="a", status="failure", timestamp=f"2024-02-{i+1:02d}T00:00:00")
        for i in range(10)
    ]
    b = _recs("b", ["success"] * 30)
    results = scan_trends(a_hist + a_recent + b, recent_window=10, min_baseline=5)
    names = {r.pipeline for r in results}
    assert "a" in names
    assert "b" in names
    a_result = next(r for r in results if r.pipeline == "a")
    assert a_result.worsening is True


def test_trend_result_str_worsening():
    r = TrendResult("pipe", 0.8, 0.2, 0.6, True, 30)
    assert "worsening" in str(r)
    assert "pipe" in str(r)


def test_trend_result_str_stable():
    r = TrendResult("pipe", 0.1, 0.2, -0.1, False, 30)
    assert "stable" in str(r)
