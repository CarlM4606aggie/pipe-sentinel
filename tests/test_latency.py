"""Tests for pipe_sentinel.latency."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest

from pipe_sentinel.latency import (
    LatencyResult,
    _mean_duration,
    detect_latency,
    scan_latency,
)


@dataclass
class _Rec:
    pipeline_name: str
    duration_seconds: Optional[float]


def _recs(name: str, durations):
    return [_Rec(pipeline_name=name, duration_seconds=d) for d in durations]


# ---------------------------------------------------------------------------
# _mean_duration
# ---------------------------------------------------------------------------

def test_mean_duration_empty():
    assert _mean_duration([]) is None


def test_mean_duration_all_none():
    records = [_Rec("p", None), _Rec("p", None)]
    assert _mean_duration(records) is None


def test_mean_duration_mixed_none():
    records = [_Rec("p", None), _Rec("p", 10.0), _Rec("p", 20.0)]
    assert _mean_duration(records) == pytest.approx(15.0)


def test_mean_duration_all_values():
    records = [_Rec("p", 5.0), _Rec("p", 15.0)]
    assert _mean_duration(records) == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# detect_latency
# ---------------------------------------------------------------------------

def test_detect_latency_returns_none_when_no_recent():
    result = detect_latency("p", [], _recs("p", [10.0, 10.0]))
    assert result is None


def test_detect_latency_returns_none_when_no_baseline():
    result = detect_latency("p", _recs("p", [10.0]), [])
    assert result is None


def test_detect_latency_returns_none_when_baseline_zero():
    result = detect_latency("p", _recs("p", [5.0]), _recs("p", [0.0]))
    assert result is None


def test_detect_latency_not_slow_when_ratio_below_threshold():
    recent = _recs("p", [12.0, 13.0])      # mean 12.5
    baseline = _recs("p", [10.0, 10.0])    # mean 10.0  ratio=1.25
    result = detect_latency("p", recent, baseline, threshold=1.5)
    assert result is not None
    assert result.is_slow is False
    assert result.ratio == pytest.approx(1.25)


def test_detect_latency_slow_when_ratio_at_threshold():
    recent = _recs("p", [15.0, 15.0])      # mean 15.0
    baseline = _recs("p", [10.0, 10.0])    # mean 10.0  ratio=1.5
    result = detect_latency("p", recent, baseline, threshold=1.5)
    assert result is not None
    assert result.is_slow is True


def test_detect_latency_slow_when_ratio_above_threshold():
    recent = _recs("p", [30.0])             # mean 30.0
    baseline = _recs("p", [10.0, 10.0])    # mean 10.0  ratio=3.0
    result = detect_latency("p", recent, baseline, threshold=2.0)
    assert result is not None
    assert result.is_slow is True
    assert result.ratio == pytest.approx(3.0)


def test_detect_latency_fields_populated():
    recent = _recs("etl", [20.0])
    baseline = _recs("etl", [10.0])
    result = detect_latency("etl", recent, baseline, threshold=1.5)
    assert result.pipeline_name == "etl"
    assert result.recent_mean == pytest.approx(20.0)
    assert result.baseline_mean == pytest.approx(10.0)
    assert result.threshold == pytest.approx(1.5)


def test_latency_result_str_slow():
    r = LatencyResult("etl", 20.0, 10.0, 2.0, 1.5, True)
    assert "etl" in str(r)
    assert "\u26a0" in str(r)


def test_latency_result_str_ok():
    r = LatencyResult("etl", 10.0, 10.0, 1.0, 1.5, False)
    assert "\u2705" in str(r)


# ---------------------------------------------------------------------------
# scan_latency
# ---------------------------------------------------------------------------

def test_scan_latency_skips_insufficient_records():
    records = _recs("p", [10.0, 10.0, 10.0])  # only 3, recent_window=5
    results = scan_latency(["p"], records, recent_window=5)
    assert results == []


def test_scan_latency_detects_slow_pipeline():
    # 5 baseline runs at 10s, 5 recent runs at 25s
    records = _recs("p", [10.0] * 5 + [25.0] * 5)
    results = scan_latency(["p"], records, recent_window=5, threshold=1.5)
    assert len(results) == 1
    assert results[0].is_slow is True


def test_scan_latency_ignores_unknown_pipeline():
    records = _recs("other", [10.0] * 10)
    results = scan_latency(["p"], records, recent_window=5)
    assert results == []


def test_scan_latency_multiple_pipelines():
    recs_a = _recs("a", [10.0] * 5 + [30.0] * 5)  # slow
    recs_b = _recs("b", [10.0] * 10)               # fine
    results = scan_latency(["a", "b"], recs_a + recs_b, recent_window=5, threshold=1.5)
    slow_names = [r.pipeline_name for r in results if r.is_slow]
    assert "a" in slow_names
    assert "b" not in slow_names
