"""Tests for pipe_sentinel.saturation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pytest

from pipe_sentinel.saturation import (
    SaturationResult,
    _failure_rate,
    detect_saturation,
    scan_saturations,
)


@dataclass
class _Rec:
    success: bool


def _recs(pattern: str) -> List[_Rec]:
    """Build records from a string of 'P' (pass) and 'F' (fail) chars."""
    return [_Rec(success=(c == "P")) for c in pattern]


# --- _failure_rate ---

def test_failure_rate_empty():
    assert _failure_rate([]) == 0.0


def test_failure_rate_all_success():
    assert _failure_rate(_recs("PPP")) == 0.0


def test_failure_rate_all_failed():
    assert _failure_rate(_recs("FFF")) == 1.0


def test_failure_rate_mixed():
    assert _failure_rate(_recs("PPFF")) == pytest.approx(0.5)


# --- detect_saturation ---

def test_detect_saturation_not_saturated_below_threshold():
    records = _recs("PPPF")  # 25% failure rate
    result = detect_saturation("my_pipe", records, threshold=0.5)
    assert not result.saturated
    assert result.failure_rate == pytest.approx(0.25)


def test_detect_saturation_saturated_at_threshold():
    records = _recs("PPFF")  # 50% failure rate
    result = detect_saturation("my_pipe", records, threshold=0.5)
    assert result.saturated


def test_detect_saturation_saturated_above_threshold():
    records = _recs("FFFF")  # 100% failure rate
    result = detect_saturation("my_pipe", records, threshold=0.5)
    assert result.saturated
    assert result.failures == 4
    assert result.total == 4


def test_detect_saturation_empty_records_not_saturated():
    result = detect_saturation("empty_pipe", [], threshold=0.5)
    assert not result.saturated
    assert result.total == 0
    assert result.failures == 0


def test_detect_saturation_stores_pipeline_name():
    result = detect_saturation("alpha", _recs("P"), threshold=0.5)
    assert result.pipeline_name == "alpha"


def test_detect_saturation_stores_window_hours():
    result = detect_saturation("beta", _recs("P"), threshold=0.5, window_hours=12)
    assert result.window_hours == 12


# --- scan_saturations ---

def test_scan_saturations_returns_all_pipelines():
    grouped = {
        "pipe_a": _recs("PPPP"),
        "pipe_b": _recs("FFFF"),
    }
    results = scan_saturations(grouped, threshold=0.5)
    assert len(results) == 2


def test_scan_saturations_sorted_by_failure_rate_descending():
    grouped = {
        "low": _recs("PPPF"),   # 25%
        "high": _recs("FFFF"),  # 100%
        "mid": _recs("PPFF"),   # 50%
    }
    results = scan_saturations(grouped, threshold=0.5)
    rates = [r.failure_rate for r in results]
    assert rates == sorted(rates, reverse=True)


def test_scan_saturations_empty_grouped():
    results = scan_saturations({}, threshold=0.5)
    assert results == []


def test_saturation_result_str_saturated():
    r = SaturationResult(
        pipeline_name="p",
        total=4,
        failures=4,
        failure_rate=1.0,
        threshold=0.5,
        saturated=True,
        window_hours=24,
    )
    assert "SATURATED" in str(r)
    assert "p" in str(r)


def test_saturation_result_str_ok():
    r = SaturationResult(
        pipeline_name="q",
        total=4,
        failures=0,
        failure_rate=0.0,
        threshold=0.5,
        saturated=False,
        window_hours=24,
    )
    assert "ok" in str(r)
