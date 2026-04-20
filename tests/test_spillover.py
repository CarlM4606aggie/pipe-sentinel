"""Tests for pipe_sentinel.spillover and pipe_sentinel.spillover_report."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest

from pipe_sentinel.spillover import (
    SpilloverResult,
    _mean_duration,
    detect_spillover,
    scan_spillovers,
)
from pipe_sentinel.spillover_report import (
    build_spillover_report,
    format_spillover_result,
)


@dataclass
class _Rec:
    pipeline_name: str
    duration: Optional[float]


@dataclass
class _FakePipeline:
    name: str
    scheduled_duration: Optional[float] = None


def _recs(name: str, durations):
    return [_Rec(pipeline_name=name, duration=d) for d in durations]


# ---------------------------------------------------------------------------
# _mean_duration
# ---------------------------------------------------------------------------

def test_mean_duration_empty():
    assert _mean_duration([]) is None


def test_mean_duration_all_none():
    records = [_Rec("p", None), _Rec("p", None)]
    assert _mean_duration(records) is None


def test_mean_duration_mixed():
    records = [_Rec("p", 10.0), _Rec("p", None), _Rec("p", 20.0)]
    assert _mean_duration(records) == pytest.approx(15.0)


def test_mean_duration_uniform():
    records = [_Rec("p", 5.0)] * 4
    assert _mean_duration(records) == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# detect_spillover
# ---------------------------------------------------------------------------

def test_detect_spillover_returns_none_insufficient_samples():
    records = _recs("pipe", [30.0, 35.0])  # only 2, min_samples=3
    result = detect_spillover("pipe", 25.0, records, min_samples=3)
    assert result is None


def test_detect_spillover_is_spilling():
    records = _recs("pipe", [40.0, 45.0, 50.0])
    result = detect_spillover("pipe", 30.0, records)
    assert result is not None
    assert result.is_spilling is True
    assert result.spillover_seconds == pytest.approx(15.0)
    assert result.sample_count == 3


def test_detect_spillover_within_schedule():
    records = _recs("pipe", [10.0, 12.0, 11.0])
    result = detect_spillover("pipe", 20.0, records)
    assert result is not None
    assert result.is_spilling is False
    assert result.spillover_seconds < 0


def test_detect_spillover_filters_by_pipeline_name():
    records = _recs("other", [100.0, 100.0, 100.0]) + _recs("pipe", [5.0, 5.0, 5.0])
    result = detect_spillover("pipe", 10.0, records)
    assert result is not None
    assert result.actual_duration == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# scan_spillovers
# ---------------------------------------------------------------------------

def test_scan_spillovers_skips_pipelines_without_scheduled_duration():
    pipelines = [
        _FakePipeline("p1", scheduled_duration=None),
        _FakePipeline("p2", scheduled_duration=20.0),
    ]
    records = _recs("p2", [25.0, 26.0, 27.0])
    results = scan_spillovers(pipelines, records)
    assert len(results) == 1
    assert results[0].pipeline_name == "p2"


def test_scan_spillovers_returns_all_with_enough_samples():
    pipelines = [
        _FakePipeline("a", scheduled_duration=10.0),
        _FakePipeline("b", scheduled_duration=10.0),
    ]
    records = _recs("a", [8.0, 9.0, 7.0]) + _recs("b", [15.0, 16.0, 14.0])
    results = scan_spillovers(pipelines, records)
    names = {r.pipeline_name for r in results}
    assert names == {"a", "b"}


# ---------------------------------------------------------------------------
# SpilloverResult.__str__
# ---------------------------------------------------------------------------

def test_spillover_result_str_spilling():
    r = SpilloverResult("pipe", 20.0, 35.0, 15.0, 5, True)
    assert "⚠" in str(r)
    assert "pipe" in str(r)


def test_spillover_result_str_ok():
    r = SpilloverResult("pipe", 20.0, 15.0, -5.0, 5, False)
    assert "✓" in str(r)


# ---------------------------------------------------------------------------
# spillover_report
# ---------------------------------------------------------------------------

def test_build_spillover_report_empty():
    report = build_spillover_report([])
    assert "No pipelines" in report


def test_build_spillover_report_shows_spilling():
    r = SpilloverResult("pipe", 10.0, 20.0, 10.0, 4, True)
    report = build_spillover_report([r])
    assert "pipe" in report
    assert "Spilling" in report


def test_build_spillover_report_shows_ok():
    r = SpilloverResult("pipe", 30.0, 20.0, -10.0, 4, False)
    report = build_spillover_report([r])
    assert "Within schedule" in report


def test_format_spillover_result_contains_fields():
    r = SpilloverResult("my_pipe", 15.0, 22.5, 7.5, 6, True)
    text = format_spillover_result(r)
    assert "my_pipe" in text
    assert "15.0" in text
    assert "22.5" in text
    assert "+7.5" in text
    assert "6" in text
