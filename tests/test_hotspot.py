"""Tests for pipe_sentinel.hotspot."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest

from pipe_sentinel.hotspot import (
    HotspotResult,
    _group_by_pipeline,
    detect_hotspot,
    scan_hotspots,
)


@dataclass
class _Rec:
    pipeline: str
    status: str
    duration: Optional[float] = 1.0


def _recs(pipeline: str, statuses: list[str]) -> list[_Rec]:
    return [_Rec(pipeline=pipeline, status=s) for s in statuses]


# --- group_by_pipeline ---

def test_group_by_pipeline_empty():
    assert _group_by_pipeline([]) == {}


def test_group_by_pipeline_single():
    recs = _recs("etl", ["success", "failure"])
    groups = _group_by_pipeline(recs)
    assert "etl" in groups
    assert len(groups["etl"]) == 2


def test_group_by_pipeline_multiple():
    recs = _recs("a", ["success"]) + _recs("b", ["failure", "failure"])
    groups = _group_by_pipeline(recs)
    assert set(groups.keys()) == {"a", "b"}


# --- detect_hotspot ---

def test_detect_hotspot_returns_none_when_insufficient_runs():
    recs = _recs("etl", ["failure", "failure"])
    assert detect_hotspot("etl", recs, min_runs=3) is None


def test_detect_hotspot_returns_result_when_enough_runs():
    recs = _recs("etl", ["success", "failure", "failure"])
    result = detect_hotspot("etl", recs, min_runs=3)
    assert result is not None
    assert result.pipeline == "etl"
    assert result.total_runs == 3
    assert result.failures == 2


def test_detect_hotspot_failure_rate_all_fail():
    recs = _recs("etl", ["failure"] * 5)
    result = detect_hotspot("etl", recs, min_runs=3)
    assert result is not None
    assert result.failure_rate == pytest.approx(1.0)


def test_detect_hotspot_failure_rate_none_fail():
    recs = _recs("etl", ["success"] * 5)
    result = detect_hotspot("etl", recs, min_runs=3)
    assert result is not None
    assert result.failure_rate == pytest.approx(0.0)
    assert not result.is_hotspot


def test_hotspot_result_str_contains_pipeline():
    r = HotspotResult(pipeline="my_pipe", total_runs=10, failures=4, failure_rate=0.4)
    assert "my_pipe" in str(r)
    assert "4/10" in str(r)


# --- scan_hotspots ---

def test_scan_hotspots_empty():
    assert scan_hotspots([]) == []


def test_scan_hotspots_excludes_no_failure_pipelines():
    recs = _recs("ok", ["success"] * 5)
    assert scan_hotspots(recs, min_runs=3) == []


def test_scan_hotspots_sorted_by_failure_rate():
    recs = (
        _recs("low", ["success", "success", "failure"])
        + _recs("high", ["failure", "failure", "failure"])
    )
    results = scan_hotspots(recs, top_n=5, min_runs=3)
    assert results[0].pipeline == "high"
    assert results[1].pipeline == "low"


def test_scan_hotspots_top_n_limits_results():
    all_recs = []
    for i in range(10):
        all_recs += _recs(f"pipe_{i}", ["failure"] * 4)
    results = scan_hotspots(all_recs, top_n=3, min_runs=3)
    assert len(results) == 3
