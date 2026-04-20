"""Tests for pipe_sentinel.surge."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest

from pipe_sentinel.surge import (
    SurgeResult,
    _count_failures,
    detect_surge,
    scan_surges,
)


@dataclass
class _Rec:
    pipeline: str
    success: bool


def _recs(spec: list[tuple[str, bool]]) -> list[_Rec]:
    return [_Rec(pipeline=p, success=s) for p, s in spec]


# ---------------------------------------------------------------------------
# _count_failures
# ---------------------------------------------------------------------------

def test_count_failures_empty():
    assert _count_failures([], "etl") == 0


def test_count_failures_all_success():
    records = _recs([("etl", True), ("etl", True)])
    assert _count_failures(records, "etl") == 0


def test_count_failures_mixed():
    records = _recs([("etl", False), ("etl", True), ("etl", False)])
    assert _count_failures(records, "etl") == 2


def test_count_failures_filters_by_pipeline():
    records = _recs([("etl", False), ("other", False)])
    assert _count_failures(records, "etl") == 1


# ---------------------------------------------------------------------------
# detect_surge
# ---------------------------------------------------------------------------

def test_no_surge_when_recent_below_min():
    recent = _recs([("etl", False)])  # only 1 failure, min_recent=2
    history = _recs([("etl", False)] * 4)
    result = detect_surge("etl", recent, history, min_recent=2, surge_ratio=3.0)
    assert not result.is_surging


def test_no_surge_when_ratio_below_threshold():
    recent = _recs([("etl", False)] * 2)
    history = _recs([("etl", False)] * 8)  # baseline = 8/4 = 2, ratio = 1.0
    result = detect_surge("etl", recent, history, history_windows=4, surge_ratio=3.0)
    assert not result.is_surging
    assert result.ratio == pytest.approx(1.0)


def test_surge_detected_when_ratio_exceeds_threshold():
    recent = _recs([("etl", False)] * 6)
    history = _recs([("etl", False)] * 4)  # baseline = 4/4 = 1, ratio = 6.0
    result = detect_surge("etl", recent, history, history_windows=4, surge_ratio=3.0)
    assert result.is_surging
    assert result.ratio == pytest.approx(6.0)


def test_surge_when_baseline_zero_and_min_recent_met():
    recent = _recs([("etl", False)] * 3)
    result = detect_surge("etl", recent, [], history_windows=4, surge_ratio=3.0, min_recent=2)
    assert result.is_surging
    assert result.ratio == float("inf")


def test_no_surge_when_baseline_zero_and_min_recent_not_met():
    recent = _recs([("etl", False)] * 1)
    result = detect_surge("etl", recent, [], history_windows=4, surge_ratio=3.0, min_recent=2)
    assert not result.is_surging


# ---------------------------------------------------------------------------
# scan_surges
# ---------------------------------------------------------------------------

def test_scan_surges_returns_one_per_pipeline():
    pipelines = ["etl", "load", "transform"]
    results = scan_surges(pipelines, [], [])
    assert len(results) == 3
    assert {r.pipeline for r in results} == set(pipelines)


def test_scan_surges_marks_correct_pipeline_surging():
    recent = _recs([("etl", False)] * 6)
    history = _recs([("etl", False)] * 4)
    results = scan_surges(["etl", "load"], recent, history, history_windows=4, surge_ratio=3.0)
    by_name = {r.pipeline: r for r in results}
    assert by_name["etl"].is_surging
    assert not by_name["load"].is_surging


# ---------------------------------------------------------------------------
# SurgeResult.__str__
# ---------------------------------------------------------------------------

def test_str_surging_contains_icon():
    r = SurgeResult("etl", 6, 1.0, 6.0, True)
    assert "🔺" in str(r)


def test_str_ok_contains_icon():
    r = SurgeResult("etl", 1, 2.0, 0.5, False)
    assert "✅" in str(r)
