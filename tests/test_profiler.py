"""Tests for pipe_sentinel.profiler and pipe_sentinel.profiler_report."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest

from pipe_sentinel.profiler import (
    ProfileStats,
    _percentile,
    compute_profile,
    scan_profiles,
)
from pipe_sentinel.profiler_report import (
    build_profiler_report,
    format_profile_stats,
)


@dataclass
class _Rec:
    pipeline: str
    duration_seconds: Optional[float]
    status: str = "success"


def _recs(pipeline: str, durations):
    return [_Rec(pipeline=pipeline, duration_seconds=d) for d in durations]


# --- _percentile ---

def test_percentile_empty():
    assert _percentile([], 0.95) == 0.0


def test_percentile_single():
    assert _percentile([5.0], 0.95) == 5.0


def test_percentile_p95():
    values = sorted(float(i) for i in range(1, 21))  # 1..20
    result = _percentile(values, 0.95)
    assert result == 20.0


# --- compute_profile ---

def test_compute_profile_none_when_no_records():
    assert compute_profile("etl", []) is None


def test_compute_profile_none_when_wrong_pipeline():
    recs = _recs("other", [1.0, 2.0])
    assert compute_profile("etl", recs) is None


def test_compute_profile_basic():
    recs = _recs("etl", [2.0, 4.0, 6.0, 8.0, 10.0])
    stats = compute_profile("etl", recs)
    assert stats is not None
    assert stats.pipeline == "etl"
    assert stats.sample_count == 5
    assert stats.min_seconds == 2.0
    assert stats.max_seconds == 10.0
    assert stats.mean_seconds == pytest.approx(6.0)


def test_compute_profile_skips_none_duration():
    recs = _recs("etl", [3.0, None, 7.0])
    stats = compute_profile("etl", recs)
    assert stats is not None
    assert stats.sample_count == 2


# --- is_slow ---

def test_is_slow_when_p95_exceeds_threshold():
    stats = ProfileStats("etl", 10, 1.0, 120.0, 60.0, 110.0)
    assert stats.is_slow(100.0) is True


def test_not_slow_when_p95_below_threshold():
    stats = ProfileStats("etl", 10, 1.0, 50.0, 25.0, 45.0)
    assert stats.is_slow(60.0) is False


# --- scan_profiles ---

def test_scan_profiles_returns_only_named():
    recs = _recs("etl", [5.0]) + _recs("load", [3.0])
    results = scan_profiles(recs, ["etl"])
    assert len(results) == 1
    assert results[0].pipeline == "etl"


def test_scan_profiles_skips_missing():
    recs = _recs("etl", [5.0])
    results = scan_profiles(recs, ["etl", "missing"])
    assert len(results) == 1


# --- report ---

def test_format_profile_stats_contains_pipeline():
    stats = ProfileStats("etl", 5, 1.0, 10.0, 5.0, 9.0)
    out = format_profile_stats(stats, threshold=60.0)
    assert "etl" in out
    assert "5.00" in out


def test_build_profiler_report_empty():
    out = build_profiler_report([])
    assert "No profiling" in out


def test_build_profiler_report_shows_count():
    profiles = [
        ProfileStats("etl", 10, 1.0, 20.0, 10.0, 18.0),
        ProfileStats("load", 5, 2.0, 5.0, 3.0, 4.9),
    ]
    out = build_profiler_report(profiles, threshold=60.0)
    assert "Total pipelines: 2" in out
    assert "Slow (p95): 0" in out
