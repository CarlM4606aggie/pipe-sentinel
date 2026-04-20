"""Tests for pipe_sentinel.velocity."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import pytest

from pipe_sentinel.velocity import (
    VelocityResult,
    _count_in_window,
    detect_velocity,
    scan_velocity,
)


@dataclass
class _Rec:
    pipeline: str
    timestamp: float
    status: str = "success"
    duration: float = 1.0
    exit_code: int = 0
    error: Optional[str] = None


NOW = time.time()
HOUR = 3600


def _recs(pipeline: str, offsets_hours: list[float]) -> list[_Rec]:
    """Create records at given hour offsets before NOW."""
    return [_Rec(pipeline=pipeline, timestamp=NOW - h * HOUR) for h in offsets_hours]


# ---------------------------------------------------------------------------
# _count_in_window
# ---------------------------------------------------------------------------

def test_count_in_window_empty():
    assert _count_in_window([], NOW - HOUR) == 0


def test_count_in_window_all_recent():
    recs = _recs("p", [0.5, 1.0, 2.0])
    assert _count_in_window(recs, NOW - 3 * HOUR) == 3


def test_count_in_window_some_excluded():
    recs = _recs("p", [1.0, 5.0, 10.0])
    # cutoff 3 hours ago — only the 1h record qualifies
    assert _count_in_window(recs, NOW - 3 * HOUR) == 1


# ---------------------------------------------------------------------------
# detect_velocity
# ---------------------------------------------------------------------------

def test_detect_velocity_no_records():
    result = detect_velocity([], "my_pipe")
    assert result.pipeline == "my_pipe"
    assert result.recent_count == 0
    assert result.baseline_count == 0
    assert result.ratio == 1.0
    assert not result.is_anomalous


def test_detect_velocity_normal_rate():
    # 2 runs in recent window, ~6 in baseline period (3 windows × 2) → ratio ≈ 1
    recent = _recs("pipe", [1, 2])
    older = _recs("pipe", [25, 26, 49, 50, 71, 72])
    result = detect_velocity(recent + older, "pipe", window_hours=24)
    assert not result.is_anomalous


def test_detect_velocity_slowdown_flagged():
    # Only 1 run in recent window vs. many in baseline
    recent = _recs("pipe", [1])
    older = _recs("pipe", [25, 26, 27, 49, 50, 51, 71, 72, 73])
    result = detect_velocity(recent + older, "pipe", window_hours=24, threshold=0.5)
    assert result.is_anomalous
    assert result.ratio < 1.0


def test_detect_velocity_speedup_flagged():
    # Many runs in recent window vs. very few in baseline
    recent = _recs("pipe", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    older = _recs("pipe", [30])
    result = detect_velocity(recent + older, "pipe", window_hours=24, threshold=0.5)
    assert result.is_anomalous
    assert result.ratio > 1.0


def test_detect_velocity_str_contains_pipeline():
    result = VelocityResult(
        pipeline="etl_load",
        window_hours=24,
        recent_count=1,
        baseline_count=5,
        ratio=0.3,
        is_anomalous=True,
    )
    assert "etl_load" in str(result)
    assert "slowdown" in str(result)


def test_detect_velocity_str_speedup_label():
    result = VelocityResult(
        pipeline="etl_load",
        window_hours=24,
        recent_count=10,
        baseline_count=2,
        ratio=3.0,
        is_anomalous=True,
    )
    assert "speedup" in str(result)


# ---------------------------------------------------------------------------
# scan_velocity
# ---------------------------------------------------------------------------

def test_scan_velocity_returns_one_per_pipeline():
    recs = _recs("a", [1, 2]) + _recs("b", [3, 4])
    results = scan_velocity(recs, ["a", "b"])
    assert len(results) == 2
    assert {r.pipeline for r in results} == {"a", "b"}


def test_scan_velocity_empty_pipelines():
    assert scan_velocity([], []) == []
