"""Tests for pipe_sentinel.rate_limit_report."""
from __future__ import annotations

import time

import pytest

from pipe_sentinel.rate_limit import RateLimitState
from pipe_sentinel.rate_limit_report import (
    _bar,
    build_rate_limit_report,
    format_state,
)


@pytest.fixture
def now() -> float:
    return time.time()


def _make(pipeline: str, max_runs: int, used: int, now: float) -> RateLimitState:
    s = RateLimitState(pipeline, 3600, max_runs)
    for _ in range(used):
        s.record_run(now)
    return s


# --- _bar ---

def test_bar_empty():
    assert _bar(0, 10) == "[----------]"


def test_bar_full():
    assert _bar(10, 10) == "[##########]"


def test_bar_half():
    result = _bar(5, 10)
    assert result.count("#") == 5
    assert result.count("-") == 5


def test_bar_zero_limit_returns_empty_bar():
    result = _bar(0, 0)
    assert result == "[----------]"


# --- format_state ---

def test_format_state_ok(now: float):
    s = _make("my_pipeline", 5, 2, now)
    line = format_state(s, now)
    assert "my_pipeline" in line
    assert "2/5" in line
    assert "[OK]" in line


def test_format_state_limited(now: float):
    s = _make("busy_pipeline", 3, 3, now)
    line = format_state(s, now)
    assert "[LIMITED]" in line
    assert "3/3" in line


def test_format_state_shows_window(now: float):
    s = _make("p", 5, 1, now)
    line = format_state(s, now)
    assert "window=3600s" in line


# --- build_rate_limit_report ---

def test_report_empty_states():
    report = build_rate_limit_report([])
    assert "no pipelines tracked" in report


def test_report_contains_all_pipelines(now: float):
    states = [
        _make("alpha", 5, 1, now),
        _make("beta", 5, 5, now),
    ]
    report = build_rate_limit_report(states, now)
    assert "alpha" in report
    assert "beta" in report


def test_report_shows_summary_counts(now: float):
    states = [
        _make("p1", 3, 3, now),  # limited
        _make("p2", 3, 1, now),  # ok
        _make("p3", 3, 2, now),  # ok
    ]
    report = build_rate_limit_report(states, now)
    assert "Pipelines tracked: 3" in report
    assert "Limited: 1" in report


def test_report_limited_pipeline_marked(now: float):
    states = [_make("critical", 2, 2, now)]
    report = build_rate_limit_report(states, now)
    assert "LIMITED" in report
