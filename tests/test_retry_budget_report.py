"""Tests for pipe_sentinel.retry_budget_report."""
from __future__ import annotations

import pytest

from pipe_sentinel.retry_budget import RetryBudgetConfig, RetryBudgetState
from pipe_sentinel.retry_budget_report import (
    _bar,
    build_retry_budget_report,
    format_budget_state,
)


@pytest.fixture()
def cfg():
    return RetryBudgetConfig(max_retries=4, window_seconds=120)


def _state(name: str, used: int) -> RetryBudgetState:
    s = RetryBudgetState(pipeline=name)
    for _ in range(used):
        s.record_attempt()
    return s


def test_bar_empty():
    assert _bar(0, 10) == "[" + "-" * 20 + "]"


def test_bar_full():
    assert _bar(10, 10) == "[" + "#" * 20 + "]"


def test_bar_half():
    result = _bar(5, 10)
    assert result.count("#") == 10
    assert result.count("-") == 10


def test_bar_zero_total():
    result = _bar(0, 0)
    assert result == "[" + " " * 20 + "]"


def test_format_budget_state_ok(cfg):
    s = _state("pipe_a", 1)
    text = format_budget_state(s, cfg)
    assert "pipe_a" in text
    assert "ok" in text
    assert "1/4" in text


def test_format_budget_state_exhausted(cfg):
    s = _state("pipe_b", 4)
    text = format_budget_state(s, cfg)
    assert "EXHAUSTED" in text


def test_build_report_empty(cfg):
    report = build_retry_budget_report([], cfg)
    assert "no data" in report


def test_build_report_shows_all_pipelines(cfg):
    states = [_state("alpha", 0), _state("beta", 2)]
    report = build_retry_budget_report(states, cfg)
    assert "alpha" in report
    assert "beta" in report
    assert "2 pipeline(s)" in report


def test_build_report_shows_window(cfg):
    states = [_state("gamma", 0)]
    report = build_retry_budget_report(states, cfg)
    assert "120s" in report
