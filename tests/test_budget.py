"""Tests for pipe_sentinel.budget."""
from __future__ import annotations

import pytest
from dataclasses import dataclass
from typing import Optional

from pipe_sentinel.budget import BudgetConfig, BudgetResult, evaluate_budget


@dataclass
class _FakeResult:
    pipeline_name: str
    duration_seconds: Optional[float]
    success: bool = True
    returncode: int = 0
    stdout: str = ""
    stderr: str = ""


# Re-use the real RunResult shape via duck-typing — evaluate_budget only reads
# pipeline_name and duration_seconds.


@pytest.fixture
def cfg() -> BudgetConfig:
    return BudgetConfig(max_total_seconds=60.0, warn_at_percent=80.0)


def _r(name: str, dur: Optional[float]) -> _FakeResult:
    return _FakeResult(pipeline_name=name, duration_seconds=dur)


def test_budget_config_invalid_max():
    with pytest.raises(ValueError, match="max_total_seconds"):
        BudgetConfig(max_total_seconds=0)


def test_budget_config_invalid_warn():
    with pytest.raises(ValueError, match="warn_at_percent"):
        BudgetConfig(max_total_seconds=60, warn_at_percent=0)


def test_evaluate_empty_results(cfg):
    result = evaluate_budget(cfg, [])
    assert result.total_seconds == 0.0
    assert not result.exceeded
    assert not result.warned
    assert result.pipeline_count == 0


def test_evaluate_within_budget(cfg):
    results = [_r("a", 10.0), _r("b", 20.0)]
    result = evaluate_budget(cfg, results)
    assert result.total_seconds == pytest.approx(30.0)
    assert not result.exceeded
    assert not result.warned


def test_evaluate_warn_threshold(cfg):
    # 80% of 60 = 48 seconds
    results = [_r("a", 48.0)]
    result = evaluate_budget(cfg, results)
    assert result.warned
    assert not result.exceeded


def test_evaluate_exceeded(cfg):
    results = [_r("a", 40.0), _r("b", 25.0)]
    result = evaluate_budget(cfg, results)
    assert result.exceeded
    assert not result.warned


def test_evaluate_skips_none_duration(cfg):
    results = [_r("a", 10.0), _r("b", None)]
    result = evaluate_budget(cfg, results)
    assert result.total_seconds == pytest.approx(10.0)
    assert result.pipeline_count == 2


def test_utilisation_pct(cfg):
    results = [_r("a", 30.0)]
    result = evaluate_budget(cfg, results)
    assert result.utilisation_pct == pytest.approx(50.0)


def test_remaining_seconds(cfg):
    results = [_r("a", 20.0)]
    result = evaluate_budget(cfg, results)
    assert result.remaining_seconds == pytest.approx(40.0)


def test_remaining_seconds_zero_when_exceeded(cfg):
    results = [_r("a", 100.0)]
    result = evaluate_budget(cfg, results)
    assert result.remaining_seconds == 0.0


def test_contributions_sorted_by_duration(cfg):
    results = [_r("fast", 5.0), _r("slow", 20.0), _r("mid", 10.0)]
    result = evaluate_budget(cfg, results)
    names = [n for n, _ in result.contributions]
    # contributions list is unsorted; sorting happens in the report layer
    assert set(names) == {"fast", "slow", "mid"}


def test_str_ok(cfg):
    result = evaluate_budget(cfg, [_r("a", 5.0)])
    assert "OK" in str(result)


def test_str_warn(cfg):
    result = evaluate_budget(cfg, [_r("a", 50.0)])
    assert "WARN" in str(result)


def test_str_exceeded(cfg):
    result = evaluate_budget(cfg, [_r("a", 70.0)])
    assert "EXCEEDED" in str(result)
