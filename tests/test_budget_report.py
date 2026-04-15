"""Tests for pipe_sentinel.budget_report."""
from __future__ import annotations

import pytest
from dataclasses import dataclass
from typing import Optional

from pipe_sentinel.budget import BudgetConfig, evaluate_budget
from pipe_sentinel.budget_report import (
    _bar,
    format_budget_result,
    build_budget_report,
)


@dataclass
class _FakeResult:
    pipeline_name: str
    duration_seconds: Optional[float]
    success: bool = True


def _r(name: str, dur: float) -> _FakeResult:
    return _FakeResult(pipeline_name=name, duration_seconds=dur)


@pytest.fixture
def cfg() -> BudgetConfig:
    return BudgetConfig(max_total_seconds=100.0, warn_at_percent=80.0)


def test_bar_empty():
    assert _bar(0) == "[" + "-" * 30 + "]"


def test_bar_full():
    assert _bar(100) == "[" + "#" * 30 + "]"


def test_bar_half():
    result = _bar(50)
    assert result.count("#") == 15
    assert result.count("-") == 15


def test_format_ok_contains_checkmark(cfg):
    result = evaluate_budget(cfg, [_r("pipe", 10.0)])
    text = format_budget_result(result)
    assert "✓" in text


def test_format_warn_contains_warning_icon(cfg):
    result = evaluate_budget(cfg, [_r("pipe", 85.0)])
    text = format_budget_result(result)
    assert "⚠" in text


def test_format_exceeded_contains_cross(cfg):
    result = evaluate_budget(cfg, [_r("pipe", 110.0)])
    text = format_budget_result(result)
    assert "✗" in text


def test_format_shows_pipeline_count(cfg):
    result = evaluate_budget(cfg, [_r("a", 5.0), _r("b", 5.0)])
    text = format_budget_result(result)
    assert "2" in text


def test_format_shows_remaining(cfg):
    result = evaluate_budget(cfg, [_r("a", 40.0)])
    text = format_budget_result(result)
    assert "60.0" in text


def test_format_breakdown_lists_pipeline_name(cfg):
    result = evaluate_budget(cfg, [_r("my_pipeline", 20.0)])
    text = format_budget_result(result)
    assert "my_pipeline" in text


def test_build_report_contains_header(cfg):
    result = evaluate_budget(cfg, [])
    report = build_budget_report(result)
    assert "Runtime Budget Report" in report
    assert "=" * 10 in report
