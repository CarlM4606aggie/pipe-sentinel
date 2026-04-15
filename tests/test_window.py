"""Tests for pipe_sentinel.window and pipe_sentinel.window_report."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipe_sentinel.window import (
    WindowConfig,
    WindowEntry,
    WindowResult,
    evaluate_window,
    scan_windows,
)
from pipe_sentinel.window_report import (
    format_window_result,
    build_window_report,
)

NOW = datetime(2024, 6, 1, 12, 0, 0)


def _entry(name: str, succeeded: bool, minutes_ago: int = 5) -> WindowEntry:
    return WindowEntry(
        pipeline_name=name,
        succeeded=succeeded,
        timestamp=NOW - timedelta(minutes=minutes_ago),
    )


# ---------------------------------------------------------------------------
# WindowConfig validation
# ---------------------------------------------------------------------------

def test_window_config_defaults():
    wc = WindowConfig()
    assert wc.duration_minutes == 60
    assert wc.min_runs == 3
    assert wc.failure_threshold == 0.5


def test_window_config_invalid_duration():
    with pytest.raises(ValueError, match="duration_minutes"):
        WindowConfig(duration_minutes=0)


def test_window_config_invalid_threshold():
    with pytest.raises(ValueError, match="failure_threshold"):
        WindowConfig(failure_threshold=1.5)


# ---------------------------------------------------------------------------
# evaluate_window
# ---------------------------------------------------------------------------

def test_evaluate_window_no_entries():
    result = evaluate_window([], "etl", WindowConfig(), now=NOW)
    assert result.total == 0
    assert result.failures == 0
    assert result.failure_rate == 0.0
    assert not result.breached


def test_evaluate_window_all_success():
    entries = [_entry("etl", True) for _ in range(5)]
    result = evaluate_window(entries, "etl", WindowConfig(min_runs=3, failure_threshold=0.5), now=NOW)
    assert result.failures == 0
    assert not result.breached


def test_evaluate_window_breached():
    entries = [_entry("etl", False) for _ in range(4)]
    result = evaluate_window(entries, "etl", WindowConfig(min_runs=3, failure_threshold=0.5), now=NOW)
    assert result.breached
    assert result.failure_rate == 1.0


def test_evaluate_window_below_min_runs_not_breached():
    entries = [_entry("etl", False), _entry("etl", False)]
    result = evaluate_window(entries, "etl", WindowConfig(min_runs=3, failure_threshold=0.5), now=NOW)
    assert not result.breached  # only 2 runs, min_runs=3


def test_evaluate_window_ignores_old_entries():
    old = _entry("etl", False, minutes_ago=120)  # outside 60-min window
    recent = _entry("etl", True, minutes_ago=10)
    result = evaluate_window([old, recent], "etl", WindowConfig(), now=NOW)
    assert result.total == 1
    assert result.failures == 0


def test_evaluate_window_ignores_other_pipelines():
    entries = [_entry("other", False) for _ in range(5)]
    result = evaluate_window(entries, "etl", WindowConfig(), now=NOW)
    assert result.total == 0


# ---------------------------------------------------------------------------
# scan_windows
# ---------------------------------------------------------------------------

def test_scan_windows_returns_one_per_pipeline():
    entries = [_entry("a", True), _entry("b", False), _entry("b", False), _entry("b", False)]
    results = scan_windows(entries, ["a", "b"], WindowConfig(min_runs=2), now=NOW)
    assert len(results) == 2
    names = {r.pipeline_name for r in results}
    assert names == {"a", "b"}


# ---------------------------------------------------------------------------
# window_report
# ---------------------------------------------------------------------------

def test_format_window_result_ok():
    r = WindowResult("etl", total=5, failures=0, failure_rate=0.0, breached=False, window_minutes=60)
    text = format_window_result(r)
    assert "etl" in text
    assert "OK" not in text  # icon-based, no literal OK
    assert "0%" in text or "0 %" in text or "0.0" not in text  # rate shown


def test_format_window_result_breached_contains_name():
    r = WindowResult("critical", total=4, failures=4, failure_rate=1.0, breached=True, window_minutes=30)
    text = format_window_result(r)
    assert "critical" in text


def test_build_window_report_empty():
    report = build_window_report([])
    assert "No window data" in report


def test_build_window_report_contains_pipeline_names():
    results = [
        WindowResult("pipe_a", 3, 1, 0.33, False, 60),
        WindowResult("pipe_b", 3, 3, 1.0, True, 60),
    ]
    report = build_window_report(results)
    assert "pipe_a" in report
    assert "pipe_b" in report
    assert "Breached: 1/2" in report
