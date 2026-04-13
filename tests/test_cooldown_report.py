"""Tests for pipe_sentinel.cooldown_report."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipe_sentinel.cooldown import CooldownEntry, CooldownStore
from pipe_sentinel.cooldown_report import (
    build_cooldown_report,
    format_entry,
    print_cooldown_report,
)


@pytest.fixture
def now() -> float:
    return time.time()


@pytest.fixture
def cooling_entry(now: float) -> CooldownEntry:
    return CooldownEntry(pipeline_name="slow_pipeline", failed_at=now, cooldown_seconds=300)


@pytest.fixture
def ready_entry(now: float) -> CooldownEntry:
    return CooldownEntry(pipeline_name="fast_pipeline", failed_at=now - 400, cooldown_seconds=300)


def test_format_entry_cooling_shows_remaining(cooling_entry: CooldownEntry, now: float) -> None:
    result = format_entry(cooling_entry, now=now + 60)
    assert "COOLING" in result
    assert "slow_pipeline" in result


def test_format_entry_ready_shows_ready(ready_entry: CooldownEntry, now: float) -> None:
    result = format_entry(ready_entry, now=now)
    assert "READY" in result
    assert "fast_pipeline" in result


def test_build_report_empty_returns_message() -> None:
    report = build_cooldown_report([])
    assert "No cooldown" in report


def test_build_report_contains_pipeline_names(
    cooling_entry: CooldownEntry, ready_entry: CooldownEntry, now: float
) -> None:
    report = build_cooldown_report([cooling_entry, ready_entry], now=now)
    assert "slow_pipeline" in report
    assert "fast_pipeline" in report


def test_build_report_summary_counts(
    cooling_entry: CooldownEntry, ready_entry: CooldownEntry, now: float
) -> None:
    report = build_cooldown_report([cooling_entry, ready_entry], now=now)
    assert "Cooling: 1" in report
    assert "Ready: 1" in report


def test_build_report_all_cooling(cooling_entry: CooldownEntry, now: float) -> None:
    report = build_cooldown_report([cooling_entry], now=now)
    assert "Cooling: 1" in report
    assert "Ready: 0" in report


def test_print_cooldown_report_runs(tmp_path: Path, now: float, capsys: pytest.CaptureFixture) -> None:
    store = CooldownStore(path=tmp_path / "cooldown.json")
    store.record_failure("my_pipe", cooldown_seconds=300, now=now)
    print_cooldown_report(store, now=now + 10)
    captured = capsys.readouterr()
    assert "my_pipe" in captured.out
