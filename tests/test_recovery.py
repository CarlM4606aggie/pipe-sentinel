"""Tests for pipe_sentinel.recovery and pipe_sentinel.recovery_report."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest

from pipe_sentinel.recovery import (
    RecoveryResult,
    _consecutive_failures_before_last,
    detect_recovery,
    scan_recoveries,
)
from pipe_sentinel.recovery_report import (
    build_recovery_report,
    format_recovery_result,
)


@dataclass
class _Rec:
    pipeline_name: str
    status: str
    finished_at: str


def _recs(pipeline: str, statuses: list[str]) -> list[_Rec]:
    """Build a newest-first list of fake records for *pipeline*."""
    return [
        _Rec(pipeline_name=pipeline, status=s, finished_at=f"2024-01-{10 - i:02d}T00:00:00")
        for i, s in enumerate(statuses)
    ]


# ---------------------------------------------------------------------------
# _consecutive_failures_before_last
# ---------------------------------------------------------------------------

def test_consecutive_failures_before_last_empty():
    assert _consecutive_failures_before_last([]) == 0


def test_consecutive_failures_before_last_single():
    records = _recs("p", ["failure"])
    assert _consecutive_failures_before_last(records) == 0


def test_consecutive_failures_before_last_two_failures():
    records = _recs("p", ["success", "failure", "failure"])
    assert _consecutive_failures_before_last(records) == 2


def test_consecutive_failures_before_last_stops_at_success():
    records = _recs("p", ["success", "failure", "success", "failure"])
    assert _consecutive_failures_before_last(records) == 1


# ---------------------------------------------------------------------------
# detect_recovery
# ---------------------------------------------------------------------------

def test_detect_recovery_empty_records():
    result = detect_recovery([])
    assert not result.recovered
    assert result.previous_failures == 0


def test_detect_recovery_latest_is_failure():
    records = _recs("pipe", ["failure", "success"])
    result = detect_recovery(records)
    assert not result.recovered


def test_detect_recovery_no_prior_failures():
    records = _recs("pipe", ["success", "success"])
    result = detect_recovery(records)
    assert not result.recovered
    assert result.pipeline_name == "pipe"


def test_detect_recovery_single_prior_failure():
    records = _recs("pipe", ["success", "failure"])
    result = detect_recovery(records)
    assert result.recovered
    assert result.previous_failures == 1
    assert result.pipeline_name == "pipe"
    assert result.recovered_at == records[0].finished_at


def test_detect_recovery_multiple_prior_failures():
    records = _recs("pipe", ["success", "failure", "failure", "failure"])
    result = detect_recovery(records)
    assert result.recovered
    assert result.previous_failures == 3


# ---------------------------------------------------------------------------
# scan_recoveries
# ---------------------------------------------------------------------------

def test_scan_recoveries_empty():
    assert scan_recoveries([]) == []


def test_scan_recoveries_returns_only_recovered():
    all_records = (
        _recs("alpha", ["success", "failure"])
        + _recs("beta", ["success", "success"])
    )
    results = scan_recoveries(all_records)
    assert len(results) == 1
    assert results[0].pipeline_name == "alpha"


def test_scan_recoveries_min_prior_failures_filter():
    all_records = (
        _recs("alpha", ["success", "failure"])
        + _recs("beta", ["success", "failure", "failure", "failure"])
    )
    results = scan_recoveries(all_records, min_prior_failures=2)
    assert len(results) == 1
    assert results[0].pipeline_name == "beta"


# ---------------------------------------------------------------------------
# recovery_report
# ---------------------------------------------------------------------------

def test_format_recovery_result_recovered():
    r = RecoveryResult(
        pipeline_name="etl_load",
        recovered=True,
        previous_failures=2,
        recovered_at="2024-01-10T12:00:00",
    )
    text = format_recovery_result(r)
    assert "etl_load" in text
    assert "2" in text
    assert "✅" in text


def test_format_recovery_result_not_recovered():
    r = RecoveryResult(pipeline_name="etl_load", recovered=False, previous_failures=0)
    text = format_recovery_result(r)
    assert "➖" in text
    assert "etl_load" in text


def test_build_recovery_report_empty():
    report = build_recovery_report([])
    assert "No recovery events" in report


def test_build_recovery_report_shows_total():
    results = [
        RecoveryResult("a", True, 1, "2024-01-10T00:00:00"),
        RecoveryResult("b", True, 3, "2024-01-09T00:00:00"),
    ]
    report = build_recovery_report(results)
    assert "Total recoveries: 2" in report
    assert "a" in report
    assert "b" in report
