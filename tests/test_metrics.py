"""Tests for pipe_sentinel.metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pytest

from pipe_sentinel.audit import AuditRecord
from pipe_sentinel.metrics import (
    MetricsReport,
    TrendPoint,
    _avg_duration,
    _failure_rate,
    build_metrics_report,
    compute_trend,
)


def _rec(status: str, duration: float = 1.0) -> AuditRecord:
    return AuditRecord(
        id=None,
        pipeline_name="p",
        status=status,
        duration_s=duration,
        ran_at="2024-01-01T00:00:00",
        retries=0,
        error=None,
    )


def test_failure_rate_all_success():
    records = [_rec("success")] * 5
    assert _failure_rate(records) == 0.0


def test_failure_rate_all_failed():
    records = [_rec("failure")] * 4
    assert _failure_rate(records) == 1.0


def test_failure_rate_mixed():
    records = [_rec("success")] * 3 + [_rec("failure")] * 1
    assert _failure_rate(records) == pytest.approx(0.25)


def test_failure_rate_empty():
    assert _failure_rate([]) == 0.0


def test_avg_duration_basic():
    records = [_rec("success", 2.0), _rec("success", 4.0)]
    assert _avg_duration(records) == pytest.approx(3.0)


def test_avg_duration_empty():
    assert _avg_duration([]) == 0.0


def test_compute_trend_not_degrading():
    records = [_rec("success")] * 10
    point = compute_trend("pipe", records, window=10, degradation_threshold=0.4)
    assert not point.is_degrading
    assert point.failure_rate == 0.0


def test_compute_trend_degrading():
    records = [_rec("failure")] * 6 + [_rec("success")] * 4
    point = compute_trend("pipe", records, window=10, degradation_threshold=0.4)
    assert point.is_degrading
    assert point.failure_rate == pytest.approx(0.6)


def test_compute_trend_respects_window():
    # 20 records: first 15 failures, last 5 successes
    records = [_rec("failure")] * 15 + [_rec("success")] * 5
    point = compute_trend("pipe", records, window=5, degradation_threshold=0.4)
    assert point.failure_rate == 0.0
    assert point.window == 5


def test_build_metrics_report_structure():
    data = {
        "a": [_rec("success")] * 5,
        "b": [_rec("failure")] * 5,
    }
    report = build_metrics_report(data, window=10, degradation_threshold=0.4)
    assert len(report.points) == 2
    assert len(report.degrading) == 1
    assert report.degrading[0].pipeline_name == "b"
    assert len(report.healthy) == 1
