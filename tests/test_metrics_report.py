"""Tests for pipe_sentinel.metrics_report formatting."""
from __future__ import annotations

import pytest

from pipe_sentinel.metrics import MetricsReport, TrendPoint
from pipe_sentinel.metrics_report import (
    _bar,
    format_metrics_report,
    format_trend_point,
)


def _point(
    name: str = "my_pipeline",
    failure_rate: float = 0.0,
    avg_duration_s: float = 1.5,
    is_degrading: bool = False,
    window: int = 10,
) -> TrendPoint:
    return TrendPoint(
        pipeline_name=name,
        window=window,
        failure_rate=failure_rate,
        avg_duration_s=avg_duration_s,
        is_degrading=is_degrading,
    )


def test_bar_empty():
    assert _bar(0.0, width=10) == "[----------]"


def test_bar_full():
    assert _bar(1.0, width=10) == "[##########]"


def test_bar_half():
    result = _bar(0.5, width=10)
    assert result == "[#####-----]"


def test_format_trend_point_ok():
    p = _point(name="etl_load", failure_rate=0.1, is_degrading=False)
    line = format_trend_point(p)
    assert "OK" in line
    assert "etl_load" in line
    assert "10%" in line


def test_format_trend_point_degrading():
    p = _point(name="etl_load", failure_rate=0.6, is_degrading=True)
    line = format_trend_point(p)
    assert "DEGRADING" in line
    assert "60%" in line


def test_format_metrics_report_empty():
    report = MetricsReport(points=[])
    text = format_metrics_report(report)
    assert "No metrics data" in text


def test_format_metrics_report_contains_pipeline_names():
    report = MetricsReport(
        points=[
            _point("alpha", failure_rate=0.0, is_degrading=False),
            _point("beta", failure_rate=0.8, is_degrading=True),
        ]
    )
    text = format_metrics_report(report)
    assert "alpha" in text
    assert "beta" in text


def test_format_metrics_report_degrading_section():
    report = MetricsReport(
        points=[
            _point("bad_pipe", failure_rate=0.9, is_degrading=True),
        ]
    )
    text = format_metrics_report(report)
    assert "Degrading" in text
    assert "bad_pipe" in text


def test_format_metrics_report_summary_counts():
    report = MetricsReport(
        points=[
            _point("a", is_degrading=False),
            _point("b", is_degrading=True),
            _point("c", is_degrading=False),
        ]
    )
    text = format_metrics_report(report)
    assert "Pipelines: 3" in text
    assert "Degrading: 1" in text
    assert "Healthy: 2" in text
