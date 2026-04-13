"""Tests for pipe_sentinel.baseline_report."""
from __future__ import annotations

import pytest

from pipe_sentinel.baseline import BaselineStats, BaselineViolation
from pipe_sentinel.baseline_report import (
    format_baseline_stats,
    format_violation,
    build_baseline_report,
)


def _stats(name: str = "etl", mean: float = 10.0, std: float = 1.0) -> BaselineStats:
    return BaselineStats(
        pipeline_name=name,
        sample_count=5,
        mean_duration=mean,
        std_duration=std,
        threshold_multiplier=2.0,
    )


def _violation(actual: float = 20.0) -> BaselineViolation:
    return BaselineViolation(
        pipeline_name="etl",
        actual_duration=actual,
        baseline=_stats(),
    )


class TestFormatBaselineStats:
    def test_contains_pipeline_name(self):
        assert "etl" in format_baseline_stats(_stats())

    def test_contains_mean(self):
        assert "10.00" in format_baseline_stats(_stats())

    def test_contains_upper_bound(self):
        # mean=10, std=1, mult=2 → upper=12
        assert "12.00" in format_baseline_stats(_stats())

    def test_contains_sample_count(self):
        assert "5" in format_baseline_stats(_stats())


class TestFormatViolation:
    def test_contains_pipeline_name(self):
        assert "etl" in format_violation(_violation())

    def test_contains_actual_duration(self):
        assert "20.00" in format_violation(_violation(20.0))

    def test_contains_warning_symbol(self):
        assert "⚠" in format_violation(_violation())


class TestBuildBaselineReport:
    def test_empty_violations_returns_ok_message(self):
        report = build_baseline_report([])
        assert "within expected" in report

    def test_violations_count_in_header(self):
        report = build_baseline_report([_violation(), _violation()])
        assert "2" in report

    def test_each_violation_appears(self):
        violations = [_violation(20.0), _violation(30.0)]
        report = build_baseline_report(violations)
        assert report.count("etl") >= 2

    def test_report_ends_with_newline(self):
        assert build_baseline_report([]).endswith("\n")
        assert build_baseline_report([_violation()]).endswith("\n")
