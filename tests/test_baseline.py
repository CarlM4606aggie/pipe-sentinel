"""Tests for pipe_sentinel.baseline."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pytest

from pipe_sentinel.baseline import (
    BaselineStats,
    BaselineViolation,
    compute_baseline,
    check_violations,
)
from pipe_sentinel.audit import AuditRecord


def _rec(
    name: str,
    duration: float,
    status: str = "success",
    ts: str = "2024-01-01T00:00:00",
) -> AuditRecord:
    return AuditRecord(
        id=None,
        pipeline_name=name,
        status=status,
        duration=duration,
        retries=0,
        timestamp=ts,
        error=None,
    )


class TestComputeBaseline:
    def _records(self):
        return [_rec("etl", d) for d in [10.0, 12.0, 11.0, 9.0, 13.0]]

    def test_returns_none_when_insufficient_samples(self):
        records = [_rec("etl", 10.0), _rec("etl", 12.0)]
        assert compute_baseline(records, "etl", min_samples=3) is None

    def test_returns_stats_with_correct_sample_count(self):
        stats = compute_baseline(self._records(), "etl", min_samples=3)
        assert stats is not None
        assert stats.sample_count == 5

    def test_mean_is_correct(self):
        stats = compute_baseline(self._records(), "etl", min_samples=3)
        assert stats.mean_duration == pytest.approx(11.0, rel=1e-3)

    def test_upper_bound_uses_multiplier(self):
        stats = compute_baseline(self._records(), "etl", threshold_multiplier=2.0, min_samples=3)
        assert stats.upper_bound == pytest.approx(stats.mean_duration + 2.0 * stats.std_duration)

    def test_excludes_failed_runs(self):
        records = self._records() + [_rec("etl", 999.0, status="failure")]
        stats = compute_baseline(records, "etl", min_samples=3)
        assert stats.sample_count == 5  # failed run excluded

    def test_unknown_pipeline_returns_none(self):
        assert compute_baseline(self._records(), "other", min_samples=3) is None


class TestCheckViolations:
    def _history(self):
        return [_rec("etl", d) for d in [10.0, 11.0, 12.0, 10.5, 11.5]]

    def test_no_violations_when_within_bounds(self):
        recent = [_rec("etl", 11.0)]
        result = check_violations(self._history(), recent, min_samples=3)
        assert result == []

    def test_violation_detected_when_far_above_mean(self):
        recent = [_rec("etl", 999.0)]
        result = check_violations(self._history(), recent, min_samples=3)
        assert len(result) == 1
        assert result[0].pipeline_name == "etl"

    def test_excess_seconds_positive(self):
        recent = [_rec("etl", 999.0)]
        v = check_violations(self._history(), recent, min_samples=3)[0]
        assert v.excess_seconds > 0

    def test_violation_str_contains_pipeline_name(self):
        recent = [_rec("etl", 999.0)]
        v = check_violations(self._history(), recent, min_samples=3)[0]
        assert "etl" in str(v)

    def test_skips_pipeline_with_no_history(self):
        recent = [_rec("new_pipe", 999.0)]
        result = check_violations(self._history(), recent, min_samples=3)
        assert result == []
