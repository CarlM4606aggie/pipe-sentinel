"""Tests for pipe_sentinel.digest."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipe_sentinel.audit import AuditRecord
from pipe_sentinel.digest import DigestReport, build_digest, format_digest


def _rec(name: str, status: str, duration: float = 1.0) -> AuditRecord:
    return AuditRecord(
        pipeline_name=name,
        status=status,
        started_at="2024-01-01T00:00:00",
        duration=duration,
        exit_code=0 if status == "success" else 1,
        attempts=1,
        error_output="" if status == "success" else "err",
    )


_TS = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def mixed_records() -> List[AuditRecord]:
    return [
        _rec("alpha", "success"),
        _rec("beta", "failure"),
        _rec("gamma", "success"),
        _rec("delta", "failure"),
    ]


@pytest.fixture()
def all_passing() -> List[AuditRecord]:
    return [_rec("alpha", "success"), _rec("beta", "success")]


def test_build_digest_totals(mixed_records):
    report = build_digest(mixed_records, generated_at=_TS)
    assert report.total == 4
    assert report.passed == 2
    assert report.failed == 2


def test_build_digest_pass_rate(mixed_records):
    report = build_digest(mixed_records, generated_at=_TS)
    assert report.pass_rate == pytest.approx(50.0)


def test_build_digest_pass_rate_all_passing(all_passing):
    """Pass rate should be 100.0 when every record is a success."""
    report = build_digest(all_passing, generated_at=_TS)
    assert report.pass_rate == pytest.approx(100.0)


def test_build_digest_failed_pipeline_names(mixed_records):
    report = build_digest(mixed_records, generated_at=_TS)
    assert "beta" in report.pipelines
    assert "delta" in report.pipelines
    assert "alpha" not in report.pipelines


def test_build_digest_has_failures_true(mixed_records):
    report = build_digest(mixed_records, generated_at=_TS)
    assert report.has_failures is True


def test_build_digest_has_failures_false(all_passing):
    report = build_digest(all_passing, generated_at=_TS)
    assert report.has_failures is False


def test_build_digest_empty_records():
    report = build_digest([], generated_at=_TS)
    assert report.total == 0
    assert report.pass_rate == 0.0
    assert report.pipelines == []


def test_format_digest_contains_header(mixed_records):
    report = build_digest(mixed_records, generated_at=_TS)
    text = format_digest(report)
    assert "Pipe-Sentinel Digest" in text
    assert "2024-06-01" in text


def test_format_digest_shows_failed_names(mixed_records):
    report = build_digest(mixed_records, generated_at=_TS)
    text = format_digest(report)
    assert "beta" in text
    assert "delta" in text


def test_format_digest_all_passed_message(all_passing):
    report = build_digest(all_passing, generated_at=_TS)
    text = format_digest(report)
    assert "All pipelines passed" in text
    assert "Failed pipelines" not in text
