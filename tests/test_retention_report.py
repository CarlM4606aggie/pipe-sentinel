"""Tests for pipe_sentinel.retention_report."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipe_sentinel.retention import PruneResult
from pipe_sentinel.retention_report import (
    format_prune_result,
    format_dry_run,
    print_retention_report,
)


@pytest.fixture()
def zero_result() -> PruneResult:
    return PruneResult(
        rows_deleted=0,
        cutoff_ts=datetime(2024, 4, 1, tzinfo=timezone.utc),
    )


@pytest.fixture()
def one_result() -> PruneResult:
    return PruneResult(
        rows_deleted=1,
        cutoff_ts=datetime(2024, 4, 1, tzinfo=timezone.utc),
    )


@pytest.fixture()
def many_result() -> PruneResult:
    return PruneResult(
        rows_deleted=42,
        cutoff_ts=datetime(2024, 4, 1, tzinfo=timezone.utc),
    )


def test_format_prune_result_zero(zero_result: PruneResult) -> None:
    msg = format_prune_result(zero_result)
    assert "0 records" in msg
    assert "2024-04-01" in msg


def test_format_prune_result_singular(one_result: PruneResult) -> None:
    msg = format_prune_result(one_result)
    assert "1 record" in msg
    assert "records" not in msg


def test_format_prune_result_plural(many_result: PruneResult) -> None:
    msg = format_prune_result(many_result)
    assert "42 records" in msg


def test_format_dry_run_prefix(many_result: PruneResult) -> None:
    msg = format_dry_run(many_result)
    assert msg.startswith("[DRY RUN]")


def test_format_dry_run_contains_count(many_result: PruneResult) -> None:
    msg = format_dry_run(many_result)
    assert "42" in msg


def test_format_dry_run_singular(one_result: PruneResult) -> None:
    msg = format_dry_run(one_result)
    assert "1 record" in msg
    assert "records" not in msg


def test_print_retention_report_live(capsys: pytest.CaptureFixture, many_result: PruneResult) -> None:
    print_retention_report(many_result, dry_run=False)
    captured = capsys.readouterr()
    assert "42" in captured.out
    assert "[DRY RUN]" not in captured.out


def test_print_retention_report_dry_run(capsys: pytest.CaptureFixture, many_result: PruneResult) -> None:
    print_retention_report(many_result, dry_run=True)
    captured = capsys.readouterr()
    assert "[DRY RUN]" in captured.out
