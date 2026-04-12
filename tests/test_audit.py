"""Tests for pipe_sentinel.audit module."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock

from pipe_sentinel.audit import (
    init_db,
    record_run,
    fetch_recent,
    AuditRecord,
)
from pipe_sentinel.runner import RunResult


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test_audit.db"


@pytest.fixture
def success_result() -> RunResult:
    return RunResult(
        pipeline_name="etl_daily",
        success=True,
        exit_code=0,
        stdout="Done",
        stderr="",
        attempts=1,
        duration_seconds=1.23,
    )


@pytest.fixture
def failure_result() -> RunResult:
    return RunResult(
        pipeline_name="etl_daily",
        success=False,
        exit_code=1,
        stdout="",
        stderr="Error occurred",
        attempts=3,
        duration_seconds=5.67,
    )


def test_init_db_creates_table(db_path: Path) -> None:
    init_db(db_path)
    assert db_path.exists()


def test_record_run_stores_success(db_path: Path, success_result: RunResult) -> None:
    record_run(success_result, db_path=db_path)
    records = fetch_recent("etl_daily", db_path=db_path)
    assert len(records) == 1
    rec = records[0]
    assert rec.success is True
    assert rec.exit_code == 0
    assert rec.stdout == "Done"
    assert rec.attempts == 1
    assert rec.pipeline_name == "etl_daily"


def test_record_run_stores_failure(db_path: Path, failure_result: RunResult) -> None:
    record_run(failure_result, db_path=db_path)
    records = fetch_recent("etl_daily", db_path=db_path)
    assert len(records) == 1
    rec = records[0]
    assert rec.success is False
    assert rec.exit_code == 1
    assert rec.stderr == "Error occurred"
    assert rec.attempts == 3


def test_fetch_recent_respects_limit(db_path: Path, success_result: RunResult) -> None:
    for _ in range(5):
        record_run(success_result, db_path=db_path)
    records = fetch_recent("etl_daily", limit=3, db_path=db_path)
    assert len(records) == 3


def test_fetch_recent_returns_newest_first(db_path: Path, failure_result: RunResult, success_result: RunResult) -> None:
    record_run(failure_result, db_path=db_path)
    record_run(success_result, db_path=db_path)
    records = fetch_recent("etl_daily", db_path=db_path)
    # Most recent (success) should come first
    assert records[0].success is True
    assert records[1].success is False


def test_fetch_recent_filters_by_pipeline(db_path: Path) -> None:
    r1 = RunResult(pipeline_name="pipe_a", success=True, exit_code=0, stdout="", stderr="", attempts=1, duration_seconds=0.5)
    r2 = RunResult(pipeline_name="pipe_b", success=False, exit_code=1, stdout="", stderr="fail", attempts=2, duration_seconds=1.0)
    record_run(r1, db_path=db_path)
    record_run(r2, db_path=db_path)
    records = fetch_recent("pipe_a", db_path=db_path)
    assert all(r.pipeline_name == "pipe_a" for r in records)
    assert len(records) == 1
