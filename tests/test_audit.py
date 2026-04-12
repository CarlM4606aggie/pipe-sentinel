"""Tests for pipe_sentinel.audit."""

import pytest
from datetime import datetime, timezone
from pipe_sentinel.audit import init_db, record_run, fetch_recent, AuditRecord
from pipe_sentinel.runner import RunResult


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "test_audit.db")
    init_db(path)
    return path


@pytest.fixture
def success_result() -> RunResult:
    return RunResult(
        pipeline_name="ingest_orders",
        success=True,
        returncode=0,
        stdout="done\n",
        stderr="",
        duration=2.5,
        attempts=1,
        started_at=datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def failure_result() -> RunResult:
    return RunResult(
        pipeline_name="transform_users",
        success=False,
        returncode=1,
        stdout="",
        stderr="Connection refused\n",
        duration=0.8,
        attempts=3,
        started_at=datetime(2024, 6, 1, 10, 5, 0, tzinfo=timezone.utc),
    )


def test_init_db_creates_table(db_path):
    records = fetch_recent(db_path)
    assert isinstance(records, list)


def test_record_run_stores_success(db_path, success_result):
    record_run(db_path, success_result)
    records = fetch_recent(db_path)
    assert len(records) == 1
    r = records[0]
    assert r.pipeline_name == "ingest_orders"
    assert r.status == "success"
    assert r.retries == 0
    assert r.error is None


def test_record_run_stores_failure(db_path, failure_result):
    record_run(db_path, failure_result)
    records = fetch_recent(db_path)
    assert len(records) == 1
    r = records[0]
    assert r.pipeline_name == "transform_users"
    assert r.status == "failure"
    assert r.retries == 2
    assert "Connection refused" in r.error


def test_fetch_recent_respects_limit(db_path, success_result):
    for _ in range(5):
        record_run(db_path, success_result)
    records = fetch_recent(db_path, limit=3)
    assert len(records) == 3


def test_fetch_recent_newest_first(db_path, success_result, failure_result):
    record_run(db_path, success_result)
    record_run(db_path, failure_result)
    records = fetch_recent(db_path)
    assert records[0].pipeline_name == "transform_users"
    assert records[1].pipeline_name == "ingest_orders"


def test_record_run_duration_stored(db_path, success_result):
    record_run(db_path, success_result)
    records = fetch_recent(db_path)
    assert abs(records[0].duration - 2.5) < 0.001
