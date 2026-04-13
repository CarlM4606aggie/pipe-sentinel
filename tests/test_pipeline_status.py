"""Tests for pipe_sentinel.pipeline_status."""
import sqlite3
from datetime import datetime, timezone

import pytest

from pipe_sentinel.audit import init_db
from pipe_sentinel.pipeline_status import (
    PipelineStatus,
    _consecutive_failures,
    build_status_snapshot,
    format_snapshot,
)
from pipe_sentinel.audit import AuditRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rec(status: str, ts: str) -> AuditRecord:
    return AuditRecord(
        pipeline="p",
        status=status,
        started_at=ts,
        finished_at=ts,
        duration=1.0,
        exit_code=0 if status == "success" else 1,
        error=None,
    )


@pytest.fixture()
def db_path(tmp_path):
    p = tmp_path / "audit.db"
    init_db(str(p))
    return str(p)


def _insert(db: str, name: str, status: str, ts: str) -> None:
    con = sqlite3.connect(db)
    con.execute(
        "INSERT INTO runs (pipeline, status, started_at, finished_at, duration, exit_code, error)"
        " VALUES (?,?,?,?,?,?,?)",
        (name, status, ts, ts, 0.5, 0 if status == "success" else 1, None),
    )
    con.commit()
    con.close()


# ---------------------------------------------------------------------------
# Unit: _consecutive_failures
# ---------------------------------------------------------------------------

def test_consecutive_failures_none():
    records = [_rec("success", "2024-01-01T00:00:00")]
    assert _consecutive_failures(records) == 0


def test_consecutive_failures_all():
    records = [_rec("failure", "2024-01-01T00:00:00")] * 3
    assert _consecutive_failures(records) == 3


def test_consecutive_failures_stops_at_success():
    records = [
        _rec("failure", "2024-01-03T00:00:00"),
        _rec("failure", "2024-01-02T00:00:00"),
        _rec("success", "2024-01-01T00:00:00"),
    ]
    assert _consecutive_failures(records) == 2


# ---------------------------------------------------------------------------
# Unit: PipelineStatus helpers
# ---------------------------------------------------------------------------

def test_pipeline_status_is_healthy_true():
    s = PipelineStatus("etl", "success", "2024-01-01T00:00:00", 0)
    assert s.is_healthy is True


def test_pipeline_status_is_healthy_false():
    s = PipelineStatus("etl", "failure", "2024-01-01T00:00:00", 2)
    assert s.is_healthy is False


def test_pipeline_status_str_contains_name():
    s = PipelineStatus("my_pipeline", "success", "2024-01-01T00:00:00", 0)
    assert "my_pipeline" in str(s)


# ---------------------------------------------------------------------------
# Integration: build_status_snapshot
# ---------------------------------------------------------------------------

def test_snapshot_unknown_when_no_records(db_path):
    snap = build_status_snapshot(db_path, ["missing_pipe"])
    assert snap["missing_pipe"].last_status == "unknown"
    assert snap["missing_pipe"].consecutive_failures == 0


def test_snapshot_reflects_latest_run(db_path):
    _insert(db_path, "pipe_a", "success", "2024-06-01T10:00:00")
    _insert(db_path, "pipe_a", "failure", "2024-06-02T10:00:00")
    snap = build_status_snapshot(db_path, ["pipe_a"])
    assert snap["pipe_a"].last_status == "failure"


def test_snapshot_consecutive_failures(db_path):
    for ts in ["2024-06-01T10:00:00", "2024-06-02T10:00:00", "2024-06-03T10:00:00"]:
        _insert(db_path, "pipe_b", "failure", ts)
    snap = build_status_snapshot(db_path, ["pipe_b"])
    assert snap["pipe_b"].consecutive_failures == 3


# ---------------------------------------------------------------------------
# format_snapshot
# ---------------------------------------------------------------------------

def test_format_snapshot_empty():
    assert format_snapshot({}) == "No pipelines tracked."


def test_format_snapshot_contains_summary(db_path):
    _insert(db_path, "p1", "success", "2024-06-01T10:00:00")
    _insert(db_path, "p2", "failure", "2024-06-01T10:00:00")
    snap = build_status_snapshot(db_path, ["p1", "p2"])
    report = format_snapshot(snap)
    assert "Healthy: 1/2" in report
    assert "p1" in report
    assert "p2" in report
