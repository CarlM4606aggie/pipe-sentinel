"""Tests for pipe_sentinel.heartbeat."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipe_sentinel.audit import init_db
from pipe_sentinel.heartbeat import (
    HeartbeatResult,
    check_heartbeat,
    scan_heartbeats,
    _latest_record,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path: Path) -> str:
    path = str(tmp_path / "audit.db")
    init_db(path)
    return path


def _insert(db_path: str, pipeline: str, started_at: datetime, success: bool = True) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO runs (pipeline_name, started_at, duration_seconds, success, exit_code, error)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (pipeline, started_at.isoformat(), 1.0, int(success), 0 if success else 1, None),
    )
    conn.commit()
    conn.close()


class _FakePipeline:
    def __init__(self, name: str, max_silence_hours: float | None = None):
        self.name = name
        self.max_silence_hours = max_silence_hours


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# _latest_record
# ---------------------------------------------------------------------------

def test_latest_record_returns_none_for_empty():
    assert _latest_record([]) is None


# ---------------------------------------------------------------------------
# check_heartbeat
# ---------------------------------------------------------------------------

def test_check_heartbeat_missing_when_no_runs(db_path: str):
    result = check_heartbeat("pipe_a", max_silence_hours=2.0, db_path=db_path, now=NOW)
    assert result.missing is True
    assert result.is_silent is True
    assert result.last_run_at is None
    assert result.silent_hours is None


def test_check_heartbeat_ok_when_recent_run(db_path: str):
    recent = NOW - timedelta(hours=1)
    _insert(db_path, "pipe_a", recent)
    result = check_heartbeat("pipe_a", max_silence_hours=2.0, db_path=db_path, now=NOW)
    assert result.missing is False
    assert result.is_silent is False
    assert result.silent_hours == pytest.approx(1.0, abs=0.01)


def test_check_heartbeat_silent_when_old_run(db_path: str):
    old = NOW - timedelta(hours=5)
    _insert(db_path, "pipe_a", old)
    result = check_heartbeat("pipe_a", max_silence_hours=2.0, db_path=db_path, now=NOW)
    assert result.is_silent is True
    assert result.silent_hours == pytest.approx(5.0, abs=0.01)


def test_check_heartbeat_uses_most_recent_of_multiple_runs(db_path: str):
    _insert(db_path, "pipe_a", NOW - timedelta(hours=6))
    _insert(db_path, "pipe_a", NOW - timedelta(hours=1))
    result = check_heartbeat("pipe_a", max_silence_hours=3.0, db_path=db_path, now=NOW)
    assert result.silent_hours == pytest.approx(1.0, abs=0.01)
    assert result.is_silent is False


# ---------------------------------------------------------------------------
# __str__
# ---------------------------------------------------------------------------

def test_str_missing():
    r = HeartbeatResult("p", None, 2.0, None, True)
    assert "MISSING" in str(r)
    assert "p" in str(r)


def test_str_silent():
    r = HeartbeatResult("p", NOW, 2.0, 4.5, False)
    assert "!" in str(r)


def test_str_ok():
    r = HeartbeatResult("p", NOW, 2.0, 0.5, False)
    assert "✓" in str(r)


# ---------------------------------------------------------------------------
# scan_heartbeats
# ---------------------------------------------------------------------------

def test_scan_heartbeats_skips_pipelines_without_max_silence(db_path: str):
    pipelines = [_FakePipeline("no_limit", None)]
    results = scan_heartbeats(pipelines, db_path, now=NOW)
    assert results == []


def test_scan_heartbeats_returns_result_per_configured_pipeline(db_path: str):
    _insert(db_path, "pipe_a", NOW - timedelta(hours=0.5))
    pipelines = [
        _FakePipeline("pipe_a", max_silence_hours=2.0),
        _FakePipeline("pipe_b", None),
        _FakePipeline("pipe_c", max_silence_hours=1.0),
    ]
    results = scan_heartbeats(pipelines, db_path, now=NOW)
    assert len(results) == 2
    names = {r.pipeline_name for r in results}
    assert names == {"pipe_a", "pipe_c"}
