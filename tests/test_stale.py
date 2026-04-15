"""Tests for pipe_sentinel.stale."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pytest

from pipe_sentinel.audit import init_db
from pipe_sentinel.stale import StaleResult, check_stale, scan_stale


@pytest.fixture()
def db_path(tmp_path: Path) -> str:
    path = str(tmp_path / "audit.db")
    init_db(path)
    return path


def _insert(db: str, name: str, started_at: datetime, status: str = "success") -> None:
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO runs (pipeline, status, started_at, duration, retries, output)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (name, status, started_at.isoformat(), 1.0, 0, ""),
    )
    conn.commit()
    conn.close()


class _FakePipeline:
    def __init__(self, name: str, max_age_hours: Optional[float] = None):
        self.name = name
        self.max_age_hours = max_age_hours


# ---------------------------------------------------------------------------
# check_stale
# ---------------------------------------------------------------------------

def test_check_stale_no_runs_is_stale(db_path: str) -> None:
    result = check_stale("missing", max_age_hours=1.0, db_path=db_path)
    assert result.is_stale is True
    assert result.last_run_at is None
    assert result.age_hours is None


def test_check_stale_recent_run_not_stale(db_path: str) -> None:
    recent = datetime.now(tz=timezone.utc) - timedelta(minutes=30)
    _insert(db_path, "etl_load", recent)
    result = check_stale("etl_load", max_age_hours=2.0, db_path=db_path)
    assert result.is_stale is False
    assert result.age_hours is not None
    assert result.age_hours < 2.0


def test_check_stale_old_run_is_stale(db_path: str) -> None:
    old = datetime.now(tz=timezone.utc) - timedelta(hours=5)
    _insert(db_path, "etl_load", old)
    result = check_stale("etl_load", max_age_hours=3.0, db_path=db_path)
    assert result.is_stale is True
    assert result.age_hours is not None
    assert result.age_hours > 3.0


def test_check_stale_returns_most_recent_run(db_path: str) -> None:
    old = datetime.now(tz=timezone.utc) - timedelta(hours=10)
    new = datetime.now(tz=timezone.utc) - timedelta(minutes=10)
    _insert(db_path, "pipe", old)
    _insert(db_path, "pipe", new)
    result = check_stale("pipe", max_age_hours=1.0, db_path=db_path)
    assert result.is_stale is False


# ---------------------------------------------------------------------------
# StaleResult.__str__
# ---------------------------------------------------------------------------

def test_str_never_run() -> None:
    r = StaleResult("etl", None, 2.0, None, True)
    assert "never run" in str(r)
    assert "STALE" in str(r)


def test_str_stale_shows_age() -> None:
    ts = datetime.now(tz=timezone.utc) - timedelta(hours=4)
    r = StaleResult("etl", ts, 2.0, 4.0, True)
    assert "STALE" in str(r)
    assert "4.0h" in str(r)


def test_str_ok_shows_ok() -> None:
    ts = datetime.now(tz=timezone.utc) - timedelta(minutes=30)
    r = StaleResult("etl", ts, 2.0, 0.5, False)
    assert "OK" in str(r)


# ---------------------------------------------------------------------------
# scan_stale
# ---------------------------------------------------------------------------

def test_scan_stale_skips_pipelines_without_max_age(db_path: str) -> None:
    pipelines = [_FakePipeline("no_age"), _FakePipeline("with_age", 1.0)]
    results = scan_stale(pipelines, db_path)
    assert len(results) == 1
    assert results[0].pipeline_name == "with_age"


def test_scan_stale_returns_all_configured(db_path: str) -> None:
    pipelines = [
        _FakePipeline("a", 1.0),
        _FakePipeline("b", 2.0),
        _FakePipeline("c", 3.0),
    ]
    results = scan_stale(pipelines, db_path)
    assert len(results) == 3
    assert all(r.is_stale for r in results)  # none have runs
