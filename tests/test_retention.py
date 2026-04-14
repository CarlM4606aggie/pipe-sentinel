"""Tests for pipe_sentinel.retention."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipe_sentinel.audit import init_db, record_run
from pipe_sentinel.retention import RetentionPolicy, PruneResult, prune_records, apply_retention
from pipe_sentinel.runner import RunResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(year: int, month: int, day: int) -> str:
    return f"{year:04d}-{month:02d}-{day:02d} 00:00:00"


def _insert_row(db_path: str, pipeline: str, status: str, ts: str) -> None:
    """Directly insert a row with a specific timestamp for testing."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO audit_log (pipeline, status, returncode, duration, timestamp)"
            " VALUES (?, ?, ?, ?, ?)",
            (pipeline, status, 0, 1.0, ts),
        )
        conn.commit()


def _count_rows(db_path: str, pipeline: str | None = None) -> int:
    """Return the number of rows in audit_log, optionally filtered by pipeline name."""
    with sqlite3.connect(db_path) as conn:
        if pipeline is not None:
            row = conn.execute(
                "SELECT COUNT(*) FROM audit_log WHERE pipeline = ?", (pipeline,)
            ).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()
    return row[0]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path: Path) -> str:
    path = str(tmp_path / "audit.db")
    init_db(path)
    return path


# ---------------------------------------------------------------------------
# RetentionPolicy tests
# ---------------------------------------------------------------------------

def test_cutoff_is_max_age_days_before_now() -> None:
    policy = RetentionPolicy(max_age_days=30)
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    cutoff = policy.cutoff(now)
    assert cutoff == datetime(2024, 5, 2, tzinfo=timezone.utc)


def test_cutoff_defaults_to_utc_now() -> None:
    policy = RetentionPolicy(max_age_days=7)
    cutoff = policy.cutoff()  # should not raise
    assert cutoff.tzinfo is not None


# ---------------------------------------------------------------------------
# prune_records tests
# ---------------------------------------------------------------------------

def test_prune_removes_old_records(db_path: str) -> None:
    _insert_row(db_path, "old_pipe", "success", _ts(2020, 1, 1))
    _insert_row(db_path, "new_pipe", "success", _ts(2099, 1, 1))

    policy = RetentionPolicy(max_age_days=30)
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    result = prune_records(db_path, policy, now=now)

    assert result.rows_deleted == 1
    assert _count_rows(db_path, "old_pipe") == 0
    assert _count_rows(db_path, "new_pipe") == 1


def test_prune_keeps_recent_records(db_path: str) -> None:
    _insert_row(db_path, "recent", "success", _ts(2099, 12, 31))

    policy = RetentionPolicy(max_age_days=90)
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    result = prune_records(db_path, policy, now=now)

    assert result.rows_deleted == 0
    assert _count_rows(db_path, "recent") == 1


def test_prune_returns_correct_cutoff(db_path: str) -> None:
    policy = RetentionPolicy(max_age_days=10)
    now = datetime(2024, 3, 20, tzinfo=timezone.utc)
   