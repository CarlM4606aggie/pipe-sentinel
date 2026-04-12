"""Tests for pipe_sentinel.watchdog."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipe_sentinel.audit import AuditRecord, init_db, record_run
from pipe_sentinel.config import PipelineConfig
from pipe_sentinel.runner import RunResult
from pipe_sentinel.watchdog import (
    WatchdogAlert,
    WatchdogReport,
    check_pipeline,
    run_watchdog,
    _latest_record,
)


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "audit.db")
    init_db(path)
    return path


@pytest.fixture
def pipeline_with_max_age():
    return PipelineConfig(
        name="etl-daily",
        command="python etl.py",
        retries=0,
        timeout=60,
        recipients=[],
        max_age_minutes=120,
    )


@pytest.fixture
def pipeline_no_max_age():
    return PipelineConfig(
        name="etl-adhoc",
        command="python adhoc.py",
        retries=0,
        timeout=60,
        recipients=[],
        max_age_minutes=None,
    )


def _make_result(name: str, success: bool, ran_at: datetime) -> RunResult:
    return RunResult(
        pipeline_name=name,
        success=success,
        returncode=0 if success else 1,
        stdout="",
        stderr="",
        duration=1.0,
        attempts=1,
        ran_at=ran_at,
    )


def test_latest_record_returns_most_recent():
    now = datetime(2024, 1, 10, 12, 0, 0)
    older = AuditRecord("p", True, 0, 1.0, 1, now - timedelta(hours=2))
    newer = AuditRecord("p", True, 0, 1.0, 1, now)
    assert _latest_record([older, newer]).ran_at == now


def test_latest_record_empty_returns_none():
    assert _latest_record([]) is None


def test_check_pipeline_no_max_age_skips(pipeline_no_max_age, db_path):
    alert = check_pipeline(pipeline_no_max_age, db_path)
    assert alert is None


def test_check_pipeline_no_history_returns_alert(pipeline_with_max_age, db_path):
    alert = check_pipeline(pipeline_with_max_age, db_path)
    assert alert is not None
    assert "No run history" in alert.reason


def test_check_pipeline_recent_run_no_alert(pipeline_with_max_age, db_path):
    now = datetime.now(tz=timezone.utc)
    result = _make_result(pipeline_with_max_age.name, True, now - timedelta(minutes=30))
    record_run(db_path, result)
    alert = check_pipeline(pipeline_with_max_age, db_path, now=now)
    assert alert is None


def test_check_pipeline_overdue_returns_alert(pipeline_with_max_age, db_path):
    now = datetime.now(tz=timezone.utc)
    result = _make_result(pipeline_with_max_age.name, True, now - timedelta(minutes=200))
    record_run(db_path, result)
    alert = check_pipeline(pipeline_with_max_age, db_path, now=now)
    assert alert is not None
    assert alert.overdue_by is not None
    assert alert.overdue_by > timedelta(minutes=0)


def test_run_watchdog_report_has_alerts(pipeline_with_max_age, db_path):
    report = run_watchdog([pipeline_with_max_age], db_path)
    assert report.has_alerts
    assert len(report.alerts) == 1


def test_watchdog_report_summary_no_alerts():
    report = WatchdogReport(alerts=[])
    assert "on schedule" in report.summary()


def test_watchdog_alert_str_includes_name():
    alert = WatchdogAlert(pipeline_name="my-pipe", reason="Too old.")
    assert "my-pipe" in str(alert)
