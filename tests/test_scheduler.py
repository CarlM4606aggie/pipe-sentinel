"""Tests for pipe_sentinel.scheduler."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipe_sentinel.config import PipelineConfig, SentinelConfig, SmtpConfig
from pipe_sentinel.runner import RunResult
from pipe_sentinel.scheduler import ScheduleReport, run_all


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def smtp_cfg() -> SmtpConfig:
    return SmtpConfig(host="smtp.example.com", port=587, username="u", password="p", from_addr="no-reply@example.com")


@pytest.fixture()
def passing_pipeline() -> PipelineConfig:
    return PipelineConfig(name="pass", command="echo ok", retries=0, timeout=10, recipients=["ops@example.com"])


@pytest.fixture()
def failing_pipeline() -> PipelineConfig:
    return PipelineConfig(name="fail", command="false", retries=1, timeout=10, recipients=["ops@example.com"])


# ---------------------------------------------------------------------------
# ScheduleReport
# ---------------------------------------------------------------------------


def test_schedule_report_all_passed_when_no_failures():
    report = ScheduleReport(total=2, succeeded=2, failed=0)
    assert report.all_passed is True


def test_schedule_report_not_all_passed_when_failures():
    report = ScheduleReport(total=2, succeeded=1, failed=1)
    assert report.all_passed is False


# ---------------------------------------------------------------------------
# run_all — dry_run
# ---------------------------------------------------------------------------


def test_run_all_dry_run_skips_execution(passing_pipeline, smtp_cfg):
    config = SentinelConfig(pipelines=[passing_pipeline], smtp=smtp_cfg)
    with patch("pipe_sentinel.scheduler.run_with_retries") as mock_run:
        report = run_all(config, dry_run=True)
    mock_run.assert_not_called()
    assert report.results == []


# ---------------------------------------------------------------------------
# run_all — success path
# ---------------------------------------------------------------------------


def test_run_all_success_increments_succeeded(passing_pipeline, smtp_cfg):
    success_result = RunResult(pipeline_name="pass", success=True, attempts=1, returncode=0, stdout="ok", stderr="", duration=0.1)
    config = SentinelConfig(pipelines=[passing_pipeline], smtp=smtp_cfg)
    with patch("pipe_sentinel.scheduler.run_with_retries", return_value=success_result):
        with patch("pipe_sentinel.scheduler.notify_recipients") as mock_notify:
            report = run_all(config)
    assert report.succeeded == 1
    assert report.failed == 0
    mock_notify.assert_not_called()


# ---------------------------------------------------------------------------
# run_all — failure path
# ---------------------------------------------------------------------------


def test_run_all_failure_increments_failed_and_notifies(failing_pipeline, smtp_cfg):
    fail_result = RunResult(pipeline_name="fail", success=False, attempts=2, returncode=1, stdout="", stderr="err", duration=0.5)
    config = SentinelConfig(pipelines=[failing_pipeline], smtp=smtp_cfg)
    with patch("pipe_sentinel.scheduler.run_with_retries", return_value=fail_result):
        with patch("pipe_sentinel.scheduler.notify_recipients") as mock_notify:
            report = run_all(config)
    assert report.failed == 1
    assert report.succeeded == 0
    mock_notify.assert_called_once()


def test_run_all_no_notification_when_no_smtp(failing_pipeline):
    fail_result = RunResult(pipeline_name="fail", success=False, attempts=1, returncode=1, stdout="", stderr="", duration=0.1)
    config = SentinelConfig(pipelines=[failing_pipeline], smtp=None)
    with patch("pipe_sentinel.scheduler.run_with_retries", return_value=fail_result):
        with patch("pipe_sentinel.scheduler.notify_recipients") as mock_notify:
            report = run_all(config)
    mock_notify.assert_not_called()
    assert report.failed == 1
