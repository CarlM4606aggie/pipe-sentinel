"""Tests for pipe_sentinel.health module."""

from __future__ import annotations

import pytest
from unittest.mock import patch

from pipe_sentinel.config import PipelineConfig, SmtpConfig, SentinelConfig
from pipe_sentinel.health import (
    HealthResult,
    check_command_exists,
    check_timeout_positive,
    run_health_checks,
    print_health_report,
)


@pytest.fixture
def good_pipeline() -> PipelineConfig:
    return PipelineConfig(
        name="good",
        command="python -m pytest",
        schedule="@daily",
        retries=1,
        timeout_seconds=30,
        recipients=["ops@example.com"],
    )


@pytest.fixture
def bad_pipeline() -> PipelineConfig:
    return PipelineConfig(
        name="bad",
        command="nonexistent_tool_xyz --run",
        schedule="@daily",
        retries=0,
        timeout_seconds=-1,
        recipients=[],
    )


@pytest.fixture
def sentinel_config(good_pipeline, bad_pipeline) -> SentinelConfig:
    smtp = SmtpConfig(host="localhost", port=25, sender="a@b.com")
    return SentinelConfig(smtp=smtp, pipelines=[good_pipeline, bad_pipeline])


def test_check_command_exists_success(good_pipeline):
    with patch("pipe_sentinel.health.shutil.which", return_value="/usr/bin/python"):
        result = check_command_exists(good_pipeline)
    assert result.healthy
    assert result.checks["command_on_path"] is True


def test_check_command_exists_failure(bad_pipeline):
    with patch("pipe_sentinel.health.shutil.which", return_value=None):
        result = check_command_exists(bad_pipeline)
    assert not result.healthy
    assert result.checks["command_on_path"] is False
    assert any("not found" in e for e in result.errors)


def test_check_timeout_positive_success(good_pipeline):
    result = check_timeout_positive(good_pipeline)
    assert result.healthy
    assert result.checks["timeout_positive"] is True


def test_check_timeout_positive_failure(bad_pipeline):
    result = check_timeout_positive(bad_pipeline)
    assert not result.healthy
    assert result.checks["timeout_positive"] is False
    assert any("timeout_seconds" in e for e in result.errors)


def test_run_health_checks_returns_one_per_pipeline(sentinel_config):
    with patch("pipe_sentinel.health.shutil.which", side_effect=lambda x: "/bin/" + x):
        results = run_health_checks(sentinel_config)
    assert len(results) == 2


def test_run_health_checks_good_pipeline_healthy(sentinel_config):
    with patch("pipe_sentinel.health.shutil.which", return_value="/usr/bin/python"):
        results = run_health_checks(sentinel_config)
    good = next(r for r in results if r.pipeline_name == "good")
    assert good.healthy


def test_run_health_checks_bad_pipeline_unhealthy(sentinel_config):
    with patch("pipe_sentinel.health.shutil.which", return_value=None):
        results = run_health_checks(sentinel_config)
    bad = next(r for r in results if r.pipeline_name == "bad")
    assert not bad.healthy
    assert len(bad.errors) >= 2


def test_print_health_report_outputs_status(sentinel_config, capsys):
    results = [
        HealthResult(pipeline_name="demo", checks={"command_on_path": True}, errors=[])
    ]
    print_health_report(results)
    captured = capsys.readouterr()
    assert "OK" in captured.out
    assert "demo" in captured.out
