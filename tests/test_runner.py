"""Tests for pipe_sentinel.runner module."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from pipe_sentinel.config import PipelineConfig
from pipe_sentinel.runner import RunResult, run_pipeline, run_with_retries


@pytest.fixture
def basic_pipeline():
    return PipelineConfig(
        name="test-pipeline",
        command="echo hello",
        schedule="@daily",
        max_retries=2,
        retry_delay_seconds=0,
        timeout_seconds=30,
        alert_on_failure=True,
    )


@pytest.fixture
def failing_pipeline():
    return PipelineConfig(
        name="fail-pipeline",
        command="exit 1",
        schedule="@daily",
        max_retries=1,
        retry_delay_seconds=0,
        timeout_seconds=30,
        alert_on_failure=True,
    )


def test_run_pipeline_success(basic_pipeline):
    result = run_pipeline(basic_pipeline)
    assert result.success is True
    assert result.exit_code == 0
    assert result.pipeline_name == "test-pipeline"


def test_run_pipeline_failure(failing_pipeline):
    result = run_pipeline(failing_pipeline)
    assert result.success is False
    assert result.exit_code != 0


def test_run_pipeline_timeout():
    config = PipelineConfig(
        name="slow-pipeline",
        command="sleep 10",
        schedule="@daily",
        max_retries=0,
        retry_delay_seconds=0,
        timeout_seconds=1,
        alert_on_failure=True,
    )
    result = run_pipeline(config)
    assert result.success is False
    assert result.exit_code == -1
    assert "Timed out" in result.stderr


def test_run_pipeline_records_duration(basic_pipeline):
    result = run_pipeline(basic_pipeline)
    assert result.duration_seconds >= 0
    assert result.started_at <= result.finished_at


def test_run_with_retries_success_on_first_attempt(basic_pipeline):
    results = run_with_retries(basic_pipeline)
    assert len(results) == 1
    assert results[0].success is True
    assert results[0].attempt == 1


def test_run_with_retries_exhausts_retries(failing_pipeline):
    results = run_with_retries(failing_pipeline)
    # max_retries=1 means 1 initial + 1 retry = 2 total attempts
    assert len(results) == 2
    assert all(not r.success for r in results)
    assert results[0].attempt == 1
    assert results[1].attempt == 2


def test_run_with_retries_stops_on_success():
    call_count = 0
    original_run = __import__("pipe_sentinel.runner", fromlist=["run_pipeline"]).run_pipeline

    config = PipelineConfig(
        name="flaky-pipeline",
        command="true",
        schedule="@daily",
        max_retries=3,
        retry_delay_seconds=0,
        timeout_seconds=30,
        alert_on_failure=True,
    )

    results = run_with_retries(config)
    # Command is 'true' which always succeeds; should stop after first attempt
    assert len(results) == 1
    assert results[0].success is True
