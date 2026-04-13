"""Tests for pipe_sentinel.circuit_breaker_report."""
from __future__ import annotations

import time

import pytest

from pipe_sentinel.circuit_breaker import CircuitState
from pipe_sentinel.circuit_breaker_report import (
    build_circuit_report,
    format_state,
)


@pytest.fixture
def closed_state() -> CircuitState:
    return CircuitState(pipeline_name="etl_load", failures=0, opened_at=None)


@pytest.fixture
def degraded_state() -> CircuitState:
    return CircuitState(pipeline_name="etl_transform", failures=2, opened_at=None)


@pytest.fixture
def open_state() -> CircuitState:
    return CircuitState(
        pipeline_name="etl_export",
        failures=3,
        opened_at=time.time() - 30,
    )


def test_format_closed_state(closed_state: CircuitState) -> None:
    line = format_state(closed_state)
    assert "🟢" in line
    assert "CLOSED" in line
    assert "etl_load" in line


def test_format_degraded_state(degraded_state: CircuitState) -> None:
    line = format_state(degraded_state)
    assert "🟡" in line
    assert "failures=2" in line


def test_format_open_state(open_state: CircuitState) -> None:
    line = format_state(open_state, recovery_seconds=60)
    assert "🔴" in line
    assert "OPEN" in line
    assert "recovery in" in line


def test_build_report_empty() -> None:
    report = build_circuit_report({})
    assert "No circuit breaker" in report


def test_build_report_counts(
    closed_state: CircuitState,
    open_state: CircuitState,
) -> None:
    states = {"etl_load": closed_state, "etl_export": open_state}
    report = build_circuit_report(states, recovery_seconds=60)
    assert "Pipelines tracked : 2" in report
    assert "Open circuits     : 1" in report


def test_build_report_lists_all_pipelines(
    closed_state: CircuitState,
    open_state: CircuitState,
) -> None:
    states = {"etl_load": closed_state, "etl_export": open_state}
    report = build_circuit_report(states)
    assert "etl_load" in report
    assert "etl_export" in report
