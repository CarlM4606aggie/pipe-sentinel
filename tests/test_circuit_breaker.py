"""Tests for pipe_sentinel.circuit_breaker."""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from pipe_sentinel.circuit_breaker import CircuitBreaker, CircuitState


@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "cb_state.json"


@pytest.fixture
def breaker(state_file: Path) -> CircuitBreaker:
    return CircuitBreaker(state_file=state_file, threshold=3, recovery_seconds=60)


# ------------------------------------------------------------------
def test_circuit_starts_closed(breaker: CircuitBreaker) -> None:
    assert not breaker.is_open("my_pipeline")


def test_record_failure_increments(breaker: CircuitBreaker) -> None:
    breaker.record_failure("pipe")
    breaker.record_failure("pipe")
    state = breaker.all_states()["pipe"]
    assert state.failures == 2
    assert not state.is_open


def test_circuit_opens_at_threshold(breaker: CircuitBreaker) -> None:
    for _ in range(3):
        breaker.record_failure("pipe")
    assert breaker.is_open("pipe")


def test_circuit_not_open_below_threshold(breaker: CircuitBreaker) -> None:
    for _ in range(2):
        breaker.record_failure("pipe")
    assert not breaker.is_open("pipe")


def test_record_success_resets_state(breaker: CircuitBreaker) -> None:
    for _ in range(3):
        breaker.record_failure("pipe")
    assert breaker.is_open("pipe")
    breaker.record_success("pipe")
    assert not breaker.is_open("pipe")
    assert breaker.all_states()["pipe"].failures == 0


def test_circuit_half_open_after_recovery(breaker: CircuitBreaker) -> None:
    """After recovery window, is_open returns False (half-open)."""
    for _ in range(3):
        breaker.record_failure("pipe")
    # Manually backdate opened_at
    breaker._states["pipe"].opened_at = time.time() - 120
    breaker._save()
    assert not breaker.is_open("pipe")


def test_state_persisted_to_disk(state_file: Path) -> None:
    cb = CircuitBreaker(state_file=state_file, threshold=2, recovery_seconds=60)
    cb.record_failure("etl_load")
    cb.record_failure("etl_load")
    # Reload from disk
    cb2 = CircuitBreaker(state_file=state_file, threshold=2, recovery_seconds=60)
    assert cb2.is_open("etl_load")


def test_reset_removes_state(breaker: CircuitBreaker) -> None:
    for _ in range(3):
        breaker.record_failure("pipe")
    breaker.reset("pipe")
    assert "pipe" not in breaker.all_states()
    assert not breaker.is_open("pipe")


def test_circuit_state_to_from_dict() -> None:
    s = CircuitState(pipeline_name="x", failures=2, opened_at=1234567890.0)
    assert CircuitState.from_dict(s.to_dict()) == s
