"""Tests for pipe_sentinel.throttle."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from pipe_sentinel.throttle import (
    ThrottleState,
    mark_alerted,
    should_alert,
)


@pytest.fixture()
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "throttle.json"


@pytest.fixture()
def state(state_file: Path) -> ThrottleState:
    s = ThrottleState(cooldown_seconds=60, state_file=state_file)
    s.load()
    return s


# ---------------------------------------------------------------------------
# ThrottleState.is_suppressed
# ---------------------------------------------------------------------------

def test_not_suppressed_when_no_prior_alert(state: ThrottleState) -> None:
    assert state.is_suppressed("my_pipeline") is False


def test_suppressed_immediately_after_alert(state: ThrottleState) -> None:
    state.record_alert("my_pipeline")
    assert state.is_suppressed("my_pipeline") is True


def test_not_suppressed_after_cooldown_expires(state: ThrottleState) -> None:
    state._timestamps["my_pipeline"] = time.time() - 120  # 2 min ago
    assert state.is_suppressed("my_pipeline") is False


def test_suppressed_just_before_cooldown_expires(state: ThrottleState) -> None:
    """Alert recorded 59 seconds ago should still be suppressed with a 60s cooldown."""
    state._timestamps["my_pipeline"] = time.time() - 59
    assert state.is_suppressed("my_pipeline") is True


# ---------------------------------------------------------------------------
# ThrottleState.load / save
# ---------------------------------------------------------------------------

def test_save_and_load_roundtrip(state: ThrottleState, state_file: Path) -> None:
    state.record_alert("pipe_a")
    state.save()

    new_state = ThrottleState(cooldown_seconds=60, state_file=state_file)
    new_state.load()
    assert new_state.is_suppressed("pipe_a") is True


def test_load_with_corrupt_file_resets_state(state: ThrottleState, state_file: Path) -> None:
    state_file.write_text("not valid json{{")
    state.load()
    assert state._timestamps == {}


def test_load_missing_file_is_noop(state: ThrottleState) -> None:
    # state_file does not exist yet — load should not raise
    state.load()
    assert state._timestamps == {}


# ---------------------------------------------------------------------------
# ThrottleState.clear
# ---------------------------------------------------------------------------

def test_clear_single_pipeline(state: ThrottleState) -> None:
    state.record_alert("pipe_a")
    state.record_alert("pipe_b")
    state.clear("pipe_a")
    assert "pipe_a" not in state._timestamps
    assert "pipe_b" in state._timestamps


def test_clear_all_pipelines(state: ThrottleState) -> None:
    state.record_alert("pipe_a")
    state.record_alert("pipe_b")
    state.clear()
    assert state._timestamps == {}


# ---------------------------------------------------------------------------
# should_alert / mark_alerted helpers
# ---------------------------------------------------------------------------

def test_should_alert_true_when_no_prior(state: ThrottleState) -> None:
    assert should_alert(state, "pipe_x") is True


def test_should_alert_false_after_mark(state: ThrottleState) -> None:
    mark_alerted(state, "pipe_x")
    assert should_alert(state, "pipe_x") is False
