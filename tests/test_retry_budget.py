"""Tests for pipe_sentinel.retry_budget."""
from __future__ import annotations

import json
import time

import pytest

from pipe_sentinel.retry_budget import (
    RetryBudgetConfig,
    RetryBudgetState,
    RetryBudgetStore,
)


@pytest.fixture()
def state_file(tmp_path):
    return tmp_path / "retry_budget.json"


@pytest.fixture()
def cfg():
    return RetryBudgetConfig(max_retries=3, window_seconds=60)


@pytest.fixture()
def store(state_file):
    return RetryBudgetStore(state_file)


def test_config_invalid_max_retries():
    with pytest.raises(ValueError):
        RetryBudgetConfig(max_retries=0, window_seconds=60)


def test_config_invalid_window():
    with pytest.raises(ValueError):
        RetryBudgetConfig(max_retries=3, window_seconds=0)


def test_not_exhausted_when_empty(store, cfg):
    state = store.get("pipe_a")
    assert not state.is_exhausted(cfg)


def test_exhausted_after_max_attempts(store, cfg):
    state = store.get("pipe_a")
    for _ in range(cfg.max_retries):
        state.record_attempt()
    assert state.is_exhausted(cfg)


def test_remaining_decrements(store, cfg):
    state = store.get("pipe_a")
    assert state.remaining(cfg) == 3
    state.record_attempt()
    assert state.remaining(cfg) == 2


def test_remaining_never_negative(store, cfg):
    state = store.get("pipe_a")
    for _ in range(10):
        state.record_attempt()
    assert state.remaining(cfg) == 0


def test_attempts_pruned_after_window(cfg):
    state = RetryBudgetState(pipeline="pipe_a")
    old_time = time.time() - cfg.window_seconds - 1
    state.attempts = [old_time, old_time]
    assert not state.is_exhausted(cfg)


def test_roundtrip_serialisation(state_file, cfg):
    store = RetryBudgetStore(state_file)
    state = store.get("pipe_b")
    state.record_attempt()
    store.save()

    store2 = RetryBudgetStore(state_file)
    loaded = store2.get("pipe_b")
    assert len(loaded.attempts) == 1


def test_store_len(store):
    store.get("a")
    store.get("b")
    assert len(store) == 2


def test_state_to_dict_roundtrip():
    s = RetryBudgetState(pipeline="x", attempts=[1.0, 2.0])
    d = s.to_dict()
    s2 = RetryBudgetState.from_dict(d)
    assert s2.pipeline == "x"
    assert s2.attempts == [1.0, 2.0]
