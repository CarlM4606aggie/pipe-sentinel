"""Tests for pipe_sentinel.checkpoint."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from pipe_sentinel.checkpoint import CheckpointStore, store_from_path


@pytest.fixture()
def cp_path(tmp_path: Path) -> Path:
    return tmp_path / "checkpoints.json"


@pytest.fixture()
def store(cp_path: Path) -> CheckpointStore:
    return CheckpointStore(cp_path)


# ------------------------------------------------------------------
# record / last_success
# ------------------------------------------------------------------

def test_last_success_none_when_no_record(store: CheckpointStore) -> None:
    assert store.last_success("pipeline_a") is None


def test_record_stores_timestamp(store: CheckpointStore) -> None:
    ts = 1_700_000_000.0
    store.record("pipeline_a", ts=ts)
    assert store.last_success("pipeline_a") == ts


def test_record_uses_current_time_by_default(store: CheckpointStore) -> None:
    before = time.time()
    store.record("pipeline_b")
    after = time.time()
    ts = store.last_success("pipeline_b")
    assert ts is not None
    assert before <= ts <= after


def test_record_overwrites_previous(store: CheckpointStore) -> None:
    store.record("pipeline_a", ts=1_000.0)
    store.record("pipeline_a", ts=2_000.0)
    assert store.last_success("pipeline_a") == 2_000.0


# ------------------------------------------------------------------
# age_seconds
# ------------------------------------------------------------------

def test_age_seconds_none_when_no_record(store: CheckpointStore) -> None:
    assert store.age_seconds("pipeline_a") is None


def test_age_seconds_correct(store: CheckpointStore) -> None:
    now = 1_700_001_000.0
    store.record("pipeline_a", ts=now - 300)
    assert store.age_seconds("pipeline_a", now=now) == pytest.approx(300.0)


# ------------------------------------------------------------------
# clear
# ------------------------------------------------------------------

def test_clear_returns_true_when_existed(store: CheckpointStore) -> None:
    store.record("pipeline_a", ts=1_000.0)
    assert store.clear("pipeline_a") is True
    assert store.last_success("pipeline_a") is None


def test_clear_returns_false_when_missing(store: CheckpointStore) -> None:
    assert store.clear("nonexistent") is False


# ------------------------------------------------------------------
# persistence
# ------------------------------------------------------------------

def test_data_persisted_to_disk(cp_path: Path) -> None:
    s1 = CheckpointStore(cp_path)
    s1.record("pipeline_a", ts=9_999.0)

    s2 = CheckpointStore(cp_path)  # reload from disk
    assert s2.last_success("pipeline_a") == 9_999.0


def test_all_checkpoints_returns_copy(store: CheckpointStore) -> None:
    store.record("p1", ts=1.0)
    store.record("p2", ts=2.0)
    cp = store.all_checkpoints()
    assert cp == {"p1": 1.0, "p2": 2.0}


def test_store_from_path_helper(tmp_path: Path) -> None:
    p = str(tmp_path / "cp.json")
    s = store_from_path(p)
    assert isinstance(s, CheckpointStore)


def test_corrupt_file_loads_empty(cp_path: Path) -> None:
    cp_path.write_text("not valid json")
    s = CheckpointStore(cp_path)
    assert s.all_checkpoints() == {}
