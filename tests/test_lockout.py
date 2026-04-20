"""Tests for pipe_sentinel.lockout and pipe_sentinel.lockout_collector."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipe_sentinel.lockout import LockoutEntry, LockoutStore
from pipe_sentinel.lockout_collector import apply_failures, filter_blocked, store_from_path
from pipe_sentinel.lockout_report import build_lockout_report, format_entry
from pipe_sentinel.runner import RunResult


@pytest.fixture()
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "lockout.json"


@pytest.fixture()
def store(state_file: Path) -> LockoutStore:
    return LockoutStore(path=state_file)


def _run(pipeline: str, success: bool) -> RunResult:
    return RunResult(pipeline=pipeline, success=success, returncode=0 if success else 1,
                     stdout="", stderr="", duration=1.0)


# --- LockoutEntry ---

def test_entry_is_locked_within_duration() -> None:
    now = time.time()
    entry = LockoutEntry(pipeline="p", locked_at=now, duration_seconds=60, reason="test")
    assert entry.is_locked(now + 30)


def test_entry_not_locked_after_duration() -> None:
    now = time.time()
    entry = LockoutEntry(pipeline="p", locked_at=now, duration_seconds=60, reason="test")
    assert not entry.is_locked(now + 61)


def test_entry_remaining_seconds() -> None:
    now = 1_000_000.0
    entry = LockoutEntry(pipeline="p", locked_at=now, duration_seconds=120, reason="test")
    assert entry.remaining_seconds(now + 30) == pytest.approx(90.0)


def test_entry_remaining_seconds_expired() -> None:
    now = 1_000_000.0
    entry = LockoutEntry(pipeline="p", locked_at=now, duration_seconds=10, reason="test")
    assert entry.remaining_seconds(now + 20) == 0.0


def test_entry_roundtrip_serialisation() -> None:
    now = 1_000_000.0
    entry = LockoutEntry(pipeline="etl", locked_at=now, duration_seconds=300, reason="3 failures")
    assert LockoutEntry.from_dict(entry.to_dict()) == entry


# --- LockoutStore ---

def test_lock_creates_entry(store: LockoutStore) -> None:
    now = time.time()
    store.lock("pipe_a", 300, "too many failures", now=now)
    assert store.is_locked("pipe_a", now + 1)


def test_release_removes_entry(store: LockoutStore) -> None:
    now = time.time()
    store.lock("pipe_a", 300, "test", now=now)
    store.release("pipe_a")
    assert not store.is_locked("pipe_a", now + 1)


def test_release_missing_returns_false(store: LockoutStore) -> None:
    assert store.release("nonexistent") is False


def test_store_persists_to_disk(state_file: Path) -> None:
    now = 1_000_000.0
    s1 = LockoutStore(path=state_file)
    s1.lock("pipe_b", 600, "persisted", now=now)
    s2 = LockoutStore(path=state_file)
    assert s2.is_locked("pipe_b", now + 10)


def test_purge_expired_removes_old_entries(store: LockoutStore) -> None:
    now = 1_000_000.0
    store.lock("old_pipe", 10, "old", now=now)
    store.lock("new_pipe", 600, "new", now=now)
    removed = store.purge_expired(now=now + 20)
    assert removed == 1
    assert not store.is_locked("old_pipe", now + 20)
    assert store.is_locked("new_pipe", now + 20)


# --- lockout_collector ---

def test_apply_failures_locks_failing_pipeline(state_file: Path) -> None:
    store = LockoutStore(path=state_file)
    now = 1_000_000.0
    results = [_run("etl", False), _run("etl", False)]
    locked = apply_failures(store, results, duration_seconds=120, threshold=1, now=now)
    assert "etl" in locked
    assert store.is_locked("etl", now + 60)


def test_apply_failures_does_not_lock_passing(state_file: Path) -> None:
    store = LockoutStore(path=state_file)
    now = 1_000_000.0
    results = [_run("etl", True)]
    locked = apply_failures(store, results, threshold=1, now=now)
    assert locked == []


def test_apply_failures_respects_threshold(state_file: Path) -> None:
    store = LockoutStore(path=state_file)
    now = 1_000_000.0
    results = [_run("etl", False)]  # only 1 failure, threshold=2
    locked = apply_failures(store, results, threshold=2, now=now)
    assert locked == []


def test_filter_blocked_excludes_locked(state_file: Path) -> None:
    store = LockoutStore(path=state_file)
    now = 1_000_000.0
    store.lock("locked_pipe", 300, "test", now=now)

    class FakePipeline:
        def __init__(self, name: str) -> None:
            self.name = name

    pipelines = [FakePipeline("locked_pipe"), FakePipeline("free_pipe")]
    result = filter_blocked(store, pipelines, now=now + 1)
    assert len(result) == 1
    assert result[0].name == "free_pipe"


# --- lockout_report ---

def test_format_entry_locked_shows_remaining() -> None:
    now = 1_000_000.0
    entry = LockoutEntry(pipeline="etl", locked_at=now, duration_seconds=120, reason="failures")
    text = format_entry(entry, now=now + 30)
    assert "LOCKED" in text
    assert "90s remaining" in text


def test_build_lockout_report_empty(store: LockoutStore) -> None:
    report = build_lockout_report(store)
    assert "No lockout entries" in report


def test_build_lockout_report_shows_pipeline(store: LockoutStore) -> None:
    now = 1_000_000.0
    store.lock("my_pipeline", 300, "too many failures", now=now)
    report = build_lockout_report(store, now=now + 10)
    assert "my_pipeline" in report
    assert "LOCKED" in report
