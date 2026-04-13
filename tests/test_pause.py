"""Tests for pipe_sentinel.pause and pipe_sentinel.pause_report."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipe_sentinel.pause import PauseEntry, PauseStore
from pipe_sentinel.pause_report import build_pause_report, format_entry


@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "pause_state.json"


@pytest.fixture
def store(state_file: Path) -> PauseStore:
    s = PauseStore(path=state_file)
    s.load()
    return s


# ── PauseEntry ────────────────────────────────────────────────────────────────

def test_entry_active_when_no_resume_at():
    entry = PauseEntry(pipeline_name="etl", paused_at=time.time())
    assert entry.is_active() is True


def test_entry_active_before_resume_at():
    future = time.time() + 3600
    entry = PauseEntry(pipeline_name="etl", paused_at=time.time(), resume_at=future)
    assert entry.is_active() is True


def test_entry_inactive_after_resume_at():
    past = time.time() - 1
    entry = PauseEntry(pipeline_name="etl", paused_at=time.time() - 10, resume_at=past)
    assert entry.is_active() is False


def test_entry_roundtrip_serialisation():
    entry = PauseEntry(pipeline_name="load", paused_at=1_000_000.0, reason="maintenance", resume_at=1_003_600.0)
    restored = PauseEntry.from_dict(entry.to_dict())
    assert restored.pipeline_name == entry.pipeline_name
    assert restored.reason == entry.reason
    assert restored.resume_at == entry.resume_at


def test_entry_roundtrip_no_resume_at():
    entry = PauseEntry(pipeline_name="load", paused_at=1_000_000.0)
    restored = PauseEntry.from_dict(entry.to_dict())
    assert restored.resume_at is None


# ── PauseStore ────────────────────────────────────────────────────────────────

def test_not_paused_when_empty(store: PauseStore):
    assert store.is_paused("etl") is False


def test_paused_after_pause_call(store: PauseStore):
    store.pause("etl", reason="testing")
    assert store.is_paused("etl") is True


def test_not_paused_after_resume(store: PauseStore):
    store.pause("etl")
    store.resume("etl")
    assert store.is_paused("etl") is False


def test_resume_returns_false_when_not_paused(store: PauseStore):
    assert store.resume("nonexistent") is False


def test_state_persists_across_instances(state_file: Path):
    s1 = PauseStore(path=state_file)
    s1.load()
    s1.pause("etl", reason="deploy")

    s2 = PauseStore(path=state_file)
    s2.load()
    assert s2.is_paused("etl") is True


def test_active_entries_excludes_expired(store: PauseStore):
    now = time.time()
    store.pause("active_pipe", resume_at=now + 3600)
    store.pause("expired_pipe", resume_at=now - 1)
    active = store.active_entries(now)
    names = [e.pipeline_name for e in active]
    assert "active_pipe" in names
    assert "expired_pipe" not in names


# ── pause_report ──────────────────────────────────────────────────────────────

def test_format_entry_contains_name():
    entry = PauseEntry(pipeline_name="my_pipe", paused_at=1_700_000_000.0)
    result = format_entry(entry)
    assert "my_pipe" in result


def test_format_entry_shows_reason():
    entry = PauseEntry(pipeline_name="p", paused_at=1_700_000_000.0, reason="hotfix")
    assert "hotfix" in format_entry(entry)


def test_format_entry_indefinite_when_no_resume_at():
    entry = PauseEntry(pipeline_name="p", paused_at=1_700_000_000.0)
    assert "indefinite" in format_entry(entry)


def test_build_pause_report_empty():
    report = build_pause_report([])
    assert "No pause entries" in report


def test_build_pause_report_counts(store: PauseStore):
    now = time.time()
    entries = [
        PauseEntry(pipeline_name="a", paused_at=now, resume_at=now + 3600),
        PauseEntry(pipeline_name="b", paused_at=now - 7200, resume_at=now - 3600),
    ]
    report = build_pause_report(entries, now)
    assert "Active pauses : 1" in report
    assert "Expired pauses: 1" in report
