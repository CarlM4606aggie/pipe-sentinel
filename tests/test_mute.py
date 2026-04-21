"""Tests for pipe_sentinel.mute."""
import json
import time
import pytest
from pathlib import Path
from pipe_sentinel.mute import MuteEntry, MuteStore


@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "mute.json"


@pytest.fixture
def store(state_file: Path) -> MuteStore:
    return MuteStore(path=state_file)


# --- MuteEntry ---

def test_entry_muted_indefinitely():
    e = MuteEntry(pipeline="p", muted_at=time.time(), duration_seconds=None)
    assert e.is_muted() is True


def test_entry_muted_within_duration():
    now = time.time()
    e = MuteEntry(pipeline="p", muted_at=now, duration_seconds=3600)
    assert e.is_muted(now=now + 10) is True


def test_entry_not_muted_after_duration():
    now = time.time()
    e = MuteEntry(pipeline="p", muted_at=now, duration_seconds=60)
    assert e.is_muted(now=now + 120) is False


def test_entry_expires_at_none_when_indefinite():
    e = MuteEntry(pipeline="p", muted_at=1000.0, duration_seconds=None)
    assert e.expires_at() is None


def test_entry_expires_at_computed():
    e = MuteEntry(pipeline="p", muted_at=1000.0, duration_seconds=500.0)
    assert e.expires_at() == 1500.0


def test_entry_roundtrip_serialisation():
    e = MuteEntry(pipeline="etl", muted_at=999.0, duration_seconds=300.0, reason="maintenance")
    restored = MuteEntry.from_dict(e.to_dict())
    assert restored.pipeline == e.pipeline
    assert restored.muted_at == e.muted_at
    assert restored.duration_seconds == e.duration_seconds
    assert restored.reason == e.reason


def test_entry_roundtrip_no_duration():
    e = MuteEntry(pipeline="etl", muted_at=999.0, duration_seconds=None)
    restored = MuteEntry.from_dict(e.to_dict())
    assert restored.duration_seconds is None


# --- MuteStore ---

def test_not_muted_when_empty(store: MuteStore):
    assert store.is_muted("pipeline_a") is False


def test_mute_pipeline(store: MuteStore, state_file: Path):
    now = time.time()
    store.mute("pipeline_a", duration_seconds=3600, now=now)
    assert store.is_muted("pipeline_a", now=now + 10) is True


def test_mute_persists_to_disk(state_file: Path):
    s1 = MuteStore(path=state_file)
    now = time.time()
    s1.mute("pipeline_b", duration_seconds=7200, reason="deploy", now=now)

    s2 = MuteStore(path=state_file)
    assert s2.is_muted("pipeline_b", now=now + 60) is True


def test_unmute_removes_entry(store: MuteStore):
    store.mute("pipeline_c", duration_seconds=3600)
    result = store.unmute("pipeline_c")
    assert result is True
    assert store.is_muted("pipeline_c") is False


def test_unmute_missing_returns_false(store: MuteStore):
    assert store.unmute("nonexistent") is False


def test_active_entries_excludes_expired(store: MuteStore):
    now = time.time()
    store.mute("active", duration_seconds=3600, now=now)
    store.mute("expired", duration_seconds=10, now=now - 100)
    active = store.active_entries(now=now)
    names = [e.pipeline for e in active]
    assert "active" in names
    assert "expired" not in names


def test_store_len(store: MuteStore):
    store.mute("p1", duration_seconds=60)
    store.mute("p2", duration_seconds=120)
    assert len(store) == 2
