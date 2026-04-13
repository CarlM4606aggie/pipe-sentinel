"""Tests for pipe_sentinel.cooldown."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from pipe_sentinel.cooldown import CooldownEntry, CooldownStore


@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "cooldown.json"


@pytest.fixture
def store(state_file: Path) -> CooldownStore:
    return CooldownStore(path=state_file)


def test_not_cooling_when_no_entry(store: CooldownStore) -> None:
    assert store.is_cooling("pipeline_a") is False


def test_cooling_immediately_after_failure(store: CooldownStore) -> None:
    now = time.time()
    store.record_failure("pipeline_a", cooldown_seconds=300, now=now)
    assert store.is_cooling("pipeline_a", now=now + 1) is True


def test_not_cooling_after_cooldown_expires(store: CooldownStore) -> None:
    now = time.time()
    store.record_failure("pipeline_a", cooldown_seconds=300, now=now)
    assert store.is_cooling("pipeline_a", now=now + 301) is False


def test_remaining_seconds_decreases(store: CooldownStore) -> None:
    now = time.time()
    store.record_failure("pipeline_a", cooldown_seconds=300, now=now)
    entry = store.get("pipeline_a")
    assert entry is not None
    remaining = entry.remaining_seconds(now=now + 100)
    assert abs(remaining - 200.0) < 0.01


def test_remaining_seconds_zero_when_expired(store: CooldownStore) -> None:
    now = time.time()
    store.record_failure("pipeline_a", cooldown_seconds=60, now=now)
    entry = store.get("pipeline_a")
    assert entry.remaining_seconds(now=now + 120) == 0.0


def test_clear_removes_entry(store: CooldownStore) -> None:
    now = time.time()
    store.record_failure("pipeline_a", cooldown_seconds=300, now=now)
    store.clear("pipeline_a")
    assert store.is_cooling("pipeline_a") is False
    assert store.get("pipeline_a") is None


def test_persists_to_disk(state_file: Path) -> None:
    now = time.time()
    s1 = CooldownStore(path=state_file)
    s1.record_failure("pipeline_b", cooldown_seconds=120, now=now)
    s2 = CooldownStore(path=state_file)
    assert s2.is_cooling("pipeline_b", now=now + 10) is True


def test_all_entries_returns_list(store: CooldownStore) -> None:
    now = time.time()
    store.record_failure("p1", 300, now=now)
    store.record_failure("p2", 60, now=now)
    entries = store.all_entries()
    assert len(entries) == 2
    names = {e.pipeline_name for e in entries}
    assert names == {"p1", "p2"}


def test_entry_roundtrip_serialisation() -> None:
    now = time.time()
    entry = CooldownEntry(pipeline_name="pipe_x", failed_at=now, cooldown_seconds=180)
    restored = CooldownEntry.from_dict(entry.to_dict())
    assert restored.pipeline_name == entry.pipeline_name
    assert abs(restored.failed_at - entry.failed_at) < 0.001
    assert restored.cooldown_seconds == entry.cooldown_seconds
