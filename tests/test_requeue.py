"""Tests for pipe_sentinel.requeue."""
import time
import pytest
from pathlib import Path
from pipe_sentinel.requeue import RequeueEntry, RequeueStore


@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "requeue.json"


@pytest.fixture
def store(state_file: Path) -> RequeueStore:
    return RequeueStore(state_file)


def test_empty_store_has_zero_length(store):
    assert len(store) == 0


def test_enqueue_adds_entry(store):
    store.enqueue("etl_load", delay_seconds=0, reason="timeout")
    assert len(store) == 1


def test_entry_ready_when_no_delay(store):
    entry = store.enqueue("etl_load", delay_seconds=0)
    assert entry.is_ready()


def test_entry_not_ready_with_future_delay(store):
    entry = store.enqueue("etl_load", delay_seconds=9999)
    assert not entry.is_ready()


def test_ready_returns_only_ready_entries(store):
    store.enqueue("a", delay_seconds=0)
    store.enqueue("b", delay_seconds=9999)
    ready = store.ready()
    assert len(ready) == 1
    assert ready[0].pipeline_name == "a"


def test_remove_deletes_entry(store):
    entry = store.enqueue("etl_load")
    store.remove(entry.entry_id)
    assert len(store) == 0


def test_remove_unknown_id_is_noop(store):
    store.enqueue("etl_load")
    store.remove("nonexistent-id")
    assert len(store) == 1


def test_persists_across_reload(state_file):
    s1 = RequeueStore(state_file)
    s1.enqueue("etl_load", reason="flap")
    s2 = RequeueStore(state_file)
    assert len(s2) == 1
    assert s2.all_entries()[0].pipeline_name == "etl_load"
    assert s2.all_entries()[0].reason == "flap"


def test_entry_roundtrip_serialisation():
    e = RequeueEntry(pipeline_name="x", queued_at=1.0, run_after=2.0, reason="test", attempts=3)
    assert RequeueEntry.from_dict(e.to_dict()) == e


def test_entry_id_auto_generated():
    e1 = RequeueEntry(pipeline_name="x", queued_at=1.0, run_after=1.0, reason="")
    e2 = RequeueEntry(pipeline_name="x", queued_at=1.0, run_after=1.0, reason="")
    assert e1.entry_id != e2.entry_id
