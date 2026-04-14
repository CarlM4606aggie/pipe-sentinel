"""Tests for the dead-letter queue module."""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from pipe_sentinel.deadletter import DeadLetterEntry, DeadLetterStore
from pipe_sentinel.deadletter_collector import collect_failures, purge_recovered
from pipe_sentinel.deadletter_report import build_deadletter_report, format_entry
from pipe_sentinel.runner import RunResult


@pytest.fixture
def dl_path(tmp_path: Path) -> Path:
    return tmp_path / "deadletter.json"


@pytest.fixture
def store(dl_path: Path) -> DeadLetterStore:
    return DeadLetterStore(path=dl_path)


def _run(name: str, success: bool) -> RunResult:
    return RunResult(
        pipeline_name=name,
        command=f"run_{name}.sh",
        success=success,
        returncode=0 if success else 1,
        stdout="",
        stderr="" if success else f"{name} exploded",
        started_at=time.time(),
        finished_at=time.time(),
    )


# --- DeadLetterEntry ---

def test_entry_id_auto_generated():
    e = DeadLetterEntry("p", "cmd", 1_700_000_000.0, 1, "", 3)
    assert e.entry_id.startswith("p-")


def test_entry_roundtrip_serialisation():
    e = DeadLetterEntry("etl", "run.sh", 1_700_000_000.0, 2, "oops", 4, "etl-custom")
    restored = DeadLetterEntry.from_dict(e.to_dict())
    assert restored == e


def test_from_run_result_maps_fields():
    r = _run("load", success=False)
    e = DeadLetterEntry.from_run_result(r, attempts=2)
    assert e.pipeline_name == "load"
    assert e.returncode == 1
    assert e.attempts == 2
    assert "load exploded" in e.stderr


# --- DeadLetterStore ---

def test_store_starts_empty(store: DeadLetterStore):
    assert len(store) == 0


def test_push_persists_to_disk(store: DeadLetterStore, dl_path: Path):
    e = DeadLetterEntry.from_run_result(_run("x", False), attempts=1)
    store.push(e)
    raw = json.loads(dl_path.read_text())
    assert len(raw) == 1
    assert raw[0]["pipeline_name"] == "x"


def test_store_reloads_from_disk(dl_path: Path):
    s1 = DeadLetterStore(path=dl_path)
    s1.push(DeadLetterEntry.from_run_result(_run("y", False), attempts=1))
    s2 = DeadLetterStore(path=dl_path)
    assert len(s2) == 1


def test_remove_existing_entry(store: DeadLetterStore):
    e = DeadLetterEntry.from_run_result(_run("z", False), attempts=1)
    store.push(e)
    removed = store.remove(e.entry_id)
    assert removed is True
    assert len(store) == 0


def test_remove_nonexistent_returns_false(store: DeadLetterStore):
    assert store.remove("no-such-id") is False


def test_find_returns_entry(store: DeadLetterStore):
    e = DeadLetterEntry.from_run_result(_run("w", False), attempts=1)
    store.push(e)
    found = store.find(e.entry_id)
    assert found is not None
    assert found.pipeline_name == "w"


# --- Collector ---

def test_collect_failures_adds_only_failures(store: DeadLetterStore):
    results = [_run("a", True), _run("b", False), _run("c", False)]
    added = collect_failures(results, store, attempts=3)
    assert len(added) == 2
    assert len(store) == 2


def test_purge_recovered_removes_succeeded(store: DeadLetterStore):
    collect_failures([_run("d", False)], store)
    purged = purge_recovered([_run("d", True)], store)
    assert "d" in purged[0]
    assert len(store) == 0


# --- Report ---

def test_build_report_empty():
    report = build_deadletter_report([])
    assert "empty" in report.lower()


def test_build_report_contains_pipeline_name(store: DeadLetterStore):
    e = DeadLetterEntry.from_run_result(_run("pipeline_alpha", False), attempts=2)
    store.push(e)
    report = build_deadletter_report(store.all_entries())
    assert "pipeline_alpha" in report


def test_format_entry_contains_exit_code():
    e = DeadLetterEntry("p", "cmd", time.time(), 137, "killed", 1)
    text = format_entry(e)
    assert "137" in text
