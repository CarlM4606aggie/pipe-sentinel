"""Tests for pipe_sentinel.debounce and pipe_sentinel.debounce_report."""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from pipe_sentinel.debounce import DebounceEntry, DebounceStore
from pipe_sentinel.debounce_report import (
    build_debounce_report,
    format_entry,
)


@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "debounce.json"


@pytest.fixture
def store(state_file: Path) -> DebounceStore:
    return DebounceStore(state_file)


NOW = 1_000_000.0
WINDOW = 60.0


# ---------------------------------------------------------------------------
# DebounceEntry
# ---------------------------------------------------------------------------

def test_not_debounced_when_no_entry(store: DebounceStore) -> None:
    assert store.is_debounced("pipe_a", now=NOW) is False


def test_debounced_immediately_after_alert(store: DebounceStore) -> None:
    store.record_alert("pipe_a", WINDOW, now=NOW)
    assert store.is_debounced("pipe_a", now=NOW) is True


def test_not_debounced_after_window_expires(store: DebounceStore) -> None:
    store.record_alert("pipe_a", WINDOW, now=NOW)
    assert store.is_debounced("pipe_a", now=NOW + WINDOW + 1) is False


def test_debounced_just_before_window_expires(store: DebounceStore) -> None:
    store.record_alert("pipe_a", WINDOW, now=NOW)
    assert store.is_debounced("pipe_a", now=NOW + WINDOW - 1) is True


def test_clear_removes_entry(store: DebounceStore) -> None:
    store.record_alert("pipe_a", WINDOW, now=NOW)
    store.clear("pipe_a")
    assert store.is_debounced("pipe_a", now=NOW) is False


def test_clear_missing_entry_is_noop(store: DebounceStore) -> None:
    store.clear("nonexistent")  # should not raise


def test_state_persisted_to_disk(state_file: Path) -> None:
    store = DebounceStore(state_file)
    store.record_alert("pipe_b", 30.0, now=NOW)
    raw = json.loads(state_file.read_text())
    assert "pipe_b" in raw
    assert raw["pipe_b"]["window_seconds"] == 30.0


def test_state_loaded_from_disk(state_file: Path) -> None:
    s1 = DebounceStore(state_file)
    s1.record_alert("pipe_c", WINDOW, now=NOW)
    s2 = DebounceStore(state_file)
    assert s2.is_debounced("pipe_c", now=NOW) is True


def test_entry_roundtrip() -> None:
    e = DebounceEntry(pipeline="p", last_alert_at=NOW, window_seconds=120.0)
    assert DebounceEntry.from_dict(e.to_dict()) == e


# ---------------------------------------------------------------------------
# debounce_report
# ---------------------------------------------------------------------------

def test_format_entry_debounced(store: DebounceStore) -> None:
    store.record_alert("pipe_x", WINDOW, now=NOW)
    entry = store._entries["pipe_x"]
    result = format_entry(entry, now=NOW + 10)
    assert "🔇" in result
    assert "pipe_x" in result
    assert "remaining" in result


def test_format_entry_ready(store: DebounceStore) -> None:
    store.record_alert("pipe_x", WINDOW, now=NOW)
    entry = store._entries["pipe_x"]
    result = format_entry(entry, now=NOW + WINDOW + 5)
    assert "🔔" in result
    assert "ready" in result


def test_build_debounce_report_empty(store: DebounceStore) -> None:
    report = build_debounce_report(store, now=NOW)
    assert "no entries" in report


def test_build_debounce_report_shows_pipeline(store: DebounceStore) -> None:
    store.record_alert("pipe_alpha", WINDOW, now=NOW)
    report = build_debounce_report(store, now=NOW)
    assert "pipe_alpha" in report
    assert "1 pipeline" in report
