"""Tests for pipe_sentinel.blackout."""
from __future__ import annotations

import json
from datetime import datetime, time
from pathlib import Path

import pytest

from pipe_sentinel.blackout import BlackoutStore, BlackoutWindow


@pytest.fixture()
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "blackout.json"


@pytest.fixture()
def store(state_file: Path) -> BlackoutStore:
    return BlackoutStore(path=state_file)


# ---------------------------------------------------------------------------
# BlackoutWindow.is_active
# ---------------------------------------------------------------------------

class TestIsActive:
    def _window(self, start: str, end: str) -> BlackoutWindow:
        return BlackoutWindow(
            pipeline="etl",
            start=time.fromisoformat(start),
            end=time.fromisoformat(end),
        )

    def test_active_within_normal_window(self) -> None:
        w = self._window("02:00", "04:00")
        assert w.is_active(datetime(2024, 1, 1, 3, 0)) is True

    def test_inactive_outside_normal_window(self) -> None:
        w = self._window("02:00", "04:00")
        assert w.is_active(datetime(2024, 1, 1, 5, 0)) is False

    def test_active_in_overnight_window_before_midnight(self) -> None:
        w = self._window("23:00", "01:00")
        assert w.is_active(datetime(2024, 1, 1, 23, 30)) is True

    def test_active_in_overnight_window_after_midnight(self) -> None:
        w = self._window("23:00", "01:00")
        assert w.is_active(datetime(2024, 1, 2, 0, 30)) is True

    def test_inactive_outside_overnight_window(self) -> None:
        w = self._window("23:00", "01:00")
        assert w.is_active(datetime(2024, 1, 1, 12, 0)) is False


# ---------------------------------------------------------------------------
# BlackoutWindow.covers
# ---------------------------------------------------------------------------

def test_covers_specific_pipeline() -> None:
    w = BlackoutWindow(pipeline="etl", start=time(2, 0), end=time(4, 0))
    assert w.covers("etl") is True
    assert w.covers("other") is False


def test_catch_all_covers_any_pipeline() -> None:
    w = BlackoutWindow(pipeline="*", start=time(2, 0), end=time(4, 0))
    assert w.covers("anything") is True


# ---------------------------------------------------------------------------
# BlackoutWindow serialisation
# ---------------------------------------------------------------------------

def test_roundtrip_serialisation() -> None:
    w = BlackoutWindow(pipeline="etl", start=time(1, 30), end=time(3, 0), reason="maintenance")
    restored = BlackoutWindow.from_dict(w.to_dict())
    assert restored.pipeline == w.pipeline
    assert restored.start == w.start
    assert restored.end == w.end
    assert restored.reason == w.reason


def test_str_includes_times() -> None:
    w = BlackoutWindow(pipeline="etl", start=time(2, 0), end=time(4, 0))
    assert "02:00" in str(w)
    assert "04:00" in str(w)


# ---------------------------------------------------------------------------
# BlackoutStore
# ---------------------------------------------------------------------------

def test_empty_store_has_zero_length(store: BlackoutStore) -> None:
    assert len(store) == 0


def test_add_persists_window(store: BlackoutStore, state_file: Path) -> None:
    w = BlackoutWindow(pipeline="etl", start=time(2, 0), end=time(4, 0))
    store.add(w)
    assert len(store) == 1
    data = json.loads(state_file.read_text())
    assert len(data["windows"]) == 1


def test_store_loads_from_existing_file(state_file: Path) -> None:
    state_file.write_text(json.dumps({"windows": [{"pipeline": "etl", "start": "02:00", "end": "04:00", "reason": ""}]}))
    store = BlackoutStore(path=state_file)
    assert len(store) == 1


def test_is_blacked_out_when_active(store: BlackoutStore) -> None:
    w = BlackoutWindow(pipeline="etl", start=time(0, 0), end=time(23, 59))
    store.add(w)
    assert store.is_blacked_out("etl") is True


def test_not_blacked_out_for_different_pipeline(store: BlackoutStore) -> None:
    w = BlackoutWindow(pipeline="etl", start=time(0, 0), end=time(23, 59))
    store.add(w)
    assert store.is_blacked_out("other") is False


def test_remove_decrements_count(store: BlackoutStore) -> None:
    store.add(BlackoutWindow(pipeline="etl", start=time(2, 0), end=time(4, 0)))
    store.add(BlackoutWindow(pipeline="etl", start=time(10, 0), end=time(11, 0)))
    removed = store.remove("etl")
    assert removed == 2
    assert len(store) == 0
