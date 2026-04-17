"""Tests for pipe_sentinel.requeue_report."""
import time
import pytest
from pathlib import Path
from pipe_sentinel.requeue import RequeueStore
from pipe_sentinel.requeue_report import format_entry, build_requeue_report


@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "requeue.json"


@pytest.fixture
def store(state_file: Path) -> RequeueStore:
    return RequeueStore(state_file)


def test_build_report_empty(store):
    report = build_requeue_report(store)
    assert "empty" in report
    assert "0 entries" in report


def test_build_report_shows_count(store):
    store.enqueue("etl_load")
    store.enqueue("etl_transform")
    report = build_requeue_report(store)
    assert "2 entries" in report


def test_format_entry_shows_pipeline_name(store):
    entry = store.enqueue("my_pipeline", delay_seconds=0)
    line = format_entry(entry)
    assert "my_pipeline" in line


def test_format_entry_shows_reason(store):
    entry = store.enqueue("my_pipeline", reason="timeout")
    line = format_entry(entry)
    assert "timeout" in line


def test_format_entry_ready_label(store):
    entry = store.enqueue("my_pipeline", delay_seconds=0)
    line = format_entry(entry, now=time.time() + 1)
    assert "ready" in line


def test_build_report_separates_ready_and_pending(store):
    store.enqueue("ready_pipe", delay_seconds=0)
    store.enqueue("pending_pipe", delay_seconds=9999)
    report = build_requeue_report(store)
    assert "Ready" in report
    assert "Pending" in report
    assert "ready_pipe" in report
    assert "pending_pipe" in report
