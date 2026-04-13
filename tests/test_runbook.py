"""Tests for pipe_sentinel.runbook."""
import pytest

from pipe_sentinel.runbook import (
    RunbookEntry,
    RunbookIndex,
    build_runbook_index,
    runbook_for_failures,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def index() -> RunbookIndex:
    raw = [
        {"pipeline": "ingest", "url": "https://wiki/ingest", "notes": "Check S3"},
        {"pipeline": "transform", "url": "https://wiki/transform"},
        {"pipeline": "export"},
    ]
    return build_runbook_index(raw)


# ---------------------------------------------------------------------------
# RunbookEntry
# ---------------------------------------------------------------------------

def test_entry_has_link_when_url_present():
    e = RunbookEntry(pipeline="p", url="https://example.com")
    assert e.has_link() is True


def test_entry_has_no_link_when_url_absent():
    e = RunbookEntry(pipeline="p")
    assert e.has_link() is False


def test_entry_str_includes_pipeline():
    e = RunbookEntry(pipeline="ingest", url="https://wiki/ingest", notes="Check S3")
    text = str(e)
    assert "ingest" in text
    assert "https://wiki/ingest" in text
    assert "Check S3" in text


def test_entry_str_no_url_or_notes():
    e = RunbookEntry(pipeline="bare")
    assert str(e) == "[bare]"


# ---------------------------------------------------------------------------
# RunbookIndex
# ---------------------------------------------------------------------------

def test_index_len(index):
    assert len(index) == 3


def test_index_get_known(index):
    entry = index.get("ingest")
    assert entry is not None
    assert entry.url == "https://wiki/ingest"


def test_index_get_unknown_returns_none(index):
    assert index.get("missing") is None


def test_index_all_entries_count(index):
    assert len(index.all_entries()) == 3


# ---------------------------------------------------------------------------
# build_runbook_index
# ---------------------------------------------------------------------------

def test_build_index_empty():
    idx = build_runbook_index([])
    assert len(idx) == 0


def test_build_index_notes_optional():
    idx = build_runbook_index([{"pipeline": "p", "url": "https://x.com"}])
    entry = idx.get("p")
    assert entry is not None
    assert entry.notes is None


# ---------------------------------------------------------------------------
# runbook_for_failures
# ---------------------------------------------------------------------------

def test_runbook_for_failures_returns_matching(index):
    results = runbook_for_failures(index, ["ingest", "transform"])
    names = [r.pipeline for r in results]
    assert "ingest" in names
    assert "transform" in names


def test_runbook_for_failures_skips_missing(index):
    results = runbook_for_failures(index, ["ghost"])
    assert results == []


def test_runbook_for_failures_partial_match(index):
    results = runbook_for_failures(index, ["ingest", "ghost"])
    assert len(results) == 1
    assert results[0].pipeline == "ingest"
