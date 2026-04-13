"""Tests for pipe_sentinel.runbook_report."""
import pytest

from pipe_sentinel.runbook import RunbookEntry, build_runbook_index
from pipe_sentinel.runbook_report import (
    build_full_index_report,
    build_runbook_report,
    format_entry,
)


@pytest.fixture()
def entries():
    return [
        RunbookEntry(pipeline="ingest", url="https://wiki/ingest", notes="Check S3"),
        RunbookEntry(pipeline="export"),
    ]


# ---------------------------------------------------------------------------
# format_entry
# ---------------------------------------------------------------------------

def test_format_entry_contains_pipeline():
    e = RunbookEntry(pipeline="ingest", url="https://wiki/ingest")
    assert "ingest" in format_entry(e)


def test_format_entry_contains_url():
    e = RunbookEntry(pipeline="ingest", url="https://wiki/ingest")
    assert "https://wiki/ingest" in format_entry(e)


def test_format_entry_no_link_label():
    e = RunbookEntry(pipeline="bare")
    assert "(no link)" in format_entry(e)


def test_format_entry_contains_notes():
    e = RunbookEntry(pipeline="p", notes="restart the pod")
    assert "restart the pod" in format_entry(e)


def test_format_entry_no_notes_omitted():
    e = RunbookEntry(pipeline="p", url="https://x")
    text = format_entry(e)
    assert "Notes" not in text


# ---------------------------------------------------------------------------
# build_runbook_report
# ---------------------------------------------------------------------------

def test_build_report_empty():
    report = build_runbook_report([])
    assert "No runbook entries" in report


def test_build_report_header(entries):
    report = build_runbook_report(entries)
    assert "Runbook Links" in report


def test_build_report_includes_all_pipelines(entries):
    report = build_runbook_report(entries)
    assert "ingest" in report
    assert "export" in report


# ---------------------------------------------------------------------------
# build_full_index_report
# ---------------------------------------------------------------------------

def test_full_index_report_uses_all_entries():
    raw = [
        {"pipeline": "a", "url": "https://a"},
        {"pipeline": "b", "url": "https://b"},
    ]
    index = build_runbook_index(raw)
    report = build_full_index_report(index)
    assert "https://a" in report
    assert "https://b" in report


def test_full_index_report_empty_index():
    from pipe_sentinel.runbook import RunbookIndex
    report = build_full_index_report(RunbookIndex())
    assert "No runbook entries" in report
