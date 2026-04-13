"""Tests for pipe_sentinel.oncall_report."""
from __future__ import annotations

import pytest

from pipe_sentinel.oncall import OnCallEntry, OnCallRotation
from pipe_sentinel.oncall_report import build_oncall_report, format_entry


@pytest.fixture()
def rotation() -> OnCallRotation:
    return OnCallRotation(entries=[
        OnCallEntry(name="Alice", email="alice@example.com", pipelines=["etl_daily"]),
        OnCallEntry(name="Bob", email="bob@example.com", pipelines=[]),
    ])


def test_format_entry_with_pipelines():
    entry = OnCallEntry(name="Alice", email="alice@example.com", pipelines=["etl_daily"])
    line = format_entry(entry)
    assert "Alice" in line
    assert "alice@example.com" in line
    assert "etl_daily" in line


def test_format_entry_catch_all_shows_all_pipelines_label():
    entry = OnCallEntry(name="Bob", email="bob@example.com", pipelines=[])
    line = format_entry(entry)
    assert "all pipelines" in line


def test_build_oncall_report_all_entries(rotation):
    report = build_oncall_report(rotation)
    assert "Alice" in report
    assert "Bob" in report
    assert "On-call rotation" in report


def test_build_oncall_report_for_pipeline(rotation):
    report = build_oncall_report(rotation, pipeline_name="etl_daily")
    assert "etl_daily" in report
    assert "Alice" in report
    assert "Bob" in report  # catch-all still shown


def test_build_oncall_report_no_owners_for_pipeline(rotation):
    # Remove catch-all
    rotation.entries = [e for e in rotation.entries if e.pipelines]
    report = build_oncall_report(rotation, pipeline_name="etl_weekly")
    assert "none configured" in report


def test_build_oncall_report_empty_rotation():
    report = build_oncall_report(OnCallRotation())
    assert "no entries configured" in report
