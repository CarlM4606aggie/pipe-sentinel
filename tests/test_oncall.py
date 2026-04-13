"""Tests for pipe_sentinel.oncall and pipe_sentinel.oncall_collector."""
from __future__ import annotations

import pytest

from pipe_sentinel.oncall import OnCallEntry, OnCallRotation, load_rotation
from pipe_sentinel.oncall_collector import owners_for_failures
from pipe_sentinel.runner import RunResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def rotation() -> OnCallRotation:
    return OnCallRotation(entries=[
        OnCallEntry(name="Alice", email="alice@example.com", pipelines=["etl_daily"]),
        OnCallEntry(name="Bob", email="bob@example.com", pipelines=[]),
    ])


def _result(name: str, success: bool) -> RunResult:
    return RunResult(
        pipeline_name=name,
        success=success,
        returncode=0 if success else 1,
        stdout="",
        stderr="" if success else "error",
        duration=1.0,
        attempts=1,
    )


# ---------------------------------------------------------------------------
# OnCallEntry.covers
# ---------------------------------------------------------------------------

def test_entry_covers_specific_pipeline():
    entry = OnCallEntry(name="X", email="x@x.com", pipelines=["etl_daily"])
    assert entry.covers("etl_daily")
    assert not entry.covers("etl_weekly")


def test_catch_all_entry_covers_any_pipeline():
    entry = OnCallEntry(name="X", email="x@x.com", pipelines=[])
    assert entry.covers("anything")
    assert entry.covers("etl_daily")


# ---------------------------------------------------------------------------
# OnCallRotation
# ---------------------------------------------------------------------------

def test_owners_for_returns_matching_entries(rotation):
    owners = rotation.owners_for("etl_daily")
    names = [o.name for o in owners]
    assert "Alice" in names
    assert "Bob" in names  # catch-all


def test_owners_for_unknown_pipeline_returns_catch_all(rotation):
    owners = rotation.owners_for("unknown_pipeline")
    assert [o.name for o in owners] == ["Bob"]


def test_emails_for_deduplicates(rotation):
    rotation.entries.append(
        OnCallEntry(name="Bob2", email="bob@example.com", pipelines=["etl_daily"])
    )
    emails = rotation.emails_for("etl_daily")
    assert emails.count("bob@example.com") == 1


# ---------------------------------------------------------------------------
# load_rotation
# ---------------------------------------------------------------------------

def test_load_rotation_empty_returns_empty_rotation():
    rot = load_rotation(None)
    assert rot.entries == []


def test_load_rotation_parses_entries():
    raw = [
        {"name": "Alice", "email": "alice@example.com", "pipelines": ["etl_daily"]},
        {"name": "Bob", "email": "bob@example.com"},
    ]
    rot = load_rotation(raw)
    assert len(rot.entries) == 2
    assert rot.entries[0].name == "Alice"
    assert rot.entries[1].pipelines == []


# ---------------------------------------------------------------------------
# owners_for_failures
# ---------------------------------------------------------------------------

def test_owners_for_failures_only_failed(rotation):
    results = [_result("etl_daily", True), _result("etl_weekly", False)]
    mapping = owners_for_failures(rotation, results)
    assert "etl_daily" not in mapping
    assert "etl_weekly" in mapping


def test_owners_for_failures_no_owner_excluded(rotation):
    # Remove catch-all Bob so etl_weekly has no owner
    rotation.entries = [e for e in rotation.entries if e.name == "Alice"]
    results = [_result("etl_weekly", False)]
    mapping = owners_for_failures(rotation, results)
    assert "etl_weekly" not in mapping
