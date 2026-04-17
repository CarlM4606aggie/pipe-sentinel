"""Tests for pipe_sentinel.roster."""
import pytest
from pipe_sentinel.roster import (
    RosterEntry,
    Roster,
    build_roster,
    owners_for_failures,
)


@pytest.fixture
def entries():
    return [
        {"pipeline": "ingest", "team": "data-eng", "owners": ["alice@x.com"], "slack_channel": "#data"},
        {"pipeline": "transform", "team": "data-eng", "owners": ["bob@x.com"]},
        {"pipeline": "export", "team": "platform", "owners": []},
    ]


@pytest.fixture
def roster(entries):
    return build_roster(entries)


def test_build_roster_length(roster):
    assert len(roster) == 3


def test_get_existing_entry(roster):
    entry = roster.get("ingest")
    assert entry is not None
    assert entry.team == "data-eng"
    assert entry.owners == ["alice@x.com"]


def test_get_missing_entry_returns_none(roster):
    assert roster.get("nonexistent") is None


def test_has_slack_true(roster):
    assert roster.get("ingest").has_slack() is True


def test_has_slack_false(roster):
    assert roster.get("transform").has_slack() is False


def test_teams(roster):
    assert roster.teams() == ["data-eng", "platform"]


def test_entries_for_team(roster):
    de = roster.entries_for_team("data-eng")
    assert len(de) == 2
    names = {e.pipeline for e in de}
    assert names == {"ingest", "transform"}


def test_all_entries(roster):
    assert len(roster.all_entries()) == 3


def test_owners_for_failures_returns_matching(roster):
    result = owners_for_failures(roster, ["ingest", "transform"])
    assert result["ingest"] == ["alice@x.com"]
    assert result["transform"] == ["bob@x.com"]


def test_owners_for_failures_skips_no_owners(roster):
    result = owners_for_failures(roster, ["export"])
    assert "export" not in result


def test_owners_for_failures_skips_unknown(roster):
    result = owners_for_failures(roster, ["ghost"])
    assert result == {}


def test_roundtrip_serialisation():
    entry = RosterEntry(pipeline="p", team="t", owners=["a@b.com"], slack_channel="#ch")
    restored = RosterEntry.from_dict(entry.to_dict())
    assert restored.pipeline == entry.pipeline
    assert restored.team == entry.team
    assert restored.owners == entry.owners
    assert restored.slack_channel == entry.slack_channel
