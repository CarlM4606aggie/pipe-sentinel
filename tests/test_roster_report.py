"""Tests for pipe_sentinel.roster_report."""
import pytest
from pipe_sentinel.roster import build_roster
from pipe_sentinel.roster_report import (
    format_entry,
    build_roster_report,
    build_team_summary,
)


@pytest.fixture
def roster():
    return build_roster([
        {"pipeline": "ingest", "team": "data-eng", "owners": ["alice@x.com"], "slack_channel": "#data"},
        {"pipeline": "transform", "team": "data-eng", "owners": ["bob@x.com"]},
        {"pipeline": "export", "team": "platform", "owners": []},
    ])


def test_format_entry_contains_pipeline(roster):
    entry = roster.get("ingest")
    assert "ingest" in format_entry(entry)


def test_format_entry_contains_team(roster):
    entry = roster.get("ingest")
    assert "data-eng" in format_entry(entry)


def test_format_entry_contains_slack(roster):
    entry = roster.get("ingest")
    assert "#data" in format_entry(entry)


def test_format_entry_no_slack_omitted(roster):
    entry = roster.get("transform")
    assert "slack" not in format_entry(entry)


def test_format_entry_no_owners_shows_none(roster):
    entry = roster.get("export")
    assert "(none)" in format_entry(entry)


def test_build_roster_report_shows_count(roster):
    report = build_roster_report(roster)
    assert "3 pipeline" in report


def test_build_roster_report_team_filter(roster):
    report = build_roster_report(roster, team_filter="platform")
    assert "export" in report
    assert "ingest" not in report


def test_build_roster_report_empty_team(roster):
    report = build_roster_report(roster, team_filter="unknown")
    assert "No entries" in report


def test_build_team_summary_lists_teams(roster):
    summary = build_team_summary(roster)
    assert "data-eng" in summary
    assert "platform" in summary


def test_build_team_summary_counts(roster):
    summary = build_team_summary(roster)
    assert "2 pipeline" in summary
