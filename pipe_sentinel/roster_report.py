"""Roster report formatting."""
from __future__ import annotations
from typing import Optional
from pipe_sentinel.roster import Roster, RosterEntry


def format_entry(entry: RosterEntry) -> str:
    owners = ", ".join(entry.owners) if entry.owners else "(none)"
    slack = f"  slack: {entry.slack_channel}" if entry.has_slack() else ""
    line = f"  [{entry.team}] {entry.pipeline} — owners: {owners}"
    return line + slack


def build_roster_report(roster: Roster, team_filter: Optional[str] = None) -> str:
    entries = (
        roster.entries_for_team(team_filter)
        if team_filter
        else roster.all_entries()
    )
    if not entries:
        label = f"team '{team_filter}'" if team_filter else "roster"
        return f"No entries in {label}."

    lines = [f"Roster ({len(entries)} pipeline(s)):"]
    for entry in sorted(entries, key=lambda e: (e.team, e.pipeline)):
        lines.append(format_entry(entry))
    return "\n".join(lines)


def build_team_summary(roster: Roster) -> str:
    teams = roster.teams()
    if not teams:
        return "No teams registered."
    lines = ["Teams:"]
    for team in teams:
        count = len(roster.entries_for_team(team))
        lines.append(f"  {team}: {count} pipeline(s)")
    return "\n".join(lines)


def print_roster_report(roster: Roster, team_filter: Optional[str] = None) -> None:
    print(build_roster_report(roster, team_filter))
