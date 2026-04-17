"""Roster: maps pipelines to responsible teams/owners for reporting."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class RosterEntry:
    pipeline: str
    team: str
    owners: List[str] = field(default_factory=list)
    slack_channel: Optional[str] = None

    def has_slack(self) -> bool:
        return bool(self.slack_channel)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "team": self.team,
            "owners": self.owners,
            "slack_channel": self.slack_channel,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RosterEntry":
        return cls(
            pipeline=data["pipeline"],
            team=data["team"],
            owners=data.get("owners", []),
            slack_channel=data.get("slack_channel"),
        )


@dataclass
class Roster:
    _entries: Dict[str, RosterEntry] = field(default_factory=dict)

    def add(self, entry: RosterEntry) -> None:
        self._entries[entry.pipeline] = entry

    def get(self, pipeline: str) -> Optional[RosterEntry]:
        return self._entries.get(pipeline)

    def all_entries(self) -> List[RosterEntry]:
        return list(self._entries.values())

    def teams(self) -> List[str]:
        return sorted({e.team for e in self._entries.values()})

    def entries_for_team(self, team: str) -> List[RosterEntry]:
        return [e for e in self._entries.values() if e.team == team]

    def __len__(self) -> int:
        return len(self._entries)


def build_roster(data: List[dict]) -> Roster:
    roster = Roster()
    for item in data:
        roster.add(RosterEntry.from_dict(item))
    return roster


def owners_for_failures(roster: Roster, failed_pipelines: List[str]) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    for name in failed_pipelines:
        entry = roster.get(name)
        if entry and entry.owners:
            result[name] = entry.owners
    return result
