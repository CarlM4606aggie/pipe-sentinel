"""On-call rotation support: map pipelines to responsible owners."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class OnCallEntry:
    """A single on-call owner entry."""
    name: str
    email: str
    pipelines: List[str] = field(default_factory=list)  # empty = catch-all

    def covers(self, pipeline_name: str) -> bool:
        """Return True if this entry covers *pipeline_name*."""
        return not self.pipelines or pipeline_name in self.pipelines


@dataclass
class OnCallRotation:
    """Collection of on-call entries loaded from config."""
    entries: List[OnCallEntry] = field(default_factory=list)

    def owners_for(self, pipeline_name: str) -> List[OnCallEntry]:
        """Return all entries that cover *pipeline_name*."""
        return [e for e in self.entries if e.covers(pipeline_name)]

    def emails_for(self, pipeline_name: str) -> List[str]:
        """Return deduplicated email addresses for *pipeline_name*."""
        seen: Dict[str, None] = {}
        for entry in self.owners_for(pipeline_name):
            seen[entry.email] = None
        return list(seen)


def _parse_entry(raw: dict) -> OnCallEntry:
    return OnCallEntry(
        name=raw["name"],
        email=raw["email"],
        pipelines=list(raw.get("pipelines") or []),
    )


def load_rotation(raw_list: Optional[List[dict]]) -> OnCallRotation:
    """Build an :class:`OnCallRotation` from the raw YAML list."""
    if not raw_list:
        return OnCallRotation()
    return OnCallRotation(entries=[_parse_entry(r) for r in raw_list])
