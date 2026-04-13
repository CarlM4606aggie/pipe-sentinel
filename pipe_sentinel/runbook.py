"""Runbook links — associate pipelines with remediation URLs or notes."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class RunbookEntry:
    pipeline: str
    url: Optional[str] = None
    notes: Optional[str] = None

    def has_link(self) -> bool:
        return bool(self.url)

    def __str__(self) -> str:
        parts = [f"[{self.pipeline}]"]
        if self.url:
            parts.append(self.url)
        if self.notes:
            parts.append(f"({self.notes})")
        return " ".join(parts)


@dataclass
class RunbookIndex:
    _entries: Dict[str, RunbookEntry] = field(default_factory=dict)

    def add(self, entry: RunbookEntry) -> None:
        self._entries[entry.pipeline] = entry

    def get(self, pipeline: str) -> Optional[RunbookEntry]:
        return self._entries.get(pipeline)

    def all_entries(self) -> List[RunbookEntry]:
        return list(self._entries.values())

    def __len__(self) -> int:
        return len(self._entries)


def build_runbook_index(raw: List[Dict]) -> RunbookIndex:
    """Build a RunbookIndex from a list of raw config dicts."""
    index = RunbookIndex()
    for item in raw:
        entry = RunbookEntry(
            pipeline=item["pipeline"],
            url=item.get("url"),
            notes=item.get("notes"),
        )
        index.add(entry)
    return index


def runbook_for_failures(index: RunbookIndex, failed_pipelines: List[str]) -> List[RunbookEntry]:
    """Return runbook entries for each failed pipeline that has one."""
    results = []
    for name in failed_pipelines:
        entry = index.get(name)
        if entry is not None:
            results.append(entry)
    return results
