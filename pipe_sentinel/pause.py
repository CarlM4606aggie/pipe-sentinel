"""Pipeline pause/resume management.

Allows individual pipelines to be paused so they are skipped during
scheduled runs without being permanently removed from the config.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class PauseEntry:
    pipeline_name: str
    paused_at: float
    reason: str = ""
    resume_at: Optional[float] = None  # epoch; None means indefinite

    def is_active(self, now: Optional[float] = None) -> bool:
        """Return True if the pause is still in effect."""
        ts = now if now is not None else time.time()
        if self.resume_at is None:
            return True
        return ts < self.resume_at

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "paused_at": self.paused_at,
            "reason": self.reason,
            "resume_at": self.resume_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PauseEntry":
        return cls(
            pipeline_name=data["pipeline_name"],
            paused_at=data["paused_at"],
            reason=data.get("reason", ""),
            resume_at=data.get("resume_at"),
        )


@dataclass
class PauseStore:
    path: Path
    _entries: Dict[str, PauseEntry] = field(default_factory=dict, init=False)

    def load(self) -> None:
        if self.path.exists():
            raw = json.loads(self.path.read_text())
            self._entries = {
                k: PauseEntry.from_dict(v) for k, v in raw.items()
            }

    def save(self) -> None:
        self.path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def pause(self, name: str, reason: str = "", resume_at: Optional[float] = None) -> PauseEntry:
        entry = PauseEntry(
            pipeline_name=name,
            paused_at=time.time(),
            reason=reason,
            resume_at=resume_at,
        )
        self._entries[name] = entry
        self.save()
        return entry

    def resume(self, name: str) -> bool:
        """Remove a pause entry. Returns True if the entry existed."""
        existed = name in self._entries
        self._entries.pop(name, None)
        self.save()
        return existed

    def is_paused(self, name: str, now: Optional[float] = None) -> bool:
        entry = self._entries.get(name)
        if entry is None:
            return False
        return entry.is_active(now)

    def active_entries(self, now: Optional[float] = None) -> List[PauseEntry]:
        return [e for e in self._entries.values() if e.is_active(now)]

    def all_entries(self) -> List[PauseEntry]:
        return list(self._entries.values())
