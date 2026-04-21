"""Mute store — temporarily silence alerts for specific pipelines."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class MuteEntry:
    pipeline: str
    muted_at: float
    duration_seconds: Optional[float]  # None = muted indefinitely
    reason: str = ""

    def is_muted(self, now: Optional[float] = None) -> bool:
        if self.duration_seconds is None:
            return True
        ts = now if now is not None else time.time()
        return ts < self.muted_at + self.duration_seconds

    def expires_at(self) -> Optional[float]:
        if self.duration_seconds is None:
            return None
        return self.muted_at + self.duration_seconds

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "muted_at": self.muted_at,
            "duration_seconds": self.duration_seconds,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "MuteEntry":
        return cls(
            pipeline=data["pipeline"],
            muted_at=float(data["muted_at"]),
            duration_seconds=data.get("duration_seconds"),
            reason=data.get("reason", ""),
        )


@dataclass
class MuteStore:
    path: Path
    _entries: Dict[str, MuteEntry] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if self.path.exists():
            raw = json.loads(self.path.read_text())
            self._entries = {
                k: MuteEntry.from_dict(v) for k, v in raw.items()
            }

    def _save(self) -> None:
        self.path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def mute(self, pipeline: str, duration_seconds: Optional[float] = None,
             reason: str = "", now: Optional[float] = None) -> MuteEntry:
        ts = now if now is not None else time.time()
        entry = MuteEntry(pipeline=pipeline, muted_at=ts,
                          duration_seconds=duration_seconds, reason=reason)
        self._entries[pipeline] = entry
        self._save()
        return entry

    def unmute(self, pipeline: str) -> bool:
        if pipeline in self._entries:
            del self._entries[pipeline]
            self._save()
            return True
        return False

    def is_muted(self, pipeline: str, now: Optional[float] = None) -> bool:
        entry = self._entries.get(pipeline)
        if entry is None:
            return False
        return entry.is_muted(now=now)

    def active_entries(self, now: Optional[float] = None) -> List[MuteEntry]:
        return [e for e in self._entries.values() if e.is_muted(now=now)]

    def __len__(self) -> int:
        return len(self._entries)
