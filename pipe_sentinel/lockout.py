"""Lockout: temporarily block pipelines that exceed a failure threshold."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class LockoutEntry:
    pipeline: str
    locked_at: float
    duration_seconds: float
    reason: str

    def is_locked(self, now: Optional[float] = None) -> bool:
        t = now if now is not None else time.time()
        return t < self.locked_at + self.duration_seconds

    def remaining_seconds(self, now: Optional[float] = None) -> float:
        t = now if now is not None else time.time()
        return max(0.0, self.locked_at + self.duration_seconds - t)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "locked_at": self.locked_at,
            "duration_seconds": self.duration_seconds,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "LockoutEntry":
        return cls(
            pipeline=d["pipeline"],
            locked_at=float(d["locked_at"]),
            duration_seconds=float(d["duration_seconds"]),
            reason=d["reason"],
        )


@dataclass
class LockoutStore:
    path: Path
    _entries: Dict[str, LockoutEntry] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if self.path.exists():
            raw = json.loads(self.path.read_text())
            self._entries = {
                k: LockoutEntry.from_dict(v) for k, v in raw.items()
            }

    def _save(self) -> None:
        self.path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def lock(self, pipeline: str, duration_seconds: float, reason: str,
             now: Optional[float] = None) -> LockoutEntry:
        t = now if now is not None else time.time()
        entry = LockoutEntry(pipeline=pipeline, locked_at=t,
                             duration_seconds=duration_seconds, reason=reason)
        self._entries[pipeline] = entry
        self._save()
        return entry

    def release(self, pipeline: str) -> bool:
        if pipeline in self._entries:
            del self._entries[pipeline]
            self._save()
            return True
        return False

    def is_locked(self, pipeline: str, now: Optional[float] = None) -> bool:
        entry = self._entries.get(pipeline)
        return entry is not None and entry.is_locked(now)

    def get(self, pipeline: str) -> Optional[LockoutEntry]:
        return self._entries.get(pipeline)

    def all_entries(self) -> List[LockoutEntry]:
        return list(self._entries.values())

    def purge_expired(self, now: Optional[float] = None) -> int:
        expired = [k for k, v in self._entries.items() if not v.is_locked(now)]
        for k in expired:
            del self._entries[k]
        if expired:
            self._save()
        return len(expired)

    def __len__(self) -> int:
        return len(self._entries)
