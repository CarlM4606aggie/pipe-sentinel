"""Pipeline cooldown enforcement — prevent a pipeline from re-running too soon after failure."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional


@dataclass
class CooldownEntry:
    pipeline_name: str
    failed_at: float  # unix timestamp
    cooldown_seconds: int

    def is_cooling(self, now: Optional[float] = None) -> bool:
        now = now if now is not None else time.time()
        return (now - self.failed_at) < self.cooldown_seconds

    def remaining_seconds(self, now: Optional[float] = None) -> float:
        now = now if now is not None else time.time()
        remaining = self.cooldown_seconds - (now - self.failed_at)
        return max(0.0, remaining)

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "failed_at": self.failed_at,
            "cooldown_seconds": self.cooldown_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CooldownEntry":
        return cls(
            pipeline_name=data["pipeline_name"],
            failed_at=float(data["failed_at"]),
            cooldown_seconds=int(data["cooldown_seconds"]),
        )


@dataclass
class CooldownStore:
    path: Path
    _entries: Dict[str, CooldownEntry] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if self.path.exists():
            raw = json.loads(self.path.read_text())
            self._entries = {
                k: CooldownEntry.from_dict(v) for k, v in raw.items()
            }

    def _save(self) -> None:
        self.path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def record_failure(self, name: str, cooldown_seconds: int, now: Optional[float] = None) -> CooldownEntry:
        entry = CooldownEntry(
            pipeline_name=name,
            failed_at=now if now is not None else time.time(),
            cooldown_seconds=cooldown_seconds,
        )
        self._entries[name] = entry
        self._save()
        return entry

    def is_cooling(self, name: str, now: Optional[float] = None) -> bool:
        entry = self._entries.get(name)
        if entry is None:
            return False
        return entry.is_cooling(now=now)

    def get(self, name: str) -> Optional[CooldownEntry]:
        return self._entries.get(name)

    def clear(self, name: str) -> None:
        self._entries.pop(name, None)
        self._save()

    def all_entries(self) -> list[CooldownEntry]:
        return list(self._entries.values())
