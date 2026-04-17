"""Requeue failed pipelines for re-execution with optional delay."""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field,dict
from pathlib import Path
from typing import List, Optional


@dataclass
class RequeueEntry:
    pipeline_name: str
    queued_at: float
    run_after: float
    reason: str
    attempts: int = 0
    entry_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def is_ready(self, now: Optional[float] = None) -> bool:
        return (now or time.time()) >= self.run_after

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "RequeueEntry":
        return cls(**d)


@dataclass
class RequeueStore:
    path: Path
    _entries: List[RequeueEntry] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        if self.path.exists():
            raw = json.loads(self.path.read_text())
            self._entries = [RequeueEntry.from_dict(e) for e in raw]

    def _save(self) -> None:
        self.path.write_text(json.dumps([e.to_dict() for e in self._entries], indent=2))

    def enqueue(self, pipeline_name: str, delay_seconds: float = 0.0, reason: str = "") -> RequeueEntry:
        now = time.time()
        entry = RequeueEntry(
            pipeline_name=pipeline_name,
            queued_at=now,
            run_after=now + delay_seconds,
            reason=reason,
        )
        self._entries.append(entry)
        self._save()
        return entry

    def ready(self, now: Optional[float] = None) -> List[RequeueEntry]:
        return [e for e in self._entries if e.is_ready(now)]

    def remove(self, entry_id: str) -> None:
        self._entries = [e for e in self._entries if e.entry_id != entry_id]
        self._save()

    def all_entries(self) -> List[RequeueEntry]:
        return list(self._entries)

    def __len__(self) -> int:
        return len(self._entries)
