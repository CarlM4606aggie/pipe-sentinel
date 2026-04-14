"""Dead-letter queue: store and replay permanently-failed pipeline runs."""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import List, Optional

from pipe_sentinel.runner import RunResult


@dataclass
class DeadLetterEntry:
    pipeline_name: str
    command: str
    failed_at: float
    returncode: int
    stderr: str
    attempts: int
    entry_id: str = ""

    def __post_init__(self) -> None:
        if not self.entry_id:
            self.entry_id = f"{self.pipeline_name}-{int(self.failed_at)}"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "DeadLetterEntry":
        return cls(**data)

    @classmethod
    def from_run_result(cls, result: RunResult, attempts: int) -> "DeadLetterEntry":
        return cls(
            pipeline_name=result.pipeline_name,
            command=result.command,
            failed_at=result.finished_at,
            returncode=result.returncode,
            stderr=result.stderr or "",
            attempts=attempts,
        )


@dataclass
class DeadLetterStore:
    path: Path
    _entries: List[DeadLetterEntry] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            raw = json.loads(self.path.read_text())
            self._entries = [DeadLetterEntry.from_dict(r) for r in raw]
        else:
            self._entries = []

    def _save(self) -> None:
        self.path.write_text(json.dumps([e.to_dict() for e in self._entries], indent=2))

    def push(self, entry: DeadLetterEntry) -> None:
        self._entries.append(entry)
        self._save()

    def all_entries(self) -> List[DeadLetterEntry]:
        return list(self._entries)

    def remove(self, entry_id: str) -> bool:
        before = len(self._entries)
        self._entries = [e for e in self._entries if e.entry_id != entry_id]
        changed = len(self._entries) < before
        if changed:
            self._save()
        return changed

    def find(self, entry_id: str) -> Optional[DeadLetterEntry]:
        for e in self._entries:
            if e.entry_id == entry_id:
                return e
        return None

    def __len__(self) -> int:
        return len(self._entries)
