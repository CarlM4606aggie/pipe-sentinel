"""Debounce module: suppress rapid repeated alerts for the same pipeline.

A pipeline is debounced if it has fired an alert within the debounce
window (seconds).  Once the window expires the next failure will fire
a fresh alert and restart the timer.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional


@dataclass
class DebounceEntry:
    pipeline: str
    last_alert_at: float  # unix timestamp
    window_seconds: float

    def is_debounced(self, now: Optional[float] = None) -> bool:
        """Return True if still inside the debounce window."""
        t = now if now is not None else time.time()
        return (t - self.last_alert_at) < self.window_seconds

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_alert_at": self.last_alert_at,
            "window_seconds": self.window_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DebounceEntry":
        return cls(
            pipeline=data["pipeline"],
            last_alert_at=float(data["last_alert_at"]),
            window_seconds=float(data["window_seconds"]),
        )


@dataclass
class DebounceStore:
    path: Path
    _entries: Dict[str, DebounceEntry] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            raw = json.loads(self.path.read_text())
            self._entries = {
                k: DebounceEntry.from_dict(v) for k, v in raw.items()
            }

    def _save(self) -> None:
        self.path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def is_debounced(self, pipeline: str, now: Optional[float] = None) -> bool:
        entry = self._entries.get(pipeline)
        if entry is None:
            return False
        return entry.is_debounced(now)

    def record_alert(self, pipeline: str, window_seconds: float,
                     now: Optional[float] = None) -> None:
        t = now if now is not None else time.time()
        self._entries[pipeline] = DebounceEntry(
            pipeline=pipeline,
            last_alert_at=t,
            window_seconds=window_seconds,
        )
        self._save()

    def clear(self, pipeline: str) -> None:
        self._entries.pop(pipeline, None)
        self._save()
