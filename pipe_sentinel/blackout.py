"""Blackout window support — suppress pipeline alerts during scheduled maintenance."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, time
from pathlib import Path
from typing import List, Optional


@dataclass
class BlackoutWindow:
    """A recurring daily blackout window during which alerts are suppressed."""

    pipeline: str  # pipeline name, or "*" for all pipelines
    start: time    # wall-clock start (UTC)
    end: time      # wall-clock end (UTC)
    reason: str = ""

    def is_active(self, at: Optional[datetime] = None) -> bool:
        """Return True if *at* (defaults to utcnow) falls within this window."""
        now = (at or datetime.utcnow()).time().replace(tzinfo=None)
        if self.start <= self.end:
            return self.start <= now < self.end
        # overnight window e.g. 23:00 – 01:00
        return now >= self.start or now < self.end

    def covers(self, pipeline_name: str) -> bool:
        return self.pipeline == "*" or self.pipeline == pipeline_name

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "start": self.start.strftime("%H:%M"),
            "end": self.end.strftime("%H:%M"),
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BlackoutWindow":
        return cls(
            pipeline=data["pipeline"],
            start=time.fromisoformat(data["start"]),
            end=time.fromisoformat(data["end"]),
            reason=data.get("reason", ""),
        )

    def __str__(self) -> str:
        tag = f"[{self.pipeline}]" if self.pipeline != "*" else "[all]"
        return (
            f"{tag} {self.start.strftime('%H:%M')}–{self.end.strftime('%H:%M')} UTC"
            + (f" ({self.reason})" if self.reason else "")
        )


@dataclass
class BlackoutStore:
    path: Path
    windows: List[BlackoutWindow] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.path.exists():
            raw = json.loads(self.path.read_text())
            self.windows = [BlackoutWindow.from_dict(w) for w in raw.get("windows", [])]

    def save(self) -> None:
        self.path.write_text(json.dumps({"windows": [w.to_dict() for w in self.windows]}, indent=2))

    def add(self, window: BlackoutWindow) -> None:
        self.windows.append(window)
        self.save()

    def remove(self, pipeline: str) -> int:
        before = len(self.windows)
        self.windows = [w for w in self.windows if w.pipeline != pipeline]
        self.save()
        return before - len(self.windows)

    def is_blacked_out(self, pipeline_name: str, at: Optional[datetime] = None) -> bool:
        return any(w.covers(pipeline_name) and w.is_active(at) for w in self.windows)

    def __len__(self) -> int:
        return len(self.windows)
