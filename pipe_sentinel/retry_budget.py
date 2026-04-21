"""Retry budget tracking — limits total retry attempts across a time window."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class RetryBudgetConfig:
    max_retries: int
    window_seconds: int

    def __post_init__(self) -> None:
        if self.max_retries < 1:
            raise ValueError("max_retries must be >= 1")
        if self.window_seconds < 1:
            raise ValueError("window_seconds must be >= 1")


@dataclass
class RetryBudgetState:
    pipeline: str
    attempts: List[float] = field(default_factory=list)

    def _prune(self, window_seconds: int, now: float) -> None:
        cutoff = now - window_seconds
        self.attempts = [t for t in self.attempts if t >= cutoff]

    def is_exhausted(self, cfg: RetryBudgetConfig, now: float | None = None) -> bool:
        now = now or time.time()
        self._prune(cfg.window_seconds, now)
        return len(self.attempts) >= cfg.max_retries

    def record_attempt(self, now: float | None = None) -> None:
        self.attempts.append(now or time.time())

    def remaining(self, cfg: RetryBudgetConfig, now: float | None = None) -> int:
        now = now or time.time()
        self._prune(cfg.window_seconds, now)
        return max(0, cfg.max_retries - len(self.attempts))

    def to_dict(self) -> dict:
        return {"pipeline": self.pipeline, "attempts": self.attempts}

    @classmethod
    def from_dict(cls, d: dict) -> "RetryBudgetState":
        return cls(pipeline=d["pipeline"], attempts=list(d.get("attempts", [])))


class RetryBudgetStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._states: dict[str, RetryBudgetState] = {}
        if path.exists():
            raw = json.loads(path.read_text())
            for d in raw:
                s = RetryBudgetState.from_dict(d)
                self._states[s.pipeline] = s

    def get(self, pipeline: str) -> RetryBudgetState:
        if pipeline not in self._states:
            self._states[pipeline] = RetryBudgetState(pipeline=pipeline)
        return self._states[pipeline]

    def save(self) -> None:
        data = [s.to_dict() for s in self._states.values()]
        self._path.write_text(json.dumps(data, indent=2))

    def __len__(self) -> int:
        return len(self._states)
