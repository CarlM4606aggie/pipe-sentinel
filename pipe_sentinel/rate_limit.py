"""Rate limiting for pipeline execution to prevent thundering herd."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional


@dataclass
class RateLimitState:
    """Tracks per-pipeline execution timestamps for rate limiting."""
    pipeline: str
    window_seconds: int
    max_runs: int
    run_timestamps: list[float] = field(default_factory=list)

    def _prune_old(self, now: float) -> None:
        cutoff = now - self.window_seconds
        self.run_timestamps = [t for t in self.run_timestamps if t >= cutoff]

    def is_limited(self, now: Optional[float] = None) -> bool:
        now = now or time.time()
        self._prune_old(now)
        return len(self.run_timestamps) >= self.max_runs

    def record_run(self, now: Optional[float] = None) -> None:
        now = now or time.time()
        self._prune_old(now)
        self.run_timestamps.append(now)

    def runs_in_window(self, now: Optional[float] = None) -> int:
        now = now or time.time()
        self._prune_old(now)
        return len(self.run_timestamps)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "window_seconds": self.window_seconds,
            "max_runs": self.max_runs,
            "run_timestamps": self.run_timestamps,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RateLimitState":
        return cls(
            pipeline=data["pipeline"],
            window_seconds=data["window_seconds"],
            max_runs=data["max_runs"],
            run_timestamps=data.get("run_timestamps", []),
        )


class RateLimiter:
    """Persisted rate limiter backed by a JSON state file."""

    def __init__(self, state_file: Path, window_seconds: int = 3600, max_runs: int = 5) -> None:
        self.state_file = state_file
        self.window_seconds = window_seconds
        self.max_runs = max_runs
        self._states: Dict[str, RateLimitState] = {}
        self._load()

    def _load(self) -> None:
        if self.state_file.exists():
            raw = json.loads(self.state_file.read_text())
            for name, data in raw.items():
                self._states[name] = RateLimitState.from_dict(data)

    def _save(self) -> None:
        self.state_file.write_text(
            json.dumps({k: v.to_dict() for k, v in self._states.items()}, indent=2)
        )

    def _get(self, pipeline: str) -> RateLimitState:
        if pipeline not in self._states:
            self._states[pipeline] = RateLimitState(
                pipeline=pipeline,
                window_seconds=self.window_seconds,
                max_runs=self.max_runs,
            )
        return self._states[pipeline]

    def is_limited(self, pipeline: str, now: Optional[float] = None) -> bool:
        return self._get(pipeline).is_limited(now)

    def record_run(self, pipeline: str, now: Optional[float] = None) -> None:
        self._get(pipeline).record_run(now)
        self._save()

    def runs_in_window(self, pipeline: str, now: Optional[float] = None) -> int:
        return self._get(pipeline).runs_in_window(now)
