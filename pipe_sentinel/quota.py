"""Pipeline run quota enforcement — cap how many runs a pipeline may
execute within a rolling time window."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class QuotaConfig:
    max_runs: int          # maximum allowed runs in the window
    window_seconds: int    # rolling window length in seconds


@dataclass
class QuotaState:
    pipeline: str
    timestamps: List[float] = field(default_factory=list)

    # --- persistence ---------------------------------------------------------

    def to_dict(self) -> dict:
        return {"pipeline": self.pipeline, "timestamps": self.timestamps}

    @classmethod
    def from_dict(cls, data: dict) -> "QuotaState":
        return cls(pipeline=data["pipeline"], timestamps=data.get("timestamps", []))


@dataclass
class QuotaStore:
    path: Path
    _states: dict = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if self.path.exists():
            try:
                raw = json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                raise ValueError(
                    f"Failed to load quota store from {self.path}: {exc}"
                ) from exc
            for name, blob in raw.items():
                self._states[name] = QuotaState.from_dict(blob)

    def _save(self) -> None:
        self.path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._states.items()}, indent=2)
        )

    def _state(self, pipeline: str) -> QuotaState:
        if pipeline not in self._states:
            self._states[pipeline] = QuotaState(pipeline=pipeline)
        return self._states[pipeline]

    def _prune(self, state: QuotaState, window_seconds: int, now: float) -> None:
        cutoff = now - window_seconds
        state.timestamps = [t for t in state.timestamps if t >= cutoff]

    def runs_in_window(self, pipeline: str, window_seconds: int,
                       now: Optional[float] = None) -> int:
        now = now or time.time()
        state = self._state(pipeline)
        self._prune(state, window_seconds, now)
        return len(state.timestamps)

    def is_exceeded(self, pipeline: str, cfg: QuotaConfig,
                    now: Optional[float] = None) -> bool:
        now = now or time.time()
        return self.runs_in_window(pipeline, cfg.window_seconds, now) >= cfg.max_runs

    def record_run(self, pipeline: str, now: Optional[float] = None) -> None:
        now = now or time.time()
        state = self._state(pipeline)
        state.timestamps.append(now)
        self._save()

    def reset(self, pipeline: str) -> None:
        """Clear all recorded run timestamps for the given pipeline.

        Useful for testing or manually lifting a quota block without
        waiting for the rolling window to expire.
        """
        if pipeline in self._states:
            self._states[pipeline].timestamps = []
            self._save()
