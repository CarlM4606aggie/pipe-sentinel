"""Circuit breaker pattern for pipeline execution.

Tracks consecutive failures per pipeline and opens the circuit
(blocks execution) when a threshold is exceeded.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional


@dataclass
class CircuitState:
    pipeline_name: str
    failures: int = 0
    opened_at: Optional[float] = None  # epoch seconds

    @property
    def is_open(self) -> bool:
        return self.opened_at is not None

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "failures": self.failures,
            "opened_at": self.opened_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CircuitState":
        return cls(
            pipeline_name=data["pipeline_name"],
            failures=data.get("failures", 0),
            opened_at=data.get("opened_at"),
        )


@dataclass
class CircuitBreaker:
    state_file: Path
    threshold: int = 3          # failures before opening
    recovery_seconds: int = 300  # seconds before half-open attempt
    _states: Dict[str, CircuitState] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self._load()

    # ------------------------------------------------------------------
    def _load(self) -> None:
        if self.state_file.exists():
            raw = json.loads(self.state_file.read_text())
            self._states = {
                k: CircuitState.from_dict(v) for k, v in raw.items()
            }

    def _save(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(
            json.dumps({k: v.to_dict() for k, v in self._states.items()}, indent=2)
        )

    def _get(self, name: str) -> CircuitState:
        if name not in self._states:
            self._states[name] = CircuitState(pipeline_name=name)
        return self._states[name]

    # ------------------------------------------------------------------
    def is_open(self, name: str) -> bool:
        """Return True if the circuit is open and recovery window has NOT elapsed."""
        state = self._get(name)
        if not state.is_open:
            return False
        elapsed = time.time() - (state.opened_at or 0)
        if elapsed >= self.recovery_seconds:
            # half-open: allow one attempt
            return False
        return True

    def record_success(self, name: str) -> None:
        state = self._get(name)
        state.failures = 0
        state.opened_at = None
        self._save()

    def record_failure(self, name: str) -> CircuitState:
        state = self._get(name)
        state.failures += 1
        if state.failures >= self.threshold and not state.is_open:
            state.opened_at = time.time()
        self._save()
        return state

    def reset(self, name: str) -> None:
        self._states.pop(name, None)
        self._save()

    def all_states(self) -> Dict[str, CircuitState]:
        return dict(self._states)
