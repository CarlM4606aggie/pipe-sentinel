"""Alert throttling to suppress duplicate notifications within a cooldown window."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

_DEFAULT_COOLDOWN = 3600  # seconds
_DEFAULT_STATE_FILE = Path(".pipe_sentinel_throttle.json")


@dataclass
class ThrottleState:
    """Tracks last-alerted timestamps per pipeline name."""

    cooldown_seconds: int = _DEFAULT_COOLDOWN
    state_file: Path = _DEFAULT_STATE_FILE
    _timestamps: Dict[str, float] = field(default_factory=dict, init=False, repr=False)

    def load(self) -> None:
        """Load persisted state from disk if available."""
        if self.state_file.exists():
            try:
                data = json.loads(self.state_file.read_text())
                self._timestamps = {k: float(v) for k, v in data.items()}
            except (json.JSONDecodeError, ValueError):
                self._timestamps = {}

    def save(self) -> None:
        """Persist current state to disk."""
        self.state_file.write_text(json.dumps(self._timestamps))

    def is_suppressed(self, pipeline_name: str) -> bool:
        """Return True if an alert for *pipeline_name* is within the cooldown window."""
        last = self._timestamps.get(pipeline_name)
        if last is None:
            return False
        return (time.time() - last) < self.cooldown_seconds

    def record_alert(self, pipeline_name: str) -> None:
        """Record that an alert was sent for *pipeline_name* right now."""
        self._timestamps[pipeline_name] = time.time()

    def clear(self, pipeline_name: Optional[str] = None) -> None:
        """Clear throttle state for one pipeline or all pipelines."""
        if pipeline_name is None:
            self._timestamps.clear()
        else:
            self._timestamps.pop(pipeline_name, None)


def should_alert(state: ThrottleState, pipeline_name: str) -> bool:
    """Return True when an alert should be sent (not suppressed)."""
    return not state.is_suppressed(pipeline_name)


def mark_alerted(state: ThrottleState, pipeline_name: str, *, persist: bool = True) -> None:
    """Record an alert and optionally persist state to disk."""
    state.record_alert(pipeline_name)
    if persist:
        state.save()
