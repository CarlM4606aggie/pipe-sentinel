"""Checkpoint tracking — persist and query the last successful run timestamp
for each pipeline so downstream tools can detect stale pipelines."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional


@dataclass
class CheckpointStore:
    path: Path
    _data: Dict[str, float] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        if self.path.exists():
            try:
                raw = json.loads(self.path.read_text())
                self._data = {k: float(v) for k, v in raw.items()}
            except (json.JSONDecodeError, ValueError):
                self._data = {}

    # ------------------------------------------------------------------
    def record(self, pipeline_name: str, ts: Optional[float] = None) -> None:
        """Mark *pipeline_name* as successfully completed at *ts* (default: now)."""
        self._data[pipeline_name] = ts if ts is not None else time.time()
        self._save()

    def last_success(self, pipeline_name: str) -> Optional[float]:
        """Return the UNIX timestamp of the last success, or *None* if unknown."""
        return self._data.get(pipeline_name)

    def age_seconds(self, pipeline_name: str, now: Optional[float] = None) -> Optional[float]:
        """Seconds since the last successful run, or *None* if never recorded."""
        ts = self.last_success(pipeline_name)
        if ts is None:
            return None
        return (now if now is not None else time.time()) - ts

    def is_stale(self, pipeline_name: str, threshold_seconds: float, now: Optional[float] = None) -> bool:
        """Return True if the pipeline has not succeeded within *threshold_seconds*.

        A pipeline with no recorded checkpoint is always considered stale.

        Args:
            pipeline_name: The name of the pipeline to check.
            threshold_seconds: Maximum acceptable age in seconds.
            now: Reference timestamp (defaults to ``time.time()``).
        """
        age = self.age_seconds(pipeline_name, now=now)
        if age is None:
            return True
        return age > threshold_seconds

    def clear(self, pipeline_name: str) -> bool:
        """Remove the checkpoint for *pipeline_name*. Returns True if it existed."""
        existed = pipeline_name in self._data
        if existed:
            del self._data[pipeline_name]
            self._save()
        return existed

    def all_checkpoints(self) -> Dict[str, float]:
        """Return a shallow copy of all stored checkpoints."""
        return dict(self._data)

    # ------------------------------------------------------------------
    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._data, indent=2))


def store_from_path(path: str) -> CheckpointStore:
    return CheckpointStore(Path(path))
