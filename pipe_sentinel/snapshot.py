"""Pipeline snapshot: capture and compare pipeline state across runs."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional


@dataclass
class PipelineSnapshot:
    name: str
    last_status: str          # 'success' | 'failure' | 'unknown'
    last_run_ts: Optional[str]  # ISO-8601 or None
    consecutive_failures: int
    captured_at: str          # ISO-8601

    def is_degraded(self) -> bool:
        return self.last_status == "failure" or self.consecutive_failures > 0


@dataclass
class SnapshotDiff:
    name: str
    previous: Optional[PipelineSnapshot]
    current: PipelineSnapshot

    def status_changed(self) -> bool:
        if self.previous is None:
            return False
        return self.previous.last_status != self.current.last_status

    def recovered(self) -> bool:
        return (
            self.previous is not None
            and self.previous.last_status == "failure"
            and self.current.last_status == "success"
        )

    def newly_failing(self) -> bool:
        return (
            self.previous is not None
            and self.previous.last_status != "failure"
            and self.current.last_status == "failure"
        )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_snapshot(snapshot: PipelineSnapshot, path: str) -> None:
    existing: dict = {}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            existing = json.load(fh)
    existing[snapshot.name] = asdict(snapshot)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(existing, fh, indent=2)


def load_snapshot(name: str, path: str) -> Optional[PipelineSnapshot]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    raw = data.get(name)
    if raw is None:
        return None
    return PipelineSnapshot(**raw)


def make_snapshot(
    name: str,
    last_status: str,
    last_run_ts: Optional[str],
    consecutive_failures: int,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        name=name,
        last_status=last_status,
        last_run_ts=last_run_ts,
        consecutive_failures=consecutive_failures,
        captured_at=_now_iso(),
    )


def diff_snapshot(name: str, current: PipelineSnapshot, path: str) -> SnapshotDiff:
    previous = load_snapshot(name, path)
    return SnapshotDiff(name=name, previous=previous, current=current)
