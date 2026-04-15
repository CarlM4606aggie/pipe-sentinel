"""Heartbeat tracking — detect pipelines that have stopped running entirely."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipe_sentinel.audit import fetch_recent, AuditRecord


@dataclass
class HeartbeatResult:
    pipeline_name: str
    last_run_at: Optional[datetime]  # None when no runs recorded
    max_silence_hours: float
    silent_hours: Optional[float]  # None when no runs recorded
    missing: bool  # True when no runs at all

    def __str__(self) -> str:
        if self.missing:
            return f"[MISSING] {self.pipeline_name} — no runs recorded"
        sign = "!" if self.is_silent else "✓"
        return (
            f"[{sign}] {self.pipeline_name} — last run "
            f"{self.silent_hours:.1f}h ago "
            f"(max {self.max_silence_hours}h)"
        )

    @property
    def is_silent(self) -> bool:
        return self.missing or (self.silent_hours is not None and self.silent_hours > self.max_silence_hours)


def _latest_record(records: List[AuditRecord]) -> Optional[AuditRecord]:
    """Return the most-recent record from *records*, or None."""
    if not records:
        return None
    return max(records, key=lambda r: r.started_at)


def check_heartbeat(
    pipeline_name: str,
    max_silence_hours: float,
    db_path: str,
    limit: int = 200,
    now: Optional[datetime] = None,
) -> HeartbeatResult:
    """Check whether *pipeline_name* has run within *max_silence_hours*."""
    if now is None:
        now = datetime.now(timezone.utc)

    records = [
        r for r in fetch_recent(db_path, limit=limit)
        if r.pipeline_name == pipeline_name
    ]
    latest = _latest_record(records)

    if latest is None:
        return HeartbeatResult(
            pipeline_name=pipeline_name,
            last_run_at=None,
            max_silence_hours=max_silence_hours,
            silent_hours=None,
            missing=True,
        )

    last_dt = latest.started_at
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)

    silent_hours = (now - last_dt).total_seconds() / 3600.0
    return HeartbeatResult(
        pipeline_name=pipeline_name,
        last_run_at=last_dt,
        max_silence_hours=max_silence_hours,
        silent_hours=silent_hours,
        missing=False,
    )


def scan_heartbeats(
    pipelines,
    db_path: str,
    now: Optional[datetime] = None,
) -> List[HeartbeatResult]:
    """Run heartbeat checks for all pipelines that declare *max_silence_hours*."""
    results: List[HeartbeatResult] = []
    for pipeline in pipelines:
        max_silence = getattr(pipeline, "max_silence_hours", None)
        if max_silence is None:
            continue
        results.append(
            check_heartbeat(pipeline.name, float(max_silence), db_path, now=now)
        )
    return results
