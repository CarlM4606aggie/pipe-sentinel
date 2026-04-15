"""Stale pipeline detection — flags pipelines that have not run recently."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipe_sentinel.audit import fetch_recent, AuditRecord


@dataclass
class StaleResult:
    pipeline_name: str
    last_run_at: Optional[datetime]  # None when no runs recorded
    max_age_hours: float
    age_hours: Optional[float]  # None when no runs recorded
    is_stale: bool

    def __str__(self) -> str:
        if self.last_run_at is None:
            return f"[STALE] {self.pipeline_name}: never run (max {self.max_age_hours}h)"
        symbol = "STALE" if self.is_stale else "OK"
        return (
            f"[{symbol}] {self.pipeline_name}: "
            f"last run {self.age_hours:.1f}h ago (max {self.max_age_hours}h)"
        )


def _age_hours(ts: datetime) -> float:
    now = datetime.now(tz=timezone.utc)
    delta = now - ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else now - ts
    return delta.total_seconds() / 3600.0


def check_stale(
    pipeline_name: str,
    max_age_hours: float,
    db_path: str,
    lookback: int = 1,
) -> StaleResult:
    """Return a StaleResult for *pipeline_name* using records from *db_path*."""
    records: List[AuditRecord] = fetch_recent(db_path, pipeline_name, limit=lookback)

    if not records:
        return StaleResult(
            pipeline_name=pipeline_name,
            last_run_at=None,
            max_age_hours=max_age_hours,
            age_hours=None,
            is_stale=True,
        )

    latest: AuditRecord = records[0]
    last_dt = datetime.fromisoformat(latest.started_at)
    age = _age_hours(last_dt)
    return StaleResult(
        pipeline_name=pipeline_name,
        last_run_at=last_dt,
        max_age_hours=max_age_hours,
        age_hours=age,
        is_stale=age > max_age_hours,
    )


def scan_stale(
    pipelines: list,  # list of PipelineConfig-like objects
    db_path: str,
) -> List[StaleResult]:
    """Check all pipelines that define *max_age_hours* and return results."""
    results: List[StaleResult] = []
    for pipeline in pipelines:
        max_age = getattr(pipeline, "max_age_hours", None)
        if max_age is None:
            continue
        results.append(check_stale(pipeline.name, float(max_age), db_path))
    return results
