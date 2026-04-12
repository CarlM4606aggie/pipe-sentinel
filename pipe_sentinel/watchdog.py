"""Watchdog module: detects stale or overdue pipelines based on audit history."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from pipe_sentinel.audit import AuditRecord, fetch_recent
from pipe_sentinel.config import PipelineConfig


@dataclass
class WatchdogAlert:
    pipeline_name: str
    reason: str
    last_run: Optional[datetime] = None
    overdue_by: Optional[timedelta] = None

    def __str__(self) -> str:
        parts = [f"[WATCHDOG] {self.pipeline_name}: {self.reason}"]
        if self.last_run:
            parts.append(f"  Last run: {self.last_run.isoformat()}")
        if self.overdue_by:
            parts.append(f"  Overdue by: {self.overdue_by}")
        return "\n".join(parts)


@dataclass
class WatchdogReport:
    alerts: List[WatchdogAlert] = field(default_factory=list)

    @property
    def has_alerts(self) -> bool:
        return len(self.alerts) > 0

    def summary(self) -> str:
        if not self.has_alerts:
            return "Watchdog: all pipelines running on schedule."
        lines = [f"Watchdog detected {len(self.alerts)} overdue pipeline(s):"]
        for alert in self.alerts:
            lines.append(str(alert))
        return "\n".join(lines)


def _latest_record(records: List[AuditRecord]) -> Optional[AuditRecord]:
    """Return the most recent audit record from a list."""
    if not records:
        return None
    return max(records, key=lambda r: r.ran_at)


def check_pipeline(pipeline: PipelineConfig, db_path: str, now: Optional[datetime] = None) -> Optional[WatchdogAlert]:
    """Return a WatchdogAlert if the pipeline is overdue, else None."""
    if pipeline.max_age_minutes is None:
        return None

    now = now or datetime.now(tz=timezone.utc)
    records = fetch_recent(db_path, pipeline.name, limit=1)
    latest = _latest_record(records)

    if latest is None:
        return WatchdogAlert(
            pipeline_name=pipeline.name,
            reason="No run history found.",
        )

    age = now - latest.ran_at.replace(tzinfo=timezone.utc)
    threshold = timedelta(minutes=pipeline.max_age_minutes)
    if age > threshold:
        return WatchdogAlert(
            pipeline_name=pipeline.name,
            reason="Pipeline has not run within the expected interval.",
            last_run=latest.ran_at,
            overdue_by=age - threshold,
        )

    return None


def run_watchdog(pipelines: List[PipelineConfig], db_path: str) -> WatchdogReport:
    """Check all pipelines and return a WatchdogReport."""
    report = WatchdogReport()
    for pipeline in pipelines:
        alert = check_pipeline(pipeline, db_path)
        if alert:
            report.alerts.append(alert)
    return report
