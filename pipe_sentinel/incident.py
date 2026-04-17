"""Incident tracking: group consecutive failures into named incidents."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipe_sentinel.audit import AuditRecord


@dataclass
class Incident:
    pipeline: str
    started_at: datetime
    ended_at: Optional[datetime]
    failure_count: int
    last_error: str

    @property
    def is_open(self) -> bool:
        return self.ended_at is None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()

    def __str__(self) -> str:
        status = "OPEN" if self.is_open else "RESOLVED"
        dur = f"{self.duration_seconds:.0f}s" if self.duration_seconds is not None else "ongoing"
        return (
            f"[{status}] {self.pipeline}: {self.failure_count} failure(s) "
            f"since {self.started_at.isoformat()} ({dur})"
        )


def _parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def detect_incidents(records: List[AuditRecord]) -> List[Incident]:
    """Scan records (oldest-first) and return one Incident per failure run."""
    sorted_recs = sorted(records, key=lambda r: r.timestamp)
    incidents: List[Incident] = []
    open_incident: Optional[Incident] = None

    for rec in sorted_recs:
        ts = _parse_ts(rec.timestamp)
        if rec.status == "failure":
            if open_incident is None:
                open_incident = Incident(
                    pipeline=rec.pipeline,
                    started_at=ts,
                    ended_at=None,
                    failure_count=1,
                    last_error=rec.error or "",
                )
            else:
                open_incident.failure_count += 1
                open_incident.last_error = rec.error or ""
                open_incident.ended_at = None
        else:
            if open_incident is not None:
                open_incident.ended_at = ts
                incidents.append(open_incident)
                open_incident = None

    if open_incident is not None:
        incidents.append(open_incident)

    return incidents


def scan_all_incidents(records: List[AuditRecord]) -> List[Incident]:
    """Group records by pipeline then detect incidents per pipeline."""
    by_pipeline: dict[str, List[AuditRecord]] = {}
    for r in records:
        by_pipeline.setdefault(r.pipeline, []).append(r)
    result: List[Incident] = []
    for recs in by_pipeline.values():
        result.extend(detect_incidents(recs))
    return result
