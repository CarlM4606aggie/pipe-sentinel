"""Pipeline status snapshot: aggregates latest run state per pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipe_sentinel.audit import AuditRecord, fetch_recent


@dataclass
class PipelineStatus:
    name: str
    last_status: str  # 'success' | 'failure' | 'unknown'
    last_run_ts: Optional[str]
    consecutive_failures: int

    @property
    def is_healthy(self) -> bool:
        return self.last_status == "success"

    def __str__(self) -> str:
        symbol = "✓" if self.is_healthy else ("✗" if self.last_status == "failure" else "?")
        ts = self.last_run_ts or "never"
        streak = f"  ({self.consecutive_failures} consecutive failures)" if self.consecutive_failures else ""
        return f"[{symbol}] {self.name:<30} last={ts}{streak}"


def _consecutive_failures(records: List[AuditRecord]) -> int:
    """Count how many of the most-recent records are failures (stopping at first success)."""
    count = 0
    for rec in records:
        if rec.status == "failure":
            count += 1
        else:
            break
    return count


def build_status_snapshot(
    db_path: str,
    pipeline_names: List[str],
    lookback: int = 20,
) -> Dict[str, PipelineStatus]:
    """Return a status snapshot for every pipeline in *pipeline_names*."""
    snapshot: Dict[str, PipelineStatus] = {}

    for name in pipeline_names:
        records: List[AuditRecord] = fetch_recent(db_path, name, limit=lookback)
        # fetch_recent returns newest-first; keep that order for consecutive calc
        if records:
            latest = records[0]
            last_status = latest.status
            last_run_ts = latest.finished_at
            consec = _consecutive_failures(records)
        else:
            last_status = "unknown"
            last_run_ts = None
            consec = 0

        snapshot[name] = PipelineStatus(
            name=name,
            last_status=last_status,
            last_run_ts=last_run_ts,
            consecutive_failures=consec,
        )

    return snapshot


def format_snapshot(snapshot: Dict[str, PipelineStatus]) -> str:
    """Render the snapshot as a human-readable string."""
    if not snapshot:
        return "No pipelines tracked."
    lines = ["Pipeline Status Snapshot", "=" * 50]
    for status in snapshot.values():
        lines.append(str(status))
    healthy = sum(1 for s in snapshot.values() if s.is_healthy)
    lines.append("-" * 50)
    lines.append(f"Healthy: {healthy}/{len(snapshot)}")
    return "\n".join(lines)
