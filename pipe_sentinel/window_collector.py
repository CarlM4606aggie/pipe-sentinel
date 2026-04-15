"""Collect WindowEntry objects from the audit database."""
from __future__ import annotations

from typing import List, Optional

from pipe_sentinel.audit import fetch_recent, AuditRecord
from pipe_sentinel.config import SentinelConfig
from pipe_sentinel.window import (
    WindowConfig,
    WindowEntry,
    WindowResult,
    scan_windows,
)


def _record_to_entry(record: AuditRecord) -> WindowEntry:
    return WindowEntry(
        pipeline_name=record.pipeline_name,
        succeeded=record.status == "success",
        timestamp=record.started_at,
    )


def entries_from_db(db_path: str, limit: int = 500) -> List[WindowEntry]:
    """Load recent audit records and convert them to WindowEntry objects."""
    records: List[AuditRecord] = fetch_recent(db_path, limit=limit)
    return [_record_to_entry(r) for r in records]


def collect_window_results(
    config: SentinelConfig,
    db_path: str,
    window_config: Optional[WindowConfig] = None,
    limit: int = 500,
) -> List[WindowResult]:
    """End-to-end helper: load entries, evaluate windows, return results."""
    wc = window_config or WindowConfig()
    pipeline_names = [p.name for p in config.pipelines]
    entries = entries_from_db(db_path, limit=limit)
    return scan_windows(entries, pipeline_names, wc)
