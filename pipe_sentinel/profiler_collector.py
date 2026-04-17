"""Collect profiling data from the audit database."""
from __future__ import annotations

from typing import List

from pipe_sentinel.audit import fetch_recent
from pipe_sentinel.profiler import ProfileStats, scan_profiles


def collect_profiles(
    db_path: str,
    pipeline_names: List[str],
    limit: int = 200,
) -> List[ProfileStats]:
    """Fetch recent audit records and compute per-pipeline profile stats."""
    records = fetch_recent(db_path, limit=limit)
    return scan_profiles(records, pipeline_names)
