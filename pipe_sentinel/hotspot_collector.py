"""Collect hotspot data from the audit database."""
from __future__ import annotations

from typing import List

from pipe_sentinel.audit import fetch_recent
from pipe_sentinel.hotspot import HotspotResult, scan_hotspots

_DEFAULT_LOOKBACK = 200
_DEFAULT_TOP_N = 5
_DEFAULT_MIN_RUNS = 3


def collect_hotspots(
    db_path: str,
    lookback: int = _DEFAULT_LOOKBACK,
    top_n: int = _DEFAULT_TOP_N,
    min_runs: int = _DEFAULT_MIN_RUNS,
) -> List[HotspotResult]:
    records = fetch_recent(db_path, limit=lookback)
    return scan_hotspots(records, top_n=top_n, min_runs=min_runs)
