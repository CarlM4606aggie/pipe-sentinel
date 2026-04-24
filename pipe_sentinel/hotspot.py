"""Hotspot detection: identify pipelines with disproportionately high failure counts."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence

from pipe_sentinel.audit import AuditRecord

_DEFAULT_TOP_N = 5
_DEFAULT_MIN_RUNS = 3


@dataclass
class HotspotResult:
    pipeline: str
    total_runs: int
    failures: int
    failure_rate: float

    def __str__(self) -> str:
        pct = self.failure_rate * 100
        return (
            f"{self.pipeline}: {self.failures}/{self.total_runs} failures "
            f"({pct:.1f}%)"
        )

    @property
    def is_hotspot(self) -> bool:
        return self.failure_rate > 0.0 and self.failures > 0


def _group_by_pipeline(records: Sequence[AuditRecord]) -> Dict[str, List[AuditRecord]]:
    groups: Dict[str, List[AuditRecord]] = {}
    for rec in records:
        groups.setdefault(rec.pipeline, []).append(rec)
    return groups


def detect_hotspot(
    pipeline: str,
    records: Sequence[AuditRecord],
    min_runs: int = _DEFAULT_MIN_RUNS,
) -> HotspotResult | None:
    if len(records) < min_runs:
        return None
    failures = sum(1 for r in records if r.status != "success")
    rate = failures / len(records) if records else 0.0
    return HotspotResult(
        pipeline=pipeline,
        total_runs=len(records),
        failures=failures,
        failure_rate=rate,
    )


def scan_hotspots(
    records: Sequence[AuditRecord],
    top_n: int = _DEFAULT_TOP_N,
    min_runs: int = _DEFAULT_MIN_RUNS,
) -> List[HotspotResult]:
    groups = _group_by_pipeline(records)
    results: List[HotspotResult] = []
    for name, recs in groups.items():
        result = detect_hotspot(name, recs, min_runs=min_runs)
        if result is not None and result.is_hotspot:
            results.append(result)
    results.sort(key=lambda r: (-r.failure_rate, -r.failures))
    return results[:top_n]
