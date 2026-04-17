"""Flap detection: identify pipelines that oscillate between pass and fail."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List

from pipe_sentinel.audit import AuditRecord


@dataclass
class FlapResult:
    pipeline_name: str
    transitions: int
    window_size: int
    is_flapping: bool
    threshold: int

    def __str__(self) -> str:
        status = "FLAPPING" if self.is_flapping else "stable"
        return (
            f"{self.pipeline_name}: {status} "
            f"({self.transitions} transitions in last {self.window_size} runs)"
        )


def _count_transitions(records: List[AuditRecord]) -> int:
    """Count status changes in an ordered list of records (oldest first)."""
    if len(records) < 2:
        return 0
    transitions = 0
    for i in range(1, len(records)):
        if records[i].status != records[i - 1].status:
            transitions += 1
    return transitions


def detect_flap(
    pipeline_name: str,
    records: List[AuditRecord],
    window: int = 10,
    threshold: int = 4,
) -> FlapResult:
    """Detect flapping for a single pipeline."""
    recent = sorted(records, key=lambda r: r.started_at)[-window:]
    transitions = _count_transitions(recent)
    return FlapResult(
        pipeline_name=pipeline_name,
        transitions=transitions,
        window_size=len(recent),
        is_flapping=transitions >= threshold,
        threshold=threshold,
    )


def scan_flaps(
    all_records: List[AuditRecord],
    window: int = 10,
    threshold: int = 4,
) -> List[FlapResult]:
    """Scan all pipelines for flapping behaviour."""
    grouped: dict[str, List[AuditRecord]] = {}
    for rec in all_records:
        grouped.setdefault(rec.pipeline_name, []).append(rec)
    return [
        detect_flap(name, recs, window, threshold)
        for name, recs in sorted(grouped.items())
    ]
