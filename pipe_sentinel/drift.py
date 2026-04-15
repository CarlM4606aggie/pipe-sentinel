"""Drift detection: flag pipelines whose success rate has dropped
between two time windows (recent vs. historical baseline)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from pipe_sentinel.audit import AuditRecord


@dataclass
class DriftResult:
    pipeline_name: str
    historical_rate: float   # 0.0 – 1.0
    recent_rate: float       # 0.0 – 1.0
    delta: float             # recent - historical (negative = degraded)
    threshold: float         # minimum acceptable delta before flagging

    @property
    def is_drifting(self) -> bool:
        return self.delta < -abs(self.threshold)

    def __str__(self) -> str:  # pragma: no cover
        direction = "↓" if self.is_drifting else "→"
        return (
            f"{direction} {self.pipeline_name}: "
            f"historical={self.historical_rate:.0%} "
            f"recent={self.recent_rate:.0%} "
            f"(Δ{self.delta:+.0%})"
        )


def _success_rate(records: Sequence[AuditRecord]) -> float:
    if not records:
        return 1.0
    return sum(1 for r in records if r.status == "success") / len(records)


def detect_drift(
    name: str,
    historical: Sequence[AuditRecord],
    recent: Sequence[AuditRecord],
    threshold: float = 0.15,
) -> DriftResult:
    """Compare success rates between two record windows for one pipeline."""
    hist_rate = _success_rate(historical)
    rec_rate = _success_rate(recent)
    return DriftResult(
        pipeline_name=name,
        historical_rate=hist_rate,
        recent_rate=rec_rate,
        delta=rec_rate - hist_rate,
        threshold=threshold,
    )


def scan_drift(
    names: Sequence[str],
    historical_map: dict[str, List[AuditRecord]],
    recent_map: dict[str, List[AuditRecord]],
    threshold: float = 0.15,
) -> List[DriftResult]:
    """Run drift detection across all named pipelines."""
    return [
        detect_drift(
            name,
            historical_map.get(name, []),
            recent_map.get(name, []),
            threshold,
        )
        for name in names
    ]
