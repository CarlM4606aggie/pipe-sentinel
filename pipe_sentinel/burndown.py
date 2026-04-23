"""Burndown: track how quickly a pipeline recovers from a failure streak."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from pipe_sentinel.audit import AuditRecord


@dataclass
class BurndownResult:
    pipeline: str
    total_failures: int
    resolved: int          # failures followed eventually by a success
    remaining: int         # failures not yet followed by a success
    burn_rate: float       # resolved / total_failures  (0.0 – 1.0)
    is_clear: bool         # remaining == 0

    def __str__(self) -> str:
        pct = f"{self.burn_rate * 100:.1f}%"
        status = "CLEAR" if self.is_clear else f"{self.remaining} remaining"
        return (
            f"[{self.pipeline}] burndown {pct} resolved "
            f"({self.resolved}/{self.total_failures}) — {status}"
        )


def _resolved_failures(records: Sequence[AuditRecord]) -> int:
    """Count failures that are followed by at least one success."""
    resolved = 0
    found_success = False
    for rec in reversed(records):
        if rec.status == "success":
            found_success = True
        elif rec.status == "failure" and found_success:
            resolved += 1
    return resolved


def compute_burndown(
    pipeline: str,
    records: Sequence[AuditRecord],
) -> Optional[BurndownResult]:
    """Return a BurndownResult for *pipeline*, or None if no failures exist."""
    failures = [r for r in records if r.status == "failure"]
    if not failures:
        return None
    total = len(failures)
    resolved = _resolved_failures(records)
    remaining = total - resolved
    rate = resolved / total if total else 0.0
    return BurndownResult(
        pipeline=pipeline,
        total_failures=total,
        resolved=resolved,
        remaining=remaining,
        burn_rate=rate,
        is_clear=(remaining == 0),
    )


def scan_burndowns(
    records_by_pipeline: dict[str, List[AuditRecord]],
) -> List[BurndownResult]:
    """Compute burndown for every pipeline that has at least one failure."""
    results: List[BurndownResult] = []
    for name, recs in records_by_pipeline.items():
        result = compute_burndown(name, recs)
        if result is not None:
            results.append(result)
    results.sort(key=lambda r: r.remaining, reverse=True)
    return results
