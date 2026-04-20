"""Recovery tracking: detect when pipelines recover after consecutive failures."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipe_sentinel.audit import AuditRecord


@dataclass
class RecoveryResult:
    pipeline_name: str
    recovered: bool
    previous_failures: int
    # timestamp of the successful run that ended the failure streak
    recovered_at: Optional[str] = None

    def __str__(self) -> str:
        if self.recovered:
            return (
                f"[RECOVERED] {self.pipeline_name} — "
                f"recovered after {self.previous_failures} failure(s) at {self.recovered_at}"
            )
        return f"[OK] {self.pipeline_name} — no recovery event"


def _consecutive_failures_before_last(records: List[AuditRecord]) -> int:
    """Count consecutive failures immediately preceding the most-recent record.

    Records are expected newest-first.  We skip the first record (index 0)
    and count failures from index 1 onwards until we see a success.
    """
    count = 0
    for rec in records[1:]:
        if rec.status == "failure":
            count += 1
        else:
            break
    return count


def detect_recovery(records: List[AuditRecord]) -> RecoveryResult:
    """Return a RecoveryResult for a single pipeline's record list.

    A recovery is detected when the most-recent run succeeded and it was
    preceded by at least one failure.
    """
    if not records:
        return RecoveryResult(pipeline_name="", recovered=False, previous_failures=0)

    latest = records[0]
    pipeline_name = latest.pipeline_name

    if latest.status != "success":
        return RecoveryResult(
            pipeline_name=pipeline_name,
            recovered=False,
            previous_failures=0,
        )

    prior_failures = _consecutive_failures_before_last(records)
    if prior_failures == 0:
        return RecoveryResult(
            pipeline_name=pipeline_name,
            recovered=False,
            previous_failures=0,
        )

    return RecoveryResult(
        pipeline_name=pipeline_name,
        recovered=True,
        previous_failures=prior_failures,
        recovered_at=latest.finished_at,
    )


def scan_recoveries(
    all_records: List[AuditRecord],
    min_prior_failures: int = 1,
) -> List[RecoveryResult]:
    """Group records by pipeline and detect recoveries across all pipelines."""
    from collections import defaultdict

    grouped: dict = defaultdict(list)
    for rec in all_records:
        grouped[rec.pipeline_name].append(rec)

    results: List[RecoveryResult] = []
    for name, recs in grouped.items():
        # sort newest-first
        recs_sorted = sorted(recs, key=lambda r: r.finished_at, reverse=True)
        result = detect_recovery(recs_sorted)
        if result.recovered and result.previous_failures >= min_prior_failures:
            results.append(result)

    return results
