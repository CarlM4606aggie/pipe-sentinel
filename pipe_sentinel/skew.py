"""Detects scheduling skew — pipelines running significantly later than expected."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Sequence

from pipe_sentinel.audit import AuditRecord


@dataclass
class SkewResult:
    pipeline_name: str
    expected_interval_seconds: float
    actual_interval_seconds: float
    skew_seconds: float
    is_skewed: bool

    def __str__(self) -> str:  # pragma: no cover
        direction = "late" if self.skew_seconds > 0 else "early"
        return (
            f"[{'SKEWED' if self.is_skewed else 'OK'}] {self.pipeline_name}: "
            f"{abs(self.skew_seconds):.1f}s {direction} "
            f"(expected {self.expected_interval_seconds:.0f}s, "
            f"actual {self.actual_interval_seconds:.1f}s)"
        )


def _timestamps(records: Sequence[AuditRecord]) -> List[float]:
    """Return sorted list of run timestamps as epoch floats."""
    result = []
    for r in records:
        try:
            dt = datetime.fromisoformat(r.timestamp)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            result.append(dt.timestamp())
        except (ValueError, AttributeError):
            continue
    return sorted(result)


def detect_skew(
    pipeline_name: str,
    records: Sequence[AuditRecord],
    expected_interval_seconds: float,
    tolerance_fraction: float = 0.2,
) -> Optional[SkewResult]:
    """Detect scheduling skew for a single pipeline.

    Returns None when there are fewer than two records to compare.
    """
    if expected_interval_seconds <= 0:
        raise ValueError("expected_interval_seconds must be positive")
    if not (0.0 < tolerance_fraction < 1.0):
        raise ValueError("tolerance_fraction must be between 0 and 1 exclusive")

    timestamps = _timestamps(records)
    if len(timestamps) < 2:
        return None

    # Use the most recent gap
    actual_interval = timestamps[-1] - timestamps[-2]
    skew = actual_interval - expected_interval_seconds
    threshold = expected_interval_seconds * tolerance_fraction
    is_skewed = abs(skew) > threshold

    return SkewResult(
        pipeline_name=pipeline_name,
        expected_interval_seconds=expected_interval_seconds,
        actual_interval_seconds=actual_interval,
        skew_seconds=skew,
        is_skewed=is_skewed,
    )


def scan_skew(
    pipelines: Sequence,
    records_by_name: dict,
    tolerance_fraction: float = 0.2,
) -> List[SkewResult]:
    """Scan all pipelines that define an expected_interval_seconds."""
    results = []
    for pipeline in pipelines:
        interval = getattr(pipeline, "expected_interval_seconds", None)
        if interval is None:
            continue
        name = pipeline.name
        recs = records_by_name.get(name, [])
        result = detect_skew(name, recs, interval, tolerance_fraction)
        if result is not None:
            results.append(result)
    return results
