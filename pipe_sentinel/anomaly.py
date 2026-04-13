"""Anomaly detection: flag pipelines whose recent failure rate spikes above baseline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from pipe_sentinel.audit import AuditRecord


@dataclass
class AnomalyResult:
    pipeline_name: str
    recent_failure_rate: float   # 0.0 – 1.0 over the short window
    baseline_failure_rate: float # 0.0 – 1.0 over the long window
    spike_ratio: float           # recent / baseline  (inf when baseline == 0)
    is_anomaly: bool

    def __str__(self) -> str:
        symbol = "🚨" if self.is_anomaly else "✅"
        return (
            f"{symbol} {self.pipeline_name}: "
            f"recent={self.recent_failure_rate:.0%} "
            f"baseline={self.baseline_failure_rate:.0%} "
            f"ratio={self.spike_ratio:.2f}"
        )


def _failure_rate(records: Sequence[AuditRecord]) -> float:
    if not records:
        return 0.0
    failures = sum(1 for r in records if r.status != "success")
    return failures / len(records)


def detect_anomaly(
    name: str,
    all_records: Sequence[AuditRecord],
    short_window: int = 5,
    long_window: int = 20,
    spike_threshold: float = 2.0,
) -> Optional[AnomalyResult]:
    """Return an AnomalyResult if recent failure rate spikes above baseline.

    Returns None when there are not enough records to make a judgement.
    """
    relevant = [r for r in all_records if r.pipeline_name == name]
    if len(relevant) < short_window:
        return None

    recent = relevant[-short_window:]
    baseline_records = relevant[-long_window:] if len(relevant) >= long_window else relevant

    recent_rate = _failure_rate(recent)
    baseline_rate = _failure_rate(baseline_records)

    if baseline_rate == 0.0:
        spike_ratio = float("inf") if recent_rate > 0 else 1.0
    else:
        spike_ratio = recent_rate / baseline_rate

    is_anomaly = spike_ratio >= spike_threshold and recent_rate > 0.0

    return AnomalyResult(
        pipeline_name=name,
        recent_failure_rate=recent_rate,
        baseline_failure_rate=baseline_rate,
        spike_ratio=spike_ratio,
        is_anomaly=is_anomaly,
    )


def scan_anomalies(
    all_records: Sequence[AuditRecord],
    short_window: int = 5,
    long_window: int = 20,
    spike_threshold: float = 2.0,
) -> List[AnomalyResult]:
    """Run anomaly detection across every distinct pipeline found in *all_records*."""
    names = list(dict.fromkeys(r.pipeline_name for r in all_records))
    results: List[AnomalyResult] = []
    for name in names:
        result = detect_anomaly(name, all_records, short_window, long_window, spike_threshold)
        if result is not None:
            results.append(result)
    return results
