"""Sliding-window failure rate tracker for pipeline runs."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional


@dataclass
class WindowConfig:
    """Configuration for a sliding window."""
    duration_minutes: int = 60
    min_runs: int = 3
    failure_threshold: float = 0.5  # 0.0 – 1.0

    def __post_init__(self) -> None:
        if self.duration_minutes <= 0:
            raise ValueError("duration_minutes must be positive")
        if not 0.0 <= self.failure_threshold <= 1.0:
            raise ValueError("failure_threshold must be between 0 and 1")


@dataclass
class WindowEntry:
    pipeline_name: str
    succeeded: bool
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class WindowResult:
    pipeline_name: str
    total: int
    failures: int
    failure_rate: float
    breached: bool
    window_minutes: int

    def __str__(self) -> str:
        status = "BREACHED" if self.breached else "OK"
        return (
            f"[{status}] {self.pipeline_name}: "
            f"{self.failures}/{self.total} failures "
            f"({self.failure_rate:.0%}) in last {self.window_minutes}m"
        )


def _prune(entries: List[WindowEntry], cutoff: datetime) -> List[WindowEntry]:
    return [e for e in entries if e.timestamp >= cutoff]


def evaluate_window(
    entries: List[WindowEntry],
    pipeline_name: str,
    config: WindowConfig,
    now: Optional[datetime] = None,
) -> WindowResult:
    """Evaluate the sliding window for a single pipeline."""
    now = now or datetime.utcnow()
    cutoff = now - timedelta(minutes=config.duration_minutes)
    relevant = _prune(
        [e for e in entries if e.pipeline_name == pipeline_name], cutoff
    )
    total = len(relevant)
    failures = sum(1 for e in relevant if not e.succeeded)
    rate = failures / total if total > 0 else 0.0
    breached = total >= config.min_runs and rate >= config.failure_threshold
    return WindowResult(
        pipeline_name=pipeline_name,
        total=total,
        failures=failures,
        failure_rate=rate,
        breached=breached,
        window_minutes=config.duration_minutes,
    )


def scan_windows(
    entries: List[WindowEntry],
    pipeline_names: List[str],
    config: WindowConfig,
    now: Optional[datetime] = None,
) -> List[WindowResult]:
    """Evaluate the sliding window for every pipeline in *pipeline_names*."""
    return [evaluate_window(entries, name, config, now) for name in pipeline_names]
