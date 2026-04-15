"""SLA tracking: check whether pipelines complete within their SLA window."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List

from pipe_sentinel.runner import RunResult


@dataclass
class SLAConfig:
    pipeline_name: str
    max_duration_seconds: float
    warn_fraction: float = 0.8  # warn when duration exceeds this fraction of max

    def __post_init__(self) -> None:
        if self.max_duration_seconds <= 0:
            raise ValueError("max_duration_seconds must be positive")
        if not (0.0 < self.warn_fraction < 1.0):
            raise ValueError("warn_fraction must be between 0 and 1 exclusive")

    @property
    def warn_threshold(self) -> float:
        return self.max_duration_seconds * self.warn_fraction


@dataclass
class SLAResult:
    pipeline_name: str
    duration: float
    max_duration: float
    breached: bool
    warned: bool

    def __str__(self) -> str:
        if self.breached:
            status = "BREACHED"
        elif self.warned:
            status = "WARNING"
        else:
            status = "OK"
        return (
            f"SLA[{self.pipeline_name}] {status} "
            f"({self.duration:.1f}s / {self.max_duration:.1f}s)"
        )


def check_sla(result: RunResult, config: SLAConfig) -> SLAResult:
    """Evaluate a single RunResult against its SLAConfig."""
    duration = result.duration or 0.0
    breached = duration > config.max_duration_seconds
    warned = (not breached) and duration > config.warn_threshold
    return SLAResult(
        pipeline_name=config.pipeline_name,
        duration=duration,
        max_duration=config.max_duration_seconds,
        breached=breached,
        warned=warned,
    )


def scan_sla(
    results: List[RunResult], configs: List[SLAConfig]
) -> List[SLAResult]:
    """Check all results against matching SLA configs."""
    config_map = {c.pipeline_name: c for c in configs}
    sla_results: List[SLAResult] = []
    for r in results:
        cfg = config_map.get(r.pipeline_name)
        if cfg is not None:
            sla_results.append(check_sla(r, cfg))
    return sla_results
