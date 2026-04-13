"""Helpers to integrate circuit breaker with the scheduler / CLI."""
from __future__ import annotations

from pathlib import Path
from typing import List

from pipe_sentinel.circuit_breaker import CircuitBreaker
from pipe_sentinel.config import PipelineConfig, SentinelConfig
from pipe_sentinel.runner import RunResult


def breaker_from_config(cfg: SentinelConfig, state_file: Path) -> CircuitBreaker:
    """Build a CircuitBreaker using thresholds from SentinelConfig if present."""
    threshold = getattr(cfg, "cb_threshold", 3)
    recovery = getattr(cfg, "cb_recovery_seconds", 300)
    return CircuitBreaker(
        state_file=state_file,
        threshold=threshold,
        recovery_seconds=recovery,
    )


def apply_results(breaker: CircuitBreaker, results: List[RunResult]) -> None:
    """Update circuit breaker state from a batch of run results."""
    for result in results:
        if result.success:
            breaker.record_success(result.pipeline_name)
        else:
            breaker.record_failure(result.pipeline_name)


def filter_blocked(
    pipelines: List[PipelineConfig],
    breaker: CircuitBreaker,
) -> tuple[List[PipelineConfig], List[str]]:
    """Return (allowed, blocked_names) based on open circuits."""
    allowed: List[PipelineConfig] = []
    blocked: List[str] = []
    for p in pipelines:
        if breaker.is_open(p.name):
            blocked.append(p.name)
        else:
            allowed.append(p)
    return allowed, blocked
