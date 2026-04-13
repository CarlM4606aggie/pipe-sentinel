"""Collect cooldown state from run results and pipeline config."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from pipe_sentinel.config import PipelineConfig
from pipe_sentinel.cooldown import CooldownStore
from pipe_sentinel.runner import RunResult


DEFAULT_COOLDOWN_SECONDS = 300  # 5 minutes


def store_from_path(path: Path) -> CooldownStore:
    return CooldownStore(path=path)


def apply_failures(
    store: CooldownStore,
    results: List[RunResult],
    pipelines: List[PipelineConfig],
    now: Optional[float] = None,
) -> None:
    """Record cooldown entries for failed pipelines."""
    cooldown_map = {
        p.name: getattr(p, "cooldown_seconds", DEFAULT_COOLDOWN_SECONDS)
        for p in pipelines
    }
    for result in results:
        if not result.success:
            cooldown_secs = cooldown_map.get(result.pipeline_name, DEFAULT_COOLDOWN_SECONDS)
            store.record_failure(result.pipeline_name, cooldown_secs, now=now)


def filter_blocked(
    pipelines: List[PipelineConfig],
    store: CooldownStore,
    now: Optional[float] = None,
) -> tuple[List[PipelineConfig], List[PipelineConfig]]:
    """Return (allowed, blocked) pipeline lists based on cooldown state."""
    allowed: List[PipelineConfig] = []
    blocked: List[PipelineConfig] = []
    for pipeline in pipelines:
        if store.is_cooling(pipeline.name, now=now):
            blocked.append(pipeline)
        else:
            allowed.append(pipeline)
    return allowed, blocked
