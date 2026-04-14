"""Per-pipeline timeout policy with override support."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipe_sentinel.config import PipelineConfig, SentinelConfig


@dataclass
class TimeoutPolicy:
    """Resolved timeout policy for a single pipeline."""

    pipeline_name: str
    timeout_seconds: int
    source: str  # 'pipeline', 'global', or 'default'

    def __str__(self) -> str:
        return (
            f"{self.pipeline_name}: {self.timeout_seconds}s "
            f"(source: {self.source})"
        )


_DEFAULT_TIMEOUT = 300  # 5 minutes


def resolve_policy(
    pipeline: PipelineConfig,
    global_timeout: Optional[int] = None,
) -> TimeoutPolicy:
    """Resolve the effective timeout for a pipeline.

    Priority: pipeline-level > global > built-in default.
    """
    if pipeline.timeout is not None:
        return TimeoutPolicy(
            pipeline_name=pipeline.name,
            timeout_seconds=pipeline.timeout,
            source="pipeline",
        )
    if global_timeout is not None:
        return TimeoutPolicy(
            pipeline_name=pipeline.name,
            timeout_seconds=global_timeout,
            source="global",
        )
    return TimeoutPolicy(
        pipeline_name=pipeline.name,
        timeout_seconds=_DEFAULT_TIMEOUT,
        source="default",
    )


def resolve_all(
    config: SentinelConfig,
) -> List[TimeoutPolicy]:
    """Resolve timeout policies for every pipeline in *config*."""
    global_timeout: Optional[int] = getattr(config, "default_timeout", None)
    return [
        resolve_policy(pipeline, global_timeout)
        for pipeline in config.pipelines
    ]


def build_policy_map(
    config: SentinelConfig,
) -> Dict[str, TimeoutPolicy]:
    """Return a dict keyed by pipeline name for quick look-up."""
    return {p.pipeline_name: p for p in resolve_all(config)}
