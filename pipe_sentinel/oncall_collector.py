"""Collect on-call data from SentinelConfig and resolve owners for failures."""
from __future__ import annotations

from typing import Dict, List

from pipe_sentinel.config import SentinelConfig
from pipe_sentinel.oncall import OnCallRotation, load_rotation
from pipe_sentinel.runner import RunResult


def rotation_from_config(cfg: SentinelConfig) -> OnCallRotation:
    """Extract on-call rotation from *cfg*, returning an empty rotation if absent."""
    raw = getattr(cfg, "oncall", None)
    return load_rotation(raw)


def owners_for_failures(
    rotation: OnCallRotation,
    results: List[RunResult],
) -> Dict[str, List[str]]:
    """Return a mapping of pipeline name -> list of owner emails for failed runs."""
    mapping: Dict[str, List[str]] = {}
    for result in results:
        if not result.success:
            emails = rotation.emails_for(result.pipeline_name)
            if emails:
                mapping[result.pipeline_name] = emails
    return mapping
