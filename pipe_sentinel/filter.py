"""Pipeline filtering utilities for selecting subsets of pipelines by name or tag."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from pipe_sentinel.config import PipelineConfig


@dataclass
class FilterCriteria:
    """Criteria used to select a subset of pipelines."""

    names: Optional[List[str]] = None  # exact pipeline names
    tags: Optional[List[str]] = None   # pipelines must have at least one matching tag

    def is_empty(self) -> bool:
        """Return True when no criteria are set (all pipelines pass)."""
        return not self.names and not self.tags


def _matches_names(pipeline: PipelineConfig, names: List[str]) -> bool:
    return pipeline.name in names


def _matches_tags(pipeline: PipelineConfig, tags: List[str]) -> bool:
    pipeline_tags: List[str] = getattr(pipeline, "tags", []) or []
    return any(t in pipeline_tags for t in tags)


def apply_filter(
    pipelines: Sequence[PipelineConfig],
    criteria: FilterCriteria,
) -> List[PipelineConfig]:
    """Return the subset of *pipelines* that satisfy *criteria*.

    When *criteria* is empty every pipeline is returned unchanged.
    When both names and tags are provided a pipeline must satisfy
    at least one of the two conditions (union semantics).
    """
    if criteria.is_empty():
        return list(pipelines)

    result: List[PipelineConfig] = []
    for pipeline in pipelines:
        matched = False
        if criteria.names and _matches_names(pipeline, criteria.names):
            matched = True
        if not matched and criteria.tags and _matches_tags(pipeline, criteria.tags):
            matched = True
        if matched:
            result.append(pipeline)
    return result


def filter_by_names(
    pipelines: Sequence[PipelineConfig],
    names: List[str],
) -> List[PipelineConfig]:
    """Convenience wrapper: filter pipelines by exact name match."""
    return apply_filter(pipelines, FilterCriteria(names=names))


def filter_by_tags(
    pipelines: Sequence[PipelineConfig],
    tags: List[str],
) -> List[PipelineConfig]:
    """Convenience wrapper: filter pipelines by tag membership."""
    return apply_filter(pipelines, FilterCriteria(tags=tags))
