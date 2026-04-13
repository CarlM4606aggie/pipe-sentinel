"""Tag-based pipeline grouping and lookup utilities."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Set

from pipe_sentinel.config import PipelineConfig


@dataclass
class TagIndex:
    """Inverted index mapping tag -> list of pipeline names."""
    _index: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))

    def add(self, pipeline: PipelineConfig) -> None:
        """Register a pipeline under each of its tags."""
        for tag in pipeline.tags:
            self._index[tag].append(pipeline.name)

    def pipelines_for_tag(self, tag: str) -> List[str]:
        """Return pipeline names associated with *tag*."""
        return list(self._index.get(tag, []))

    def all_tags(self) -> Set[str]:
        """Return the set of all known tags."""
        return set(self._index.keys())

    def __len__(self) -> int:
        return len(self._index)


def build_tag_index(pipelines: List[PipelineConfig]) -> TagIndex:
    """Build a :class:`TagIndex` from a list of pipeline configs."""
    index = TagIndex()
    for pipeline in pipelines:
        index.add(pipeline)
    return index


def pipelines_by_tag(
    pipelines: List[PipelineConfig],
) -> Dict[str, List[PipelineConfig]]:
    """Return a dict mapping each tag to the pipelines that carry it."""
    result: Dict[str, List[PipelineConfig]] = defaultdict(list)
    for pipeline in pipelines:
        for tag in pipeline.tags:
            result[tag].append(pipeline)
    return dict(result)


def tags_for_pipeline(pipeline: PipelineConfig) -> List[str]:
    """Convenience: return sorted tags for a single pipeline."""
    return sorted(pipeline.tags)
