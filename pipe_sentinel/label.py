"""Pipeline label management — attach arbitrary key/value labels to pipelines
and query/filter by them."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class LabelSet:
    """An immutable-ish mapping of string labels for a single pipeline."""

    pipeline: str
    labels: Dict[str, str] = field(default_factory=dict)

    def get(self, key: str) -> Optional[str]:
        return self.labels.get(key)

    def matches(self, selector: Dict[str, str]) -> bool:
        """Return True when every key/value in *selector* is present in labels."""
        return all(self.labels.get(k) == v for k, v in selector.items())

    def __len__(self) -> int:
        return len(self.labels)


@dataclass
class LabelIndex:
    """Collection of LabelSets, indexed by pipeline name."""

    _index: Dict[str, LabelSet] = field(default_factory=dict, repr=False)

    def add(self, label_set: LabelSet) -> None:
        self._index[label_set.pipeline] = label_set

    def for_pipeline(self, pipeline: str) -> Optional[LabelSet]:
        return self._index.get(pipeline)

    def select(self, selector: Dict[str, str]) -> List[str]:
        """Return pipeline names whose labels match all key/value pairs in *selector*."""
        return [
            name
            for name, ls in self._index.items()
            if ls.matches(selector)
        ]

    def all_pipelines(self) -> List[str]:
        return list(self._index.keys())


def build_label_index(pipelines: list) -> LabelIndex:
    """Build a LabelIndex from a list of PipelineConfig objects."""
    index = LabelIndex()
    for p in pipelines:
        raw: Dict[str, str] = getattr(p, "labels", {}) or {}
        index.add(LabelSet(pipeline=p.name, labels=dict(raw)))
    return index
