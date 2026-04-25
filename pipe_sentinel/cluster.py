"""Cluster pipelines by failure pattern similarity."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Sequence


@dataclass
class ClusterResult:
    """A group of pipelines sharing a common failure pattern."""

    cluster_id: str
    pipelines: List[str]
    sample_error: str
    size: int = field(init=False)

    def __post_init__(self) -> None:
        self.size = len(self.pipelines)

    def __str__(self) -> str:
        pipes = ", ".join(self.pipelines)
        return f"[{self.cluster_id}] {self.size} pipeline(s): {pipes} — {self.sample_error[:60]}"


@dataclass
class ClusterReport:
    clusters: List[ClusterResult]

    @property
    def total_clusters(self) -> int:
        return len(self.clusters)

    @property
    def singleton_count(self) -> int:
        return sum(1 for c in self.clusters if c.size == 1)

    @property
    def multi_count(self) -> int:
        return sum(1 for c in self.clusters if c.size > 1)


def _normalise(error: str) -> str:
    """Strip volatile tokens so similar errors hash the same."""
    import re

    error = re.sub(r"\b0x[0-9a-fA-F]+\b", "<addr>", error)
    error = re.sub(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\b", "<ts>", error)
    error = re.sub(r"line \d+", "line <N>", error)
    error = re.sub(r"\d+", "<N>", error)
    return error.strip().lower()


def _fingerprint(error: str) -> str:
    import hashlib

    return hashlib.md5(_normalise(error).encode()).hexdigest()[:8]


def cluster_failures(
    failures: Sequence[tuple[str, str]],
) -> ClusterReport:
    """Group (pipeline_name, error_message) pairs into clusters.

    Args:
        failures: sequence of (pipeline_name, error_text) tuples.

    Returns:
        ClusterReport with one ClusterResult per distinct fingerprint.
    """
    buckets: Dict[str, List[str]] = {}
    samples: Dict[str, str] = {}

    for name, error in failures:
        fp = _fingerprint(error)
        buckets.setdefault(fp, []).append(name)
        samples.setdefault(fp, error)

    clusters = [
        ClusterResult(
            cluster_id=fp,
            pipelines=sorted(set(names)),
            sample_error=samples[fp],
        )
        for fp, names in sorted(buckets.items(), key=lambda kv: -len(kv[1]))
    ]
    return ClusterReport(clusters=clusters)
