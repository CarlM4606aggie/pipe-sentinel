"""Cascade failure detection — identify pipelines whose failures
correlate with upstream dependency failures."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Sequence

from pipe_sentinel.dependency import DependencyGraph
from pipe_sentinel.audit import AuditRecord


@dataclass
class CascadeResult:
    pipeline: str
    failed: bool
    upstream_failures: List[str] = field(default_factory=list)

    @property
    def is_cascade(self) -> bool:
        """True when this pipeline failed and at least one upstream also failed."""
        return self.failed and bool(self.upstream_failures)

    def __str__(self) -> str:
        if self.is_cascade:
            ups = ", ".join(self.upstream_failures)
            return f"[CASCADE] {self.pipeline} failed; upstream failures: {ups}"
        if self.failed:
            return f"[ISOLATED] {self.pipeline} failed with no upstream failures"
        return f"[OK] {self.pipeline}"


@dataclass
class CascadeReport:
    results: List[CascadeResult] = field(default_factory=list)

    @property
    def cascades(self) -> List[CascadeResult]:
        return [r for r in self.results if r.is_cascade]

    @property
    def isolated_failures(self) -> List[CascadeResult]:
        return [r for r in self.results if r.failed and not r.is_cascade]

    @property
    def has_cascades(self) -> bool:
        return bool(self.cascades)


def _failed_names(records: Sequence[AuditRecord]) -> Dict[str, bool]:
    """Return mapping of pipeline name -> True if its latest run failed."""
    latest: Dict[str, AuditRecord] = {}
    for rec in records:
        prev = latest.get(rec.pipeline)
        if prev is None or rec.timestamp > prev.timestamp:
            latest[rec.pipeline] = rec
    return {name: rec.status != "success" for name, rec in latest.items()}


def detect_cascade(
    pipeline: str,
    graph: DependencyGraph,
    failed_map: Dict[str, bool],
) -> CascadeResult:
    """Analyse a single pipeline for cascade conditions."""
    this_failed = failed_map.get(pipeline, False)
    upstream_failures = [
        p for p in graph.predecessors(pipeline) if failed_map.get(p, False)
    ]
    return CascadeResult(
        pipeline=pipeline,
        failed=this_failed,
        upstream_failures=upstream_failures,
    )


def scan_cascades(
    graph: DependencyGraph,
    records: Sequence[AuditRecord],
) -> CascadeReport:
    """Scan all known pipelines for cascade failures."""
    failed_map = _failed_names(records)
    results = [
        detect_cascade(name, graph, failed_map) for name in sorted(graph.all_names())
    ]
    return CascadeReport(results=results)
