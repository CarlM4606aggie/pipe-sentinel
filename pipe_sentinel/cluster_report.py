"""Formatting helpers for ClusterReport."""
from __future__ import annotations

from typing import List

from pipe_sentinel.cluster import ClusterReport, ClusterResult


def _icon(size: int) -> str:
    if size >= 5:
        return "🔴"
    if size >= 2:
        return "🟡"
    return "⚪"


def format_cluster_result(result: ClusterResult) -> str:
    icon = _icon(result.size)
    pipes = ", ".join(result.pipelines)
    sample = result.sample_error[:72]
    lines = [
        f"{icon} Cluster {result.cluster_id}  ({result.size} pipeline(s))",
        f"   Pipelines : {pipes}",
        f"   Sample    : {sample}",
    ]
    return "\n".join(lines)


def build_cluster_report(report: ClusterReport) -> str:
    header = (
        f"Pipeline Failure Clusters — "
        f"{report.total_clusters} cluster(s), "
        f"{report.multi_count} multi-pipeline"
    )
    separator = "─" * 60
    sections: List[str] = [header, separator]

    if not report.clusters:
        sections.append("  No failure clusters detected.")
    else:
        for result in report.clusters:
            sections.append(format_cluster_result(result))

    sections.append(separator)
    sections.append(
        f"Singletons: {report.singleton_count}  "
        f"Multi-pipeline groups: {report.multi_count}"
    )
    return "\n".join(sections)


def print_cluster_report(report: ClusterReport) -> None:  # pragma: no cover
    print(build_cluster_report(report))
