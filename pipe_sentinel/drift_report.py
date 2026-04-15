"""Formatting helpers for drift detection results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.drift import DriftResult


def _icon(result: DriftResult) -> str:
    return "⚠" if result.is_drifting else "✓"


def format_drift_result(result: DriftResult) -> str:
    icon = _icon(result)
    return (
        f"  {icon} {result.pipeline_name:<30} "
        f"hist={result.historical_rate:>6.1%}  "
        f"recent={result.recent_rate:>6.1%}  "
        f"delta={result.delta:>+7.1%}"
    )


def build_drift_report(results: List[DriftResult]) -> str:
    if not results:
        return "Drift Report\n  (no pipelines)"

    drifting = [r for r in results if r.is_drifting]
    lines: List[str] = [
        "Drift Report",
        f"  Pipelines checked : {len(results)}",
        f"  Drifting          : {len(drifting)}",
        "",
    ]
    for result in sorted(results, key=lambda r: r.delta):
        lines.append(format_drift_result(result))
    return "\n".join(lines)


def print_drift_report(results: List[DriftResult]) -> None:  # pragma: no cover
    print(build_drift_report(results))
