"""Formatting helpers for baseline violation reports."""
from __future__ import annotations

from typing import List

from pipe_sentinel.baseline import BaselineStats, BaselineViolation


HEADER = "=" * 56


def format_baseline_stats(stats: BaselineStats) -> str:
    lines = [
        f"  Pipeline : {stats.pipeline_name}",
        f"  Samples  : {stats.sample_count}",
        f"  Mean     : {stats.mean_duration:.2f}s",
        f"  Std Dev  : {stats.std_duration:.2f}s",
        f"  Upper    : {stats.upper_bound:.2f}s  (×{stats.threshold_multiplier}σ)",
    ]
    return "\n".join(lines)


def format_violation(v: BaselineViolation) -> str:
    return (
        f"  ⚠  {v.pipeline_name}\n"
        f"     actual={v.actual_duration:.2f}s  "
        f"limit={v.baseline.upper_bound:.2f}s  "
        f"excess=+{v.excess_seconds:.2f}s"
    )


def build_baseline_report(violations: List[BaselineViolation]) -> str:
    if not violations:
        return "Baseline check: all pipelines within expected duration.\n"
    lines = [
        HEADER,
        f"BASELINE VIOLATIONS  ({len(violations)} found)",
        HEADER,
    ]
    for v in violations:
        lines.append(format_violation(v))
    lines.append(HEADER)
    return "\n".join(lines) + "\n"


def print_baseline_report(violations: List[BaselineViolation]) -> None:
    print(build_baseline_report(violations), end="")
