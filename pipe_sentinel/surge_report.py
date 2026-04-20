"""Formatting helpers for surge detection results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.surge import SurgeResult


def _icon(result: SurgeResult) -> str:
    return "🔺" if result.is_surging else "✅"


def format_surge_result(result: SurgeResult) -> str:
    lines = [
        f"{_icon(result)} {result.pipeline}",
        f"   Recent failures : {result.recent_failures}",
        f"   Baseline (avg)  : {result.baseline_failures:.1f}",
        f"   Surge ratio     : {result.ratio:.1f}x",
        f"   Status          : {'SURGING' if result.is_surging else 'normal'}",
    ]
    return "\n".join(lines)


def build_surge_report(results: List[SurgeResult]) -> str:
    if not results:
        return "No surge data available."

    surging = [r for r in results if r.is_surging]
    header = [
        "=== Surge Report ===",
        f"Pipelines checked : {len(results)}",
        f"Surging           : {len(surging)}",
        "",
    ]
    body = []
    for r in results:
        body.append(format_surge_result(r))
        body.append("")

    return "\n".join(header + body).rstrip()


def print_surge_report(results: List[SurgeResult]) -> None:
    print(build_surge_report(results))
