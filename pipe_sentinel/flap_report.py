"""Formatting helpers for flap detection results."""
from __future__ import annotations
from typing import List

from pipe_sentinel.flap import FlapResult


def _icon(result: FlapResult) -> str:
    return "⚡" if result.is_flapping else "✔"


def format_flap_result(result: FlapResult) -> str:
    icon = _icon(result)
    label = "FLAPPING" if result.is_flapping else "stable"
    return (
        f"{icon} {result.pipeline_name:<30} {label:<10} "
        f"{result.transitions} transitions / {result.window_size} runs "
        f"(threshold {result.threshold})"
    )


def build_flap_report(results: List[FlapResult]) -> str:
    if not results:
        return "No pipeline data available for flap detection."
    flapping = [r for r in results if r.is_flapping]
    lines = [
        f"Flap Detection Report  ({len(flapping)}/{len(results)} flapping)",
        "-" * 60,
    ]
    for r in results:
        lines.append(format_flap_result(r))
    return "\n".join(lines)


def print_flap_report(results: List[FlapResult]) -> None:
    print(build_flap_report(results))
