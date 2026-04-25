"""Formatting helpers for breach detection results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.breach import BreachResult


def _icon(result: BreachResult) -> str:
    return "🔴" if result.breached else "✅"


def format_breach_result(result: BreachResult) -> str:
    icon = _icon(result)
    bar_width = 20
    filled = round(result.failure_rate * bar_width)
    bar = "█" * filled + "░" * (bar_width - filled)
    threshold_marker = round(result.threshold * bar_width)
    threshold_str = f"threshold={result.threshold:.0%}"
    return (
        f"{icon} {result.pipeline}\n"
        f"   [{bar}] {result.failure_rate:.1%}  ({threshold_str}, "
        f"marker at {threshold_marker}/{bar_width})\n"
        f"   Failures: {result.failure_count}/{result.total_runs}"
    )


def build_breach_report(results: List[BreachResult]) -> str:
    if not results:
        return "No breach data available."
    breached = [r for r in results if r.breached]
    lines = [
        f"Breach Report — {len(breached)}/{len(results)} pipeline(s) breached",
        "=" * 50,
    ]
    for result in results:
        lines.append(format_breach_result(result))
    return "\n".join(lines)


def print_breach_report(results: List[BreachResult]) -> None:
    print(build_breach_report(results))
