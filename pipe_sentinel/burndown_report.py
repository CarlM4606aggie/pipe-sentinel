"""Formatting helpers for burndown results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.burndown import BurndownResult


def _bar(rate: float, width: int = 20) -> str:
    """Render a simple ASCII progress bar for *rate* (0.0 – 1.0)."""
    filled = round(rate * width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def _icon(result: BurndownResult) -> str:
    if result.is_clear:
        return "✅"
    if result.burn_rate >= 0.5:
        return "🔶"
    return "🔴"


def format_burndown_result(result: BurndownResult) -> str:
    icon = _icon(result)
    bar = _bar(result.burn_rate)
    pct = f"{result.burn_rate * 100:.1f}%"
    lines = [
        f"{icon} {result.pipeline}",
        f"   {bar} {pct} resolved",
        f"   failures: {result.total_failures}  resolved: {result.resolved}  remaining: {result.remaining}",
    ]
    return "\n".join(lines)


def build_burndown_report(results: List[BurndownResult]) -> str:
    if not results:
        return "No pipeline failures to burn down."
    header = f"Burndown Report  ({len(results)} pipeline(s) with failures)"
    separator = "─" * len(header)
    body = "\n\n".join(format_burndown_result(r) for r in results)
    return "\n".join([header, separator, "", body])


def print_burndown_report(results: List[BurndownResult]) -> None:
    print(build_burndown_report(results))
