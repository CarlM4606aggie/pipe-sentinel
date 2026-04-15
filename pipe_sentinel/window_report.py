"""Formatting helpers for sliding-window results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.window import WindowResult

_BREACH_ICON = "\u26a0\ufe0f "
_OK_ICON = "\u2705 "


def _icon(result: WindowResult) -> str:
    return _BREACH_ICON if result.breached else _OK_ICON


def format_window_result(result: WindowResult) -> str:
    """Return a single-line summary for one pipeline window."""
    icon = _icon(result)
    bar_width = 20
    filled = round(result.failure_rate * bar_width)
    bar = "\u2588" * filled + "\u2591" * (bar_width - filled)
    return (
        f"{icon}{result.pipeline_name:<30} "
        f"[{bar}] {result.failure_rate:>5.0%} "
        f"({result.failures}/{result.total} in {result.window_minutes}m)"
    )


def build_window_report(results: List[WindowResult]) -> str:
    """Build a multi-line report for all window results."""
    if not results:
        return "No window data available."
    breached = [r for r in results if r.breached]
    lines: List[str] = [
        f"Sliding-Window Failure Report  [{len(results)} pipeline(s)]",
        "=" * 64,
    ]
    for r in results:
        lines.append(format_window_result(r))
    lines.append("=" * 64)
    lines.append(
        f"Breached: {len(breached)}/{len(results)}  "
        f"(\'breach\' = \u2265 threshold within window)"
    )
    return "\n".join(lines)


def print_window_report(results: List[WindowResult]) -> None:  # pragma: no cover
    print(build_window_report(results))
