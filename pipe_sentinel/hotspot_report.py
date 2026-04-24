"""Formatting utilities for hotspot detection results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.hotspot import HotspotResult


def _icon(result: HotspotResult) -> str:
    if result.failure_rate >= 0.75:
        return "\U0001f525"  # fire
    if result.failure_rate >= 0.40:
        return "\u26a0\ufe0f "  # warning
    return "\U0001f7e1"  # yellow circle


def format_hotspot_result(result: HotspotResult) -> str:
    icon = _icon(result)
    pct = result.failure_rate * 100
    return (
        f"{icon} {result.pipeline}: "
        f"{result.failures}/{result.total_runs} failures ({pct:.1f}%)"
    )


def build_hotspot_report(results: List[HotspotResult]) -> str:
    if not results:
        return "No hotspots detected."
    lines = [f"Hotspot Report ({len(results)} pipeline(s)):", ""]
    for r in results:
        lines.append(f"  {format_hotspot_result(r)}")
    return "\n".join(lines)


def print_hotspot_report(results: List[HotspotResult]) -> None:  # pragma: no cover
    print(build_hotspot_report(results))
