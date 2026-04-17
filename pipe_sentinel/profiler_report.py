"""Formatting helpers for profiler output."""
from __future__ import annotations

from typing import List

from pipe_sentinel.profiler import ProfileStats

_SLOW_ICON = "🐢"
_OK_ICON = "✅"
_DEFAULT_THRESHOLD = 60.0  # seconds


def _icon(stats: ProfileStats, threshold: float) -> str:
    return _SLOW_ICON if stats.is_slow(threshold) else _OK_ICON


def format_profile_stats(
    stats: ProfileStats, threshold: float = _DEFAULT_THRESHOLD
) -> str:
    icon = _icon(stats, threshold)
    return (
        f"{icon} {stats.pipeline}\n"
        f"   samples : {stats.sample_count}\n"
        f"   mean    : {stats.mean_seconds:.2f}s\n"
        f"   p95     : {stats.p95_seconds:.2f}s\n"
        f"   min/max : {stats.min_seconds:.2f}s / {stats.max_seconds:.2f}s"
    )


def build_profiler_report(
    profiles: List[ProfileStats],
    threshold: float = _DEFAULT_THRESHOLD,
) -> str:
    if not profiles:
        return "No profiling data available."
    lines = [f"Pipeline Profiler Report (slow threshold: {threshold:.0f}s)", "="*50]
    for p in profiles:
        lines.append(format_profile_stats(p, threshold))
    slow = [p for p in profiles if p.is_slow(threshold)]
    lines.append("="*50)
    lines.append(f"Total pipelines: {len(profiles)}  Slow (p95): {len(slow)}")
    return "\n".join(lines)


def print_profiler_report(
    profiles: List[ProfileStats],
    threshold: float = _DEFAULT_THRESHOLD,
) -> None:
    print(build_profiler_report(profiles, threshold))
