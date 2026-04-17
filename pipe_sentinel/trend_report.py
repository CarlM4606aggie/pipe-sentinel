"""Formatting helpers for trend detection results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.trend import TrendResult


def _icon(result: TrendResult) -> str:
    return "⚠" if result.worsening else "✓"


def format_trend_result(result: TrendResult) -> str:
    icon = _icon(result)
    return (
        f"{icon} {result.pipeline}\n"
        f"   recent={result.recent_rate:.1%}  "
        f"baseline={result.baseline_rate:.1%}  "
        f"delta={result.delta:+.1%}  "
        f"samples={result.sample_count}"
    )


def build_trend_report(results: List[TrendResult]) -> str:
    if not results:
        return "No trend data available (insufficient samples)."

    worsening = [r for r in results if r.worsening]
    stable = [r for r in results if not r.worsening]

    lines = [f"Trend Report  ({len(results)} pipelines)"]
    lines.append("=" * 40)

    if worsening:
        lines.append(f"Worsening ({len(worsening)}):")
        for r in worsening:
            lines.append(format_trend_result(r))

    if stable:
        lines.append(f"Stable/Improving ({len(stable)}):")
        for r in stable:
            lines.append(format_trend_result(r))

    return "\n".join(lines)


def print_trend_report(results: List[TrendResult]) -> None:
    print(build_trend_report(results))
