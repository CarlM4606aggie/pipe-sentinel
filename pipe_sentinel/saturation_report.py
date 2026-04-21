"""Formatting helpers for saturation detection results."""

from __future__ import annotations

from typing import List

from pipe_sentinel.saturation import SaturationResult


def _icon(result: SaturationResult) -> str:
    if result.saturated:
        return "\u2716"  # ✖
    return "\u2714"  # ✔


def format_saturation_result(result: SaturationResult) -> str:
    icon = _icon(result)
    bar_width = 20
    filled = round(result.failure_rate * bar_width)
    bar = "#" * filled + "-" * (bar_width - filled)
    lines = [
        f"{icon} {result.pipeline_name}",
        f"   failures : {result.failures}/{result.total}",
        f"   rate     : {result.failure_rate:.0%}  [{bar}]  threshold={result.threshold:.0%}",
        f"   window   : {result.window_hours}h",
    ]
    return "\n".join(lines)


def build_saturation_report(results: List[SaturationResult]) -> str:
    if not results:
        return "No saturation data available."
    saturated = [r for r in results if r.saturated]
    header_lines = [
        f"Saturation Report  ({len(results)} pipeline(s) checked)",
        f"Saturated: {len(saturated)}/{len(results)}",
        "=" * 50,
    ]
    body = "\n\n".join(format_saturation_result(r) for r in results)
    return "\n".join(header_lines) + "\n" + body


def print_saturation_report(results: List[SaturationResult]) -> None:
    print(build_saturation_report(results))
