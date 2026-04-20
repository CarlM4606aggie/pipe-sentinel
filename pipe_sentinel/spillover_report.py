"""Formatting helpers for spillover detection results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.spillover import SpilloverResult


def _icon(result: SpilloverResult) -> str:
    return "⚠" if result.is_spilling else "✓"


def format_spillover_result(result: SpilloverResult) -> str:
    lines = [
        f"{_icon(result)} {result.pipeline_name}",
        f"   scheduled : {result.scheduled_duration:.1f}s",
        f"   actual    : {result.actual_duration:.1f}s",
        f"   spillover : {result.spillover_seconds:+.1f}s",
        f"   samples   : {result.sample_count}",
    ]
    return "\n".join(lines)


def build_spillover_report(results: List[SpilloverResult]) -> str:
    if not results:
        return "Spillover Report\n  No pipelines with scheduled_duration configured."

    spilling = [r for r in results if r.is_spilling]
    ok = [r for r in results if not r.is_spilling]

    sections: List[str] = []
    sections.append(
        f"Spillover Report  [{len(spilling)} spilling / {len(results)} checked]"
    )
    sections.append("-" * 50)

    if spilling:
        sections.append("Spilling pipelines:")
        for r in sorted(spilling, key=lambda x: x.spillover_seconds, reverse=True):
            sections.append(format_spillover_result(r))

    if ok:
        sections.append("Within schedule:")
        for r in sorted(ok, key=lambda x: x.pipeline_name):
            sections.append(f"  ✓ {r.pipeline_name} ({r.spillover_seconds:+.1f}s)")

    return "\n".join(sections)


def print_spillover_report(results: List[SpilloverResult]) -> None:
    print(build_spillover_report(results))
