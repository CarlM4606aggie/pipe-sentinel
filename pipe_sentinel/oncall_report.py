"""Formatting helpers for on-call rotation output."""
from __future__ import annotations

from typing import List

from pipe_sentinel.oncall import OnCallEntry, OnCallRotation


def format_entry(entry: OnCallEntry) -> str:
    scope = ", ".join(entry.pipelines) if entry.pipelines else "(all pipelines)"
    return f"  {entry.name} <{entry.email}>  —  {scope}"


def build_oncall_report(rotation: OnCallRotation, pipeline_name: str | None = None) -> str:
    lines: List[str] = []
    if pipeline_name:
        entries = rotation.owners_for(pipeline_name)
        lines.append(f"On-call owners for '{pipeline_name}':")
        if not entries:
            lines.append("  (none configured)")
        else:
            lines.extend(format_entry(e) for e in entries)
    else:
        lines.append("On-call rotation:")
        if not rotation.entries:
            lines.append("  (no entries configured)")
        else:
            lines.extend(format_entry(e) for e in rotation.entries)
    return "\n".join(lines)


def print_oncall_report(rotation: OnCallRotation, pipeline_name: str | None = None) -> None:
    print(build_oncall_report(rotation, pipeline_name))
