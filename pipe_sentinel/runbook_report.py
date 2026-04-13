"""Formatting helpers for runbook entries."""
from __future__ import annotations

from typing import List

from pipe_sentinel.runbook import RunbookEntry, RunbookIndex


def format_entry(entry: RunbookEntry) -> str:
    lines = [f"  Pipeline : {entry.pipeline}"]
    if entry.url:
        lines.append(f"  Runbook  : {entry.url}")
    else:
        lines.append("  Runbook  : (no link)")
    if entry.notes:
        lines.append(f"  Notes    : {entry.notes}")
    return "\n".join(lines)


def build_runbook_report(entries: List[RunbookEntry]) -> str:
    if not entries:
        return "No runbook entries found."
    sections = ["=== Runbook Links ==="]
    for entry in entries:
        sections.append(format_entry(entry))
        sections.append("")
    return "\n".join(sections).rstrip()


def build_full_index_report(index: RunbookIndex) -> str:
    return build_runbook_report(index.all_entries())


def print_runbook_report(entries: List[RunbookEntry]) -> None:
    print(build_runbook_report(entries))
