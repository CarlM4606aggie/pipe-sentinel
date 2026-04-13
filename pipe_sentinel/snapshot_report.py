"""Format and print snapshot diff reports."""
from __future__ import annotations

from typing import List

from pipe_sentinel.snapshot import SnapshotDiff


def _status_icon(status: str) -> str:
    icons = {"success": "✔", "failure": "✘", "unknown": "?"}
    return icons.get(status, "?")


def format_diff(diff: SnapshotDiff) -> str:
    cur = diff.current
    icon = _status_icon(cur.last_status)
    lines = [f"  {icon} {diff.name}  [{cur.last_status}]"]

    if diff.recovered():
        lines.append("    ↑ RECOVERED (was failing)")
    elif diff.newly_failing():
        lines.append("    ↓ NEWLY FAILING")
    elif diff.status_changed() and diff.previous:
        prev_icon = _status_icon(diff.previous.last_status)
        lines.append(f"    ~ changed: {prev_icon} {diff.previous.last_status} → {icon} {cur.last_status}")

    if cur.consecutive_failures > 0:
        lines.append(f"    consecutive failures: {cur.consecutive_failures}")
    if cur.last_run_ts:
        lines.append(f"    last run: {cur.last_run_ts}")
    return "\n".join(lines)


def build_snapshot_report(diffs: List[SnapshotDiff]) -> str:
    if not diffs:
        return "No pipeline snapshots available."

    recovered = [d for d in diffs if d.recovered()]
    newly_failing = [d for d in diffs if d.newly_failing()]
    unchanged = [d for d in diffs if not d.status_changed()]

    sections: List[str] = ["=== Pipeline Snapshot Report ==="]

    if newly_failing:
        sections.append("\n--- Newly Failing ---")
        sections.extend(format_diff(d) for d in newly_failing)

    if recovered:
        sections.append("\n--- Recovered ---")
        sections.extend(format_diff(d) for d in recovered)

    if unchanged:
        sections.append("\n--- Unchanged ---")
        sections.extend(format_diff(d) for d in unchanged)

    total = len(diffs)
    failing_count = sum(1 for d in diffs if d.current.last_status == "failure")
    sections.append(f"\nTotal: {total}  |  Failing: {failing_count}  |  Passing: {total - failing_count}")
    return "\n".join(sections)


def print_snapshot_report(diffs: List[SnapshotDiff]) -> None:
    print(build_snapshot_report(diffs))
