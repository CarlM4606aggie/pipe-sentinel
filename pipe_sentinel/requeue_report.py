"""Formatting helpers for requeue store reports."""
from __future__ import annotations

import time
from typing import List

from pipe_sentinel.requeue import RequeueEntry, RequeueStore


def _fmt_delay(entry: RequeueEntry, now: float) -> str:
    remaining = entry.run_after - now
    if remaining <= 0:
        return "ready"
    if remaining < 60:
        return f"{remaining:.0f}s"
    return f"{remaining / 60:.1f}m"


def format_entry(entry: RequeueEntry, now: float | None = None) -> str:
    now = now or time.time()
    delay_label = _fmt_delay(entry, now)
    reason = f" ({entry.reason})" if entry.reason else ""
    return f"  [{delay_label:>6}] {entry.pipeline_name}{reason}  attempts={entry.attempts}"


def build_requeue_report(store: RequeueStore, now: float | None = None) -> str:
    now = now or time.time()
    entries = store.all_entries()
    lines: List[str] = [f"Requeue Queue  ({len(entries)} entries)"]
    if not entries:
        lines.append("  (empty)")
        return "\n".join(lines)
    ready = [e for e in entries if e.is_ready(now)]
    pending = [e for e in entries if not e.is_ready(now)]
    if ready:
        lines.append("  Ready:")
        for e in ready:
            lines.append(format_entry(e, now))
    if pending:
        lines.append("  Pending:")
        for e in pending:
            lines.append(format_entry(e, now))
    return "\n".join(lines)


def print_requeue_report(store: RequeueStore) -> None:
    print(build_requeue_report(store))
