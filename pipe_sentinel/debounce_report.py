"""Formatting helpers for debounce state reports."""
from __future__ import annotations

import time
from typing import List

from pipe_sentinel.debounce import DebounceEntry, DebounceStore


def _remaining_label(entry: DebounceEntry, now: float) -> str:
    remaining = entry.window_seconds - (now - entry.last_alert_at)
    if remaining <= 0:
        return "ready"
    minutes, seconds = divmod(int(remaining), 60)
    if minutes:
        return f"{minutes}m {seconds}s remaining"
    return f"{seconds}s remaining"


def format_entry(entry: DebounceEntry, now: float | None = None) -> str:
    t = now if now is not None else time.time()
    debounced = entry.is_debounced(t)
    icon = "🔇" if debounced else "🔔"
    label = _remaining_label(entry, t)
    return f"  {icon} {entry.pipeline:<30} window={entry.window_seconds:.0f}s  {label}"


def build_debounce_report(
    store: DebounceStore,
    now: float | None = None,
) -> str:
    t = now if now is not None else time.time()
    entries: List[DebounceEntry] = list(store._entries.values())
    if not entries:
        return "Debounce report: no entries recorded."

    lines = [f"Debounce report ({len(entries)} pipeline(s)):", ""]
    for entry in sorted(entries, key=lambda e: e.pipeline):
        lines.append(format_entry(entry, t))
    return "\n".join(lines)


def print_debounce_report(store: DebounceStore, now: float | None = None) -> None:
    print(build_debounce_report(store, now))
