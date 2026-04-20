"""Formatting helpers for lockout state reports."""
from __future__ import annotations

from typing import List

from pipe_sentinel.lockout import LockoutEntry, LockoutStore


def _format_remaining(seconds: float) -> str:
    if seconds <= 0:
        return "expired"
    minutes, secs = divmod(int(seconds), 60)
    if minutes:
        return f"{minutes}m {secs}s remaining"
    return f"{secs}s remaining"


def format_entry(entry: LockoutEntry, now: float | None = None) -> str:
    locked = entry.is_locked(now)
    status = "LOCKED" if locked else "RELEASED"
    remaining = _format_remaining(entry.remaining_seconds(now)) if locked else "—"
    return (
        f"  [{status}] {entry.pipeline}\n"
        f"    reason   : {entry.reason}\n"
        f"    duration : {entry.duration_seconds:.0f}s\n"
        f"    remaining: {remaining}"
    )


def build_lockout_report(store: LockoutStore, now: float | None = None) -> str:
    entries = store.all_entries()
    lines: List[str] = [f"Lockout Report  ({len(entries)} entr{'y' if len(entries) == 1 else 'ies'})"]
    lines.append("-" * 40)
    if not entries:
        lines.append("  No lockout entries.")
    else:
        for entry in sorted(entries, key=lambda e: e.pipeline):
            lines.append(format_entry(entry, now))
    return "\n".join(lines)


def print_lockout_report(store: LockoutStore, now: float | None = None) -> None:
    print(build_lockout_report(store, now))
