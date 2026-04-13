"""Formatting helpers for the pause/resume report."""
from __future__ import annotations

import time
from typing import List

from pipe_sentinel.pause import PauseEntry, PauseStore


def _format_ts(epoch: float) -> str:
    import datetime
    return datetime.datetime.utcfromtimestamp(epoch).strftime("%Y-%m-%d %H:%M UTC")


def _format_duration(seconds: float) -> str:
    """Return a human-readable string for a duration given in seconds.

    Examples: '2h 15m', '45m 30s', '10s'.
    """
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs or not parts:
        parts.append(f"{secs}s")
    return " ".join(parts)


def format_entry(entry: PauseEntry, now: float | None = None) -> str:
    ts = now if now is not None else time.time()
    active = entry.is_active(ts)
    status = "PAUSED" if active else "expired"
    lines = [f"  [{status}] {entry.pipeline_name}"]
    lines.append(f"    paused_at : {_format_ts(entry.paused_at)}")
    if entry.reason:
        lines.append(f"    reason    : {entry.reason}")
    if entry.resume_at is not None:
        lines.append(f"    resume_at : {_format_ts(entry.resume_at)}")
        if active:
            remaining = entry.resume_at - ts
            lines.append(f"    remaining : {_format_duration(remaining)}")
    else:
        lines.append("    resume_at : indefinite")
    return "\n".join(lines)


def build_pause_report(entries: List[PauseEntry], now: float | None = None) -> str:
    ts = now if now is not None else time.time()
    active = [e for e in entries if e.is_active(ts)]
    expired = [e for e in entries if not e.is_active(ts)]

    sections: List[str] = ["=== Pipeline Pause Report ==="]
    sections.append(f"Active pauses : {len(active)}")
    sections.append(f"Expired pauses: {len(expired)}")

    if active:
        sections.append("\nCurrently paused:")
        for e in active:
            sections.append(format_entry(e, ts))

    if expired:
        sections.append("\nExpired pauses:")
        for e in expired:
            sections.append(format_entry(e, ts))

    if not entries:
        sections.append("\nNo pause entries recorded.")

    return "\n".join(sections)


def print_pause_report(store: PauseStore, now: float | None = None) -> None:
    store.load()
    print(build_pause_report(store.all_entries(), now))
