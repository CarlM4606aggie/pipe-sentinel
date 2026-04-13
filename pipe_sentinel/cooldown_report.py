"""Formatting helpers for cooldown status reports."""

from __future__ import annotations

from typing import List, Optional
import time

from pipe_sentinel.cooldown import CooldownEntry, CooldownStore


def _format_remaining(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}m {secs}s"


def format_entry(entry: CooldownEntry, now: Optional[float] = None) -> str:
    now = now if now is not None else time.time()
    if entry.is_cooling(now=now):
        remaining = _format_remaining(entry.remaining_seconds(now=now))
        status = f"COOLING  ({remaining} remaining)"
    else:
        status = "READY"
    return f"  {entry.pipeline_name:<30} {status}"


def build_cooldown_report(entries: List[CooldownEntry], now: Optional[float] = None) -> str:
    now = now if now is not None else time.time()
    if not entries:
        return "No cooldown entries recorded."
    lines = ["Pipeline Cooldown Status", "=" * 50]
    cooling = [e for e in entries if e.is_cooling(now=now)]
    ready = [e for e in entries if not e.is_cooling(now=now)]
    for entry in sorted(cooling, key=lambda e: e.remaining_seconds(now=now), reverse=True):
        lines.append(format_entry(entry, now=now))
    for entry in sorted(ready, key=lambda e: e.pipeline_name):
        lines.append(format_entry(entry, now=now))
    lines.append("=" * 50)
    lines.append(f"Cooling: {len(cooling)}  Ready: {len(ready)}")
    return "\n".join(lines)


def print_cooldown_report(store: CooldownStore, now: Optional[float] = None) -> None:
    print(build_cooldown_report(store.all_entries(), now=now))
