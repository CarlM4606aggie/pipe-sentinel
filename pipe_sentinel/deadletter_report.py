"""Formatting helpers for dead-letter queue entries."""
from __future__ import annotations

from typing import List

from pipe_sentinel.deadletter import DeadLetterEntry, DeadLetterStore

_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _fmt_ts(ts: float) -> str:
    import datetime
    return datetime.datetime.utcfromtimestamp(ts).strftime(_DATE_FMT)


def format_entry(entry: DeadLetterEntry) -> str:
    lines = [
        f"  ID       : {entry.entry_id}",
        f"  Pipeline : {entry.pipeline_name}",
        f"  Command  : {entry.command}",
        f"  Failed   : {_fmt_ts(entry.failed_at)} UTC",
        f"  Exit code: {entry.returncode}",
        f"  Attempts : {entry.attempts}",
    ]
    if entry.stderr:
        truncated = entry.stderr[:120].replace("\n", " ")
        lines.append(f"  Stderr   : {truncated}")
    return "\n".join(lines)


def build_deadletter_report(entries: List[DeadLetterEntry]) -> str:
    if not entries:
        return "Dead-letter queue is empty."
    header = f"Dead-letter queue — {len(entries)} entr{'y' if len(entries) == 1 else 'ies'}"
    separator = "-" * len(header)
    blocks = [f"{header}\n{separator}"]
    for entry in entries:
        blocks.append(format_entry(entry))
        blocks.append("")
    return "\n".join(blocks).rstrip()


def print_deadletter_report(store: DeadLetterStore) -> None:
    print(build_deadletter_report(store.all_entries()))
