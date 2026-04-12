"""Audit report formatting utilities for pipe-sentinel."""

from dataclasses import dataclass
from typing import List
from pipe_sentinel.audit import AuditRecord


def _status_symbol(status: str) -> str:
    """Return a visual symbol for a given status string."""
    return "✓" if status == "success" else "✗"


def format_record(record: AuditRecord) -> str:
    """Format a single AuditRecord into a human-readable string."""
    symbol = _status_symbol(record.status)
    duration = f"{record.duration:.2f}s" if record.duration is not None else "N/A"
    error_part = f" | error: {record.error}" if record.error else ""
    return (
        f"[{symbol}] {record.pipeline_name:<24} "
        f"ran_at={record.ran_at}  "
        f"duration={duration}  "
        f"retries={record.retries}"
        f"{error_part}"
    )


def build_report(records: List[AuditRecord]) -> str:
    """Build a full report string from a list of AuditRecords."""
    if not records:
        return "No audit records found."

    lines = ["=== Pipe Sentinel Audit Report ===", ""]
    for record in records:
        lines.append(format_record(record))
    lines.append("")
    total = len(records)
    failures = sum(1 for r in records if r.status != "success")
    lines.append(f"Total: {total}  Passed: {total - failures}  Failed: {failures}")
    return "\n".join(lines)


def print_report(records: List[AuditRecord]) -> None:
    """Print the audit report to stdout."""
    print(build_report(records))
