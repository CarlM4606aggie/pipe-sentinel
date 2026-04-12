"""Generate a human-readable audit summary report from stored run history."""

from pathlib import Path
from typing import List

from pipe_sentinel.audit import AuditRecord, fetch_recent, DEFAULT_DB_PATH


def _status_symbol(success: bool) -> str:
    return "✓" if success else "✗"


def format_record(record: AuditRecord) -> str:
    status = _status_symbol(record.success)
    return (
        f"  [{status}] {record.recorded_at}  "
        f"exit={record.exit_code}  attempts={record.attempts}  "
        f"duration={record.duration_seconds:.2f}s"
    )


def build_report(pipeline_names: List[str], limit: int = 5, db_path: Path = DEFAULT_DB_PATH) -> str:
    """Build a multi-pipeline audit summary string."""
    lines: List[str] = ["=== Pipe-Sentinel Audit Report ===", ""]
    for name in pipeline_names:
        records = fetch_recent(name, limit=limit, db_path=db_path)
        total = len(records)
        passed = sum(1 for r in records if r.success)
        lines.append(f"Pipeline: {name}  (last {total} runs: {passed} passed, {total - passed} failed)")
        if records:
            for rec in records:
                lines.append(format_record(rec))
        else:
            lines.append("  No runs recorded.")
        lines.append("")
    return "\n".join(lines)


def print_report(pipeline_names: List[str], limit: int = 5, db_path: Path = DEFAULT_DB_PATH) -> None:
    """Print the audit report to stdout."""
    print(build_report(pipeline_names, limit=limit, db_path=db_path))
