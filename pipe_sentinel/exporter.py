"""Export audit records to CSV or JSON for external analysis."""
from __future__ import annotations

import csv
import json
import io
from dataclasses import asdict
from typing import List, Literal

from pipe_sentinel.audit import AuditRecord, fetch_recent

ExportFormat = Literal["csv", "json"]


def _records_to_dicts(records: List[AuditRecord]) -> List[dict]:
    """Convert AuditRecord dataclasses to plain dicts."""
    return [asdict(r) for r in records]


def export_csv(records: List[AuditRecord]) -> str:
    """Serialise records to a CSV string."""
    if not records:
        return ""
    dicts = _records_to_dicts(records)
    fieldnames = list(dicts[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(dicts)
    return buf.getvalue()


def export_json(records: List[AuditRecord]) -> str:
    """Serialise records to a JSON string."""
    return json.dumps(_records_to_dicts(records), indent=2, default=str)


def export_records(
    db_path: str,
    fmt: ExportFormat = "json",
    limit: int = 100,
    pipeline_name: str | None = None,
) -> str:
    """Fetch recent audit records and export them in the requested format.

    Args:
        db_path: Path to the SQLite audit database.
        fmt: Output format – ``"csv"`` or ``"json"``.
        limit: Maximum number of records to retrieve.
        pipeline_name: Optional filter; only records for this pipeline.

    Returns:
        Serialised string ready to write to a file or stdout.
    """
    records = fetch_recent(db_path, limit=limit)
    if pipeline_name:
        records = [r for r in records if r.pipeline_name == pipeline_name]
    if fmt == "csv":
        return export_csv(records)
    return export_json(records)
