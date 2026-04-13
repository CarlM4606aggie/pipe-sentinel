"""Formatting helpers for retention pruning results."""
from __future__ import annotations

from pipe_sentinel.retention import PruneResult


def format_prune_result(result: PruneResult) -> str:
    """Return a human-readable single-line summary of a prune operation."""
    ts = result.cutoff_ts.strftime("%Y-%m-%d")
    noun = "record" if result.rows_deleted == 1 else "records"
    return (
        f"Retention sweep complete — "
        f"{result.rows_deleted} {noun} removed "
        f"(cutoff: {ts})"
    )


def format_dry_run(result: PruneResult) -> str:
    """Return a dry-run preview message (nothing was actually deleted)."""
    ts = result.cutoff_ts.strftime("%Y-%m-%d")
    noun = "record" if result.rows_deleted == 1 else "records"
    return (
        f"[DRY RUN] Would remove {result.rows_deleted} {noun} "
        f"older than {ts}"
    )


def print_retention_report(result: PruneResult, dry_run: bool = False) -> None:
    """Print the retention report to stdout."""
    if dry_run:
        print(format_dry_run(result))
    else:
        print(format_prune_result(result))
