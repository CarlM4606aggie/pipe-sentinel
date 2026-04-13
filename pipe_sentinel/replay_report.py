"""Formatting helpers for ReplayReport."""
from __future__ import annotations

from pipe_sentinel.replay import ReplayReport


def _symbol(success: bool) -> str:
    return "\u2713" if success else "\u2717"


def format_replay_report(report: ReplayReport, dry_run: bool = False) -> str:
    lines: list[str] = []

    if dry_run:
        lines.append("[dry-run] Replay skipped — pipelines that would re-run:")
        for name in report.skipped:
            lines.append(f"  - {name}")
        if not report.skipped:
            lines.append("  (none)")
        return "\n".join(lines)

    lines.append("=== Replay Report ===")
    lines.append(
        f"Total: {report.total}  "
        f"Succeeded: {report.succeeded}  "
        f"Failed: {report.failed}  "
        f"Skipped: {len(report.skipped)}"
    )

    if report.replayed:
        lines.append("\nReplayed:")
        for r in report.replayed:
            sym = _symbol(r.success)
            dur = f"{r.duration:.2f}s" if r.duration is not None else "n/a"
            lines.append(f"  [{sym}] {r.pipeline_name}  ({dur})")
            if not r.success and r.stderr:
                snippet = r.stderr.strip().splitlines()[-1]
                lines.append(f"       {snippet}")

    if report.skipped:
        lines.append("\nSkipped (no config found):")
        for name in report.skipped:
            lines.append(f"  - {name}")

    return "\n".join(lines)


def print_replay_report(report: ReplayReport, dry_run: bool = False) -> None:
    print(format_replay_report(report, dry_run=dry_run))
