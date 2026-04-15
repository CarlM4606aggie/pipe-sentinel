"""Formatting helpers for SLA check results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.sla import SLAResult


def _icon(result: SLAResult) -> str:
    if result.breached:
        return "✗"
    if result.warned:
        return "!"
    return "✓"


def format_sla_result(result: SLAResult) -> str:
    icon = _icon(result)
    pct = (result.duration / result.max_duration * 100) if result.max_duration else 0.0
    label = "BREACHED" if result.breached else ("WARNING" if result.warned else "OK")
    return (
        f"  {icon} {result.pipeline_name:<30} "
        f"{result.duration:>7.1f}s / {result.max_duration:.1f}s "
        f"({pct:.0f}%)  [{label}]"
    )


def build_sla_report(results: List[SLAResult]) -> str:
    if not results:
        return "No SLA data available.\n"

    breached = [r for r in results if r.breached]
    warned = [r for r in results if r.warned]
    ok = [r for r in results if not r.breached and not r.warned]

    lines: List[str] = []
    lines.append(f"SLA Report  ({len(results)} pipelines checked)")
    lines.append("=" * 60)
    lines.append(
        f"  OK: {len(ok)}   Warnings: {len(warned)}   Breached: {len(breached)}"
    )
    lines.append("")

    for section, label in [(breached, "BREACHED"), (warned, "WARNING"), (ok, "OK")]:
        if section:
            lines.append(f"--- {label} ---")
            for r in section:
                lines.append(format_sla_result(r))
            lines.append("")

    return "\n".join(lines)


def print_sla_report(results: List[SLAResult]) -> None:
    print(build_sla_report(results))
