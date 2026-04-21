"""Formatting helpers for CascadeReport."""
from __future__ import annotations

from pipe_sentinel.cascade import CascadeReport, CascadeResult


def _icon(result: CascadeResult) -> str:
    if result.is_cascade:
        return "\u26a1"
    if result.failed:
        return "\u2716"
    return "\u2714"


def format_cascade_result(result: CascadeResult) -> str:
    icon = _icon(result)
    if result.is_cascade:
        ups = ", ".join(result.upstream_failures)
        return f"{icon}  {result.pipeline}  [cascade from: {ups}]"
    if result.failed:
        return f"{icon}  {result.pipeline}  [isolated failure]"
    return f"{icon}  {result.pipeline}"


def build_cascade_report(report: CascadeReport) -> str:
    lines: list[str] = []
    total = len(report.results)
    n_cascade = len(report.cascades)
    n_isolated = len(report.isolated_failures)

    lines.append("=== Cascade Failure Report ===")
    lines.append(
        f"Pipelines: {total} | Cascades: {n_cascade} | Isolated failures: {n_isolated}"
    )
    lines.append("")

    if report.cascades:
        lines.append("Cascade failures:")
        for r in report.cascades:
            lines.append(f"  {format_cascade_result(r)}")
        lines.append("")

    if report.isolated_failures:
        lines.append("Isolated failures:")
        for r in report.isolated_failures:
            lines.append(f"  {format_cascade_result(r)}")
        lines.append("")

    passing = [r for r in report.results if not r.failed]
    if passing:
        lines.append("Passing:")
        for r in passing:
            lines.append(f"  {format_cascade_result(r)}")

    return "\n".join(lines)


def print_cascade_report(report: CascadeReport) -> None:
    print(build_cascade_report(report))
