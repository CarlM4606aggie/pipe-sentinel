"""Formatting helpers for MetricsReport output."""
from __future__ import annotations

from pipe_sentinel.metrics import MetricsReport, TrendPoint

_BAR_WIDTH = 20


def _bar(rate: float, width: int = _BAR_WIDTH) -> str:
    filled = round(rate * width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def format_trend_point(point: TrendPoint) -> str:
    status = "DEGRADING" if point.is_degrading else "OK"
    bar = _bar(point.failure_rate)
    return (
        f"  {point.pipeline_name:<30} {status:<10} "
        f"fail={point.failure_rate:.0%} {bar} "
        f"avg={point.avg_duration_s:.2f}s  (n={point.window})"
    )


def format_metrics_report(report: MetricsReport) -> str:
    if not report.points:
        return "No metrics data available."

    lines = ["=== Pipeline Metrics Report ==="]
    lines.append(
        f"  Pipelines: {len(report.points)}  "
        f"Degrading: {len(report.degrading)}  "
        f"Healthy: {len(report.healthy)}"
    )
    lines.append("")

    if report.degrading:
        lines.append("--- Degrading ---")
        for p in report.degrading:
            lines.append(format_trend_point(p))
        lines.append("")

    lines.append("--- All Pipelines ---")
    for p in sorted(report.points, key=lambda x: x.pipeline_name):
        lines.append(format_trend_point(p))

    return "\n".join(lines)


def print_metrics_report(report: MetricsReport) -> None:
    print(format_metrics_report(report))
