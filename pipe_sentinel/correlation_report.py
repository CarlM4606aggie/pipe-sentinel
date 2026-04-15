"""Formatting helpers for CorrelationReport."""
from __future__ import annotations

from pipe_sentinel.correlation import CorrelationPair, CorrelationReport

_ICON_HIGH = "🔴"
_ICON_MED = "🟡"
_ICON_LOW = "🟢"


def _icon(rate: float, threshold: float) -> str:
    if rate >= threshold:
        return _ICON_HIGH
    if rate >= threshold * 0.5:
        return _ICON_MED
    return _ICON_LOW


def format_pair(pair: CorrelationPair, threshold: float = 0.5) -> str:
    icon = _icon(pair.rate, threshold)
    pct = pair.rate * 100
    return (
        f"{icon} {pair.pipeline_a} <-> {pair.pipeline_b}  "
        f"co-failures: {pair.co_failures}/{pair.total_windows}  "
        f"rate: {pct:.1f}%"
    )


def build_correlation_report(report: CorrelationReport) -> str:
    lines = [
        f"Correlation Report  (threshold={report.threshold * 100:.0f}%,  "
        f"pairs={len(report.pairs)},  significant={len(report.significant)})",
        "-" * 60,
    ]
    if not report.pairs:
        lines.append("  No co-failure data found.")
        return "\n".join(lines)

    for pair in report.pairs:
        lines.append("  " + format_pair(pair, report.threshold))

    sig = report.significant
    lines.append("-" * 60)
    if sig:
        names = ", ".join(f"{p.pipeline_a}/{p.pipeline_b}" for p in sig)
        lines.append(f"  ⚠  Significant pairs: {names}")
    else:
        lines.append("  ✓  No significant co-failure correlations detected.")
    return "\n".join(lines)


def print_correlation_report(report: CorrelationReport) -> None:
    print(build_correlation_report(report))
