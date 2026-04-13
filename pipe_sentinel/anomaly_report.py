"""Render anomaly detection results as human-readable text."""
from __future__ import annotations

from typing import List

from pipe_sentinel.anomaly import AnomalyResult


def format_anomaly_result(result: AnomalyResult) -> str:
    return str(result)


def build_anomaly_report(results: List[AnomalyResult]) -> str:
    if not results:
        return "No anomalies detected."

    flagged = [r for r in results if r.is_anomaly]
    clean = [r for r in results if not r.is_anomaly]

    lines: List[str] = []
    lines.append("=== Anomaly Detection Report ===")
    lines.append(f"Pipelines checked : {len(results)}")
    lines.append(f"Anomalies flagged : {len(flagged)}")
    lines.append("")

    if flagged:
        lines.append("--- Anomalies ---")
        for r in flagged:
            lines.append(f"  {r}")
        lines.append("")

    if clean:
        lines.append("--- Healthy ---")
        for r in clean:
            lines.append(f"  {r}")

    return "\n".join(lines)


def print_anomaly_report(results: List[AnomalyResult]) -> None:
    print(build_anomaly_report(results))
