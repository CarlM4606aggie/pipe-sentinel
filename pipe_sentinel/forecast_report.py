"""Render forecast results as human-readable text."""
from __future__ import annotations

from typing import List

from pipe_sentinel.forecast import ForecastResult

_RISK_ICON = {"low": "✅", "medium": "⚠️", "high": "🔴"}


def format_forecast_result(result: ForecastResult) -> str:
    icon = _RISK_ICON.get(result.risk_level, "?")
    lines = [
        f"{icon} {result.pipeline_name}",
        f"   Risk       : {result.risk_level.upper()}",
        f"   Samples    : {result.sample_count}",
        f"   Recent rate: {result.recent_failure_rate:.0%}",
        f"   Baseline   : {result.baseline_failure_rate:.0%}",
        f"   Trend      : {result.trend:+.0%}",
    ]
    return "\n".join(lines)


def build_forecast_report(results: List[ForecastResult]) -> str:
    if not results:
        return "No forecast data available (insufficient samples)."

    high = [r for r in results if r.risk_level == "high"]
    medium = [r for r in results if r.risk_level == "medium"]
    low = [r for r in results if r.risk_level == "low"]

    header = (
        f"Failure Forecast Report  "
        f"[high={len(high)} medium={len(medium)} low={len(low)}]"
    )
    separator = "=" * len(header)

    sections: List[str] = [separator, header, separator]
    for result in results:
        sections.append(format_forecast_result(result))

    return "\n".join(sections)


def print_forecast_report(results: List[ForecastResult]) -> None:
    print(build_forecast_report(results))
