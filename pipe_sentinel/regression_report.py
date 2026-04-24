"""Formatting helpers for regression detection results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.regression import RegressionResult


def _icon(result: RegressionResult) -> str:
    return "🔴" if result.is_regression else "🟢"


def format_regression_result(result: RegressionResult) -> str:
    lines = [
        f"{_icon(result)} {result.pipeline}",
        f"   Baseline failure rate : {result.baseline_rate:.1%}",
        f"   Current  failure rate : {result.current_rate:.1%}",
        f"   Delta                 : {result.delta:+.1%}",
    ]
    if result.is_regression:
        lines.append("   ⚠  Regression detected")
    return "\n".join(lines)


def build_regression_report(results: List[RegressionResult]) -> str:
    if not results:
        return "No regressions detected."
    header = f"Regression Report — {len(results)} pipeline(s) regressed"
    separator = "─" * len(header)
    sections = [header, separator]
    for r in results:
        sections.append(format_regression_result(r))
    return "\n".join(sections)


def print_regression_report(results: List[RegressionResult]) -> None:
    print(build_regression_report(results))
