"""Formatting helpers for BudgetResult."""
from __future__ import annotations

from pipe_sentinel.budget import BudgetResult

_BAR_WIDTH = 30


def _bar(pct: float, width: int = _BAR_WIDTH) -> str:
    filled = min(int(round(pct / 100 * width)), width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def format_budget_result(result: BudgetResult) -> str:
    lines = []
    if result.exceeded:
        icon = "✗"
    elif result.warned:
        icon = "⚠"
    else:
        icon = "✓"

    lines.append(
        f"{icon} Runtime Budget: {result.total_seconds:.1f}s "
        f"/ {result.config.max_total_seconds:.1f}s "
        f"({result.utilisation_pct:.1f}%)"
    )
    lines.append(f"  {_bar(result.utilisation_pct)}")
    lines.append(f"  Pipelines run : {result.pipeline_count}")
    lines.append(f"  Remaining     : {result.remaining_seconds:.1f}s")

    if result.contributions:
        lines.append("  Breakdown:")
        for name, dur in sorted(result.contributions, key=lambda x: -x[1]):
            lines.append(f"    {name:<30} {dur:>8.2f}s")

    return "\n".join(lines)


def build_budget_report(result: BudgetResult) -> str:
    header = "=" * 50
    return "\n".join([header, "Runtime Budget Report", header, format_budget_result(result)])


def print_budget_report(result: BudgetResult) -> None:
    print(build_budget_report(result))
