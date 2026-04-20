"""Formatting helpers for recovery results."""
from __future__ import annotations

from typing import List

from pipe_sentinel.recovery import RecoveryResult


def _icon(result: RecoveryResult) -> str:
    return "✅" if result.recovered else "➖"


def format_recovery_result(result: RecoveryResult) -> str:
    icon = _icon(result)
    if result.recovered:
        return (
            f"{icon} {result.pipeline_name}: recovered after "
            f"{result.previous_failures} failure(s)  (at {result.recovered_at})"
        )
    return f"{icon} {result.pipeline_name}: no recovery detected"


def build_recovery_report(results: List[RecoveryResult]) -> str:
    if not results:
        return "Recovery Report\n" + "=" * 40 + "\nNo recovery events detected.\n"

    lines = ["Recovery Report", "=" * 40]
    for result in sorted(results, key=lambda r: r.pipeline_name):
        lines.append(format_recovery_result(result))
    lines.append("")
    lines.append(f"Total recoveries: {len(results)}")
    return "\n".join(lines) + "\n"


def print_recovery_report(results: List[RecoveryResult]) -> None:
    print(build_recovery_report(results), end="")
