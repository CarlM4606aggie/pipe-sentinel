"""Formatting helpers for retry budget state."""
from __future__ import annotations

from typing import List

from pipe_sentinel.retry_budget import RetryBudgetConfig, RetryBudgetState

_BAR_WIDTH = 20


def _bar(used: int, total: int) -> str:
    if total == 0:
        return "[" + " " * _BAR_WIDTH + "]"
    filled = round(_BAR_WIDTH * used / total)
    filled = max(0, min(_BAR_WIDTH, filled))
    return "[" + "#" * filled + "-" * (_BAR_WIDTH - filled) + "]"


def format_budget_state(state: RetryBudgetState, cfg: RetryBudgetConfig) -> str:
    used = len(state.attempts)
    remaining = state.remaining(cfg)
    exhausted = state.is_exhausted(cfg)
    status = "EXHAUSTED" if exhausted else "ok"
    bar = _bar(used, cfg.max_retries)
    return (
        f"  pipeline : {state.pipeline}\n"
        f"  status   : {status}\n"
        f"  used     : {used}/{cfg.max_retries} {bar}\n"
        f"  remaining: {remaining}\n"
        f"  window   : {cfg.window_seconds}s\n"
    )


def build_retry_budget_report(
    states: List[RetryBudgetState], cfg: RetryBudgetConfig
) -> str:
    if not states:
        return "Retry Budget Report\n  (no data)\n"
    lines = [f"Retry Budget Report  [{len(states)} pipeline(s)]"]
    for s in states:
        lines.append("")
        lines.append(format_budget_state(s, cfg).rstrip())
    return "\n".join(lines) + "\n"


def print_retry_budget_report(
    states: List[RetryBudgetState], cfg: RetryBudgetConfig
) -> None:
    print(build_retry_budget_report(states, cfg), end="")
