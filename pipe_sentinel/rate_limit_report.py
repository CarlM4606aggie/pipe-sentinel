"""Formatting helpers for rate limiter state reports."""
from __future__ import annotations

from typing import List

from pipe_sentinel.rate_limit import RateLimitState


def _bar(used: int, limit: int, width: int = 10) -> str:
    filled = int(round(width * used / limit)) if limit else 0
    filled = min(filled, width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def format_state(state: RateLimitState, now: float | None = None) -> str:
    used = state.runs_in_window(now)
    bar = _bar(used, state.max_runs)
    status = "LIMITED" if state.is_limited(now) else "OK"
    return (
        f"  {state.pipeline:<30} {bar} {used}/{state.max_runs} "
        f"(window={state.window_seconds}s) [{status}]"
    )


def build_rate_limit_report(
    states: List[RateLimitState],
    now: float | None = None,
) -> str:
    if not states:
        return "Rate Limit Report\n  (no pipelines tracked)\n"
    lines = ["Rate Limit Report", "-" * 60]
    limited = [s for s in states if s.is_limited(now)]
    ok = [s for s in states if not s.is_limited(now)]
    for s in sorted(ok, key=lambda x: x.pipeline):
        lines.append(format_state(s, now))
    for s in sorted(limited, key=lambda x: x.pipeline):
        lines.append(format_state(s, now))
    lines.append("-" * 60)
    lines.append(f"  Pipelines tracked: {len(states)}  Limited: {len(limited)}")
    return "\n".join(lines) + "\n"


def print_rate_limit_report(
    states: List[RateLimitState],
    now: float | None = None,
) -> None:
    print(build_rate_limit_report(states, now), end="")
