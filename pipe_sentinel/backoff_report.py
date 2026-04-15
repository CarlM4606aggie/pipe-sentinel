"""Formatting helpers for BackoffConfig and delay schedules."""
from __future__ import annotations

from typing import List

from pipe_sentinel.backoff import BackoffConfig, delay_schedule

_COL_WIDTH = 12


def _bar(value: float, max_value: float, width: int = 20) -> str:
    if max_value <= 0:
        return " " * width
    filled = int(round(value / max_value * width))
    return "#" * filled + "-" * (width - filled)


def format_backoff_config(config: BackoffConfig) -> str:
    lines = [
        "Backoff Configuration",
        f"  Strategy  : {config.strategy.value}",
        f"  Base delay: {config.base_delay:.2f}s",
        f"  Multiplier: {config.multiplier:.2f}",
        f"  Max delay : {config.max_delay:.2f}s",
        f"  Jitter    : {'yes' if config.jitter else 'no'}",
    ]
    return "\n".join(lines)


def format_delay_table(config: BackoffConfig, retries: int) -> str:
    if retries <= 0:
        return "No retries configured."

    schedule: List[float] = delay_schedule(config, retries)
    max_d = max(schedule) if schedule else 0.0

    header = f"  {'Attempt':>7}  {'Delay (s)':>10}  {'':20}"
    rows = [header, "  " + "-" * (len(header) - 2)]
    for i, d in enumerate(schedule, start=1):
        bar = _bar(d, max_d)
        rows.append(f"  {i:>7}  {d:>10.2f}  {bar}")
    return "\n".join(rows)


def build_backoff_report(config: BackoffConfig, retries: int) -> str:
    parts = [
        format_backoff_config(config),
        "",
        "Delay Schedule:",
        format_delay_table(config, retries),
    ]
    return "\n".join(parts)


def print_backoff_report(config: BackoffConfig, retries: int) -> None:
    print(build_backoff_report(config, retries))
