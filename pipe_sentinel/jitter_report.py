"""Formatting helpers for JitterConfig inspection."""
from __future__ import annotations

from pipe_sentinel.jitter import JitterConfig, JitterStrategy


_STRATEGY_LABELS: dict[JitterStrategy, str] = {
    JitterStrategy.NONE: "No jitter (deterministic exponential backoff)",
    JitterStrategy.FULL: "Full jitter  (uniform 0 … cap)",
    JitterStrategy.EQUAL: "Equal jitter (half-cap + uniform 0 … half-cap)",
    JitterStrategy.DECORRELATED: "Decorrelated jitter (AWS-style)",
}


def format_jitter_config(cfg: JitterConfig) -> str:
    lines = [
        "Jitter Configuration",
        "=" * 30,
        f"  Strategy   : {_STRATEGY_LABELS.get(cfg.strategy, cfg.strategy)}",
        f"  Base delay : {cfg.base_delay:.2f}s",
        f"  Max delay  : {cfg.max_delay:.2f}s",
        f"  Multiplier : {cfg.multiplier:.2f}x",
    ]
    return "\n".join(lines)


def format_delay_table(cfg: JitterConfig, attempts: int = 5) -> str:
    """Show example delays for the first *attempts* retries."""
    from pipe_sentinel.jitter import JitterConfig, parse_jitter_config
    import dataclasses

    # Use a fresh deterministic copy for display
    preview_cfg = dataclasses.replace(cfg, seed=0)
    preview_cfg.__post_init__()

    header = f"{'Attempt':>8}  {'Delay (s)':>10}"
    separator = "-" * len(header)
    rows = [header, separator]
    for i in range(attempts):
        delay = preview_cfg.delay_for(i)
        rows.append(f"{i + 1:>8}  {delay:>10.3f}")
    return "\n".join(rows)


def build_jitter_report(cfg: JitterConfig, attempts: int = 5) -> str:
    parts = [
        format_jitter_config(cfg),
        "",
        "Example delays (preview with seed=0):",
        format_delay_table(cfg, attempts),
    ]
    return "\n".join(parts)


def print_jitter_report(cfg: JitterConfig, attempts: int = 5) -> None:
    print(build_jitter_report(cfg, attempts))
