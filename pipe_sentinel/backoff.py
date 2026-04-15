"""Backoff policy for retry delays between pipeline run attempts."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List


class BackoffStrategy(str, Enum):
    CONSTANT = "constant"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


@dataclass
class BackoffConfig:
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    base_delay: float = 1.0      # seconds
    multiplier: float = 2.0
    max_delay: float = 60.0
    jitter: bool = False

    def __post_init__(self) -> None:
        if self.base_delay <= 0:
            raise ValueError("base_delay must be positive")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if self.multiplier <= 0:
            raise ValueError("multiplier must be positive")


def delay_for(config: BackoffConfig, attempt: int) -> float:
    """Return the delay in seconds before *attempt* (0-indexed)."""
    if attempt <= 0:
        return 0.0

    strategy = config.strategy
    if strategy == BackoffStrategy.CONSTANT:
        raw = config.base_delay
    elif strategy == BackoffStrategy.LINEAR:
        raw = config.base_delay * attempt
    else:  # EXPONENTIAL
        raw = config.base_delay * (config.multiplier ** (attempt - 1))

    delay = min(raw, config.max_delay)

    if config.jitter:
        import random
        delay = random.uniform(0.0, delay)

    return delay


def delay_schedule(config: BackoffConfig, retries: int) -> List[float]:
    """Return a list of delays for each retry attempt (length == retries)."""
    return [delay_for(config, attempt) for attempt in range(retries)]


def parse_backoff_config(raw: dict) -> BackoffConfig:
    """Build a BackoffConfig from a plain dict (e.g. parsed from YAML)."""
    strategy = BackoffStrategy(raw.get("strategy", BackoffStrategy.EXPONENTIAL))
    return BackoffConfig(
        strategy=strategy,
        base_delay=float(raw.get("base_delay", 1.0)),
        multiplier=float(raw.get("multiplier", 2.0)),
        max_delay=float(raw.get("max_delay", 60.0)),
        jitter=bool(raw.get("jitter", False)),
    )
