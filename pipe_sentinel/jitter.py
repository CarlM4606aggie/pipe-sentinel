"""Retry jitter strategies for pipeline execution backoff."""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class JitterStrategy(str, Enum):
    NONE = "none"
    FULL = "full"
    EQUAL = "equal"
    DECORRELATED = "decorrelated"


@dataclass
class JitterConfig:
    strategy: JitterStrategy = JitterStrategy.FULL
    base_delay: float = 1.0          # seconds
    max_delay: float = 60.0          # seconds
    multiplier: float = 2.0
    seed: Optional[int] = None
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)

    def delay_for(self, attempt: int) -> float:
        """Return the sleep duration (seconds) for *attempt* (0-indexed)."""
        cap = self.max_delay
        base = self.base_delay
        exp = min(base * (self.multiplier ** attempt), cap)

        if self.strategy == JitterStrategy.NONE:
            return exp

        if self.strategy == JitterStrategy.FULL:
            return self._rng.uniform(0, exp)

        if self.strategy == JitterStrategy.EQUAL:
            half = exp / 2
            return half + self._rng.uniform(0, half)

        if self.strategy == JitterStrategy.DECORRELATED:
            # Each call uses the previous delay; we approximate with exp here.
            return min(self._rng.uniform(base, exp * 3), cap)

        return exp  # fallback


def parse_jitter_config(raw: dict) -> JitterConfig:
    """Build a JitterConfig from a plain dict (e.g. loaded from YAML)."""
    strategy = JitterStrategy(raw.get("strategy", JitterStrategy.FULL))
    return JitterConfig(
        strategy=strategy,
        base_delay=float(raw.get("base_delay", 1.0)),
        max_delay=float(raw.get("max_delay", 60.0)),
        multiplier=float(raw.get("multiplier", 2.0)),
        seed=raw.get("seed"),
    )
