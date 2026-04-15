"""Runtime budget enforcement — cap total wall-clock seconds across a run batch."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipe_sentinel.runner import RunResult


@dataclass
class BudgetConfig:
    max_total_seconds: float
    warn_at_percent: float = 80.0

    def __post_init__(self) -> None:
        if self.max_total_seconds <= 0:
            raise ValueError("max_total_seconds must be positive")
        if not (0 < self.warn_at_percent <= 100):
            raise ValueError("warn_at_percent must be in (0, 100]")

    @property
    def warn_threshold(self) -> float:
        return self.max_total_seconds * (self.warn_at_percent / 100.0)


@dataclass
class BudgetResult:
    config: BudgetConfig
    total_seconds: float
    pipeline_count: int
    exceeded: bool
    warned: bool
    contributions: List[tuple] = field(default_factory=list)  # (name, duration)

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, self.config.max_total_seconds - self.total_seconds)

    @property
    def utilisation_pct(self) -> float:
        if self.config.max_total_seconds == 0:
            return 100.0
        return (self.total_seconds / self.config.max_total_seconds) * 100.0

    def __str__(self) -> str:
        status = "EXCEEDED" if self.exceeded else ("WARN" if self.warned else "OK")
        return (
            f"BudgetResult[{status}] "
            f"{self.total_seconds:.1f}s / {self.config.max_total_seconds:.1f}s "
            f"({self.utilisation_pct:.1f}%)"
        )


def evaluate_budget(config: BudgetConfig, results: List[RunResult]) -> BudgetResult:
    """Evaluate whether a list of run results stays within the configured budget."""
    contributions = [
        (r.pipeline_name, r.duration_seconds)
        for r in results
        if r.duration_seconds is not None
    ]
    total = sum(d for _, d in contributions)
    exceeded = total > config.max_total_seconds
    warned = (not exceeded) and (total >= config.warn_threshold)
    return BudgetResult(
        config=config,
        total_seconds=total,
        pipeline_count=len(results),
        exceeded=exceeded,
        warned=warned,
        contributions=contributions,
    )
