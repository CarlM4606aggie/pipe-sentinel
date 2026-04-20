"""Shadow mode: run pipelines without side-effects and compare results."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipe_sentinel.runner import RunResult


@dataclass
class ShadowComparison:
    pipeline_name: str
    live_result: RunResult
    shadow_result: RunResult

    @property
    def outcomes_match(self) -> bool:
        return self.live_result.success == self.shadow_result.success

    @property
    def duration_delta(self) -> float:
        """Difference in duration (shadow - live) in seconds."""
        if self.live_result.duration is None or self.shadow_result.duration is None:
            return 0.0
        return self.shadow_result.duration - self.live_result.duration

    def __str__(self) -> str:
        match_label = "MATCH" if self.outcomes_match else "DIVERGE"
        delta = f"{self.duration_delta:+.2f}s"
        return (
            f"[{match_label}] {self.pipeline_name} "
            f"live={'OK' if self.live_result.success else 'FAIL'} "
            f"shadow={'OK' if self.shadow_result.success else 'FAIL'} "
            f"Δduration={delta}"
        )


@dataclass
class ShadowReport:
    comparisons: List[ShadowComparison] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.comparisons)

    @property
    def divergences(self) -> List[ShadowComparison]:
        return [c for c in self.comparisons if not c.outcomes_match]

    @property
    def divergence_count(self) -> int:
        return len(self.divergences)

    @property
    def all_match(self) -> bool:
        return self.divergence_count == 0


def compare_results(
    pipeline_name: str,
    live: RunResult,
    shadow: RunResult,
) -> ShadowComparison:
    """Build a ShadowComparison from two RunResult objects."""
    return ShadowComparison(
        pipeline_name=pipeline_name,
        live_result=live,
        shadow_result=shadow,
    )


def build_shadow_report(comparisons: List[ShadowComparison]) -> ShadowReport:
    return ShadowReport(comparisons=comparisons)


def print_shadow_report(report: ShadowReport) -> None:
    print(f"Shadow Report  ({report.total} pipeline(s))")
    print(f"  Divergences : {report.divergence_count}")
    print()
    for c in report.comparisons:
        print(f"  {c}")
    if not report.comparisons:
        print("  (no comparisons recorded)")
