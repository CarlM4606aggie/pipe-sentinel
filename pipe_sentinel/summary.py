"""Summary report generator for pipeline run schedules."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipe_sentinel.scheduler import ScheduleReport
from pipe_sentinel.runner import RunResult


@dataclass
class SummaryStats:
    total: int
    passed: int
    failed: int
    total_duration: float

    @property
    def pass_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.passed / self.total * 100


def compute_stats(results: List[RunResult]) -> SummaryStats:
    """Compute aggregate statistics from a list of RunResults."""
    total = len(results)
    passed = sum(1 for r in results if r.success)
    failed = total - passed
    total_duration = sum(r.duration for r in results)
    return SummaryStats(
        total=total,
        passed=passed,
        failed=failed,
        total_duration=total_duration,
    )


def format_summary(report: ScheduleReport) -> str:
    """Return a human-readable summary string for a ScheduleReport."""
    stats = compute_stats(report.results)
    lines: List[str] = [
        "=" * 40,
        "Pipeline Run Summary",
        "=" * 40,
        f"Total pipelines : {stats.total}",
        f"Passed          : {stats.passed}",
        f"Failed          : {stats.failed}",
        f"Pass rate       : {stats.pass_rate:.1f}%",
        f"Total duration  : {stats.total_duration:.2f}s",
        "-" * 40,
    ]
    for result in report.results:
        status = "OK" if result.success else "FAIL"
        lines.append(
            f"  [{status}] {result.pipeline_name:<25} {result.duration:.2f}s"
        )
    lines.append("=" * 40)
    return "\n".join(lines)


def print_summary(report: ScheduleReport) -> None:
    """Print the formatted summary to stdout."""
    print(format_summary(report))
