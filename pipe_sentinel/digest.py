"""Daily/periodic digest builder: aggregates run results into a human-readable summary email body."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

from pipe_sentinel.audit import AuditRecord


@dataclass
class DigestReport:
    generated_at: datetime
    total: int
    passed: int
    failed: int
    pipelines: List[str]  # names of failed pipelines

    @property
    def pass_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.passed / self.total * 100

    @property
    def has_failures(self) -> bool:
        return self.failed > 0


def build_digest(records: List[AuditRecord], generated_at: datetime | None = None) -> DigestReport:
    """Compute a DigestReport from a list of AuditRecord objects."""
    if generated_at is None:
        generated_at = datetime.now(timezone.utc)

    total = len(records)
    passed = sum(1 for r in records if r.status == "success")
    failed = total - passed
    failed_names = [r.pipeline_name for r in records if r.status != "success"]

    return DigestReport(
        generated_at=generated_at,
        total=total,
        passed=passed,
        failed=failed,
        pipelines=failed_names,
    )


def format_digest(report: DigestReport) -> str:
    """Render a DigestReport as a plain-text email body."""
    ts = report.generated_at.strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"Pipe-Sentinel Digest — {ts}",
        "=" * 40,
        f"Total runs : {report.total}",
        f"Passed     : {report.passed}",
        f"Failed     : {report.failed}",
        f"Pass rate  : {report.pass_rate:.1f}%",
    ]

    if report.has_failures:
        lines.append("")
        lines.append("Failed pipelines:")
        for name in report.pipelines:
            lines.append(f"  - {name}")
    else:
        lines.append("")
        lines.append("All pipelines passed. ✓")

    return "\n".join(lines)


def print_digest(report: DigestReport) -> None:  # pragma: no cover
    print(format_digest(report))
