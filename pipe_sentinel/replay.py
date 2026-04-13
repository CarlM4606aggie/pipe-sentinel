"""Replay failed pipeline runs from audit history."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipe_sentinel.audit import AuditRecord, fetch_recent
from pipe_sentinel.config import PipelineConfig, SentinelConfig
from pipe_sentinel.runner import RunResult, run_with_retries


@dataclass
class ReplayReport:
    replayed: List[RunResult] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.replayed) + len(self.skipped)

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.replayed if r.success)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.replayed if not r.success)

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"Replay: {len(self.replayed)} run(s), "
            f"{self.succeeded} succeeded, {self.failed} failed, "
            f"{len(self.skipped)} skipped (no matching config)"
        )


def _find_config(
    name: str, pipelines: List[PipelineConfig]
) -> Optional[PipelineConfig]:
    for p in pipelines:
        if p.name == name:
            return p
    return None


def _failed_names(db_path: str, limit: int) -> List[str]:
    records: List[AuditRecord] = fetch_recent(db_path, limit)
    seen: set = set()
    names: List[str] = []
    for rec in records:
        if rec.status == "failure" and rec.pipeline not in seen:
            seen.add(rec.pipeline)
            names.append(rec.pipeline)
    return names


def replay_failures(
    config: SentinelConfig,
    db_path: str,
    limit: int = 50,
    dry_run: bool = False,
) -> ReplayReport:
    """Re-run every pipeline that has a recent failure record."""
    report = ReplayReport()
    names = _failed_names(db_path, limit)

    for name in names:
        cfg = _find_config(name, config.pipelines)
        if cfg is None:
            report.skipped.append(name)
            continue
        if dry_run:
            report.skipped.append(name)
            continue
        result = run_with_retries(cfg)
        report.replayed.append(result)

    return report
