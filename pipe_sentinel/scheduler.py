"""Scheduler module for orchestrating pipeline runs with retry and notification logic."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List

from pipe_sentinel.config import PipelineConfig, SentinelConfig
from pipe_sentinel.notifier import notify_recipients
from pipe_sentinel.runner import RunResult, run_with_retries

logger = logging.getLogger(__name__)


@dataclass
class ScheduleReport:
    """Aggregated report produced after running all configured pipelines."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    results: List[RunResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return self.failed == 0


def run_all(config: SentinelConfig, *, dry_run: bool = False) -> ScheduleReport:
    """Run every pipeline defined in *config* and send alerts for failures.

    Parameters
    ----------
    config:
        Fully parsed :class:`SentinelConfig` instance.
    dry_run:
        When *True* pipelines are logged but not executed and no alerts are sent.
    """
    report = ScheduleReport(total=len(config.pipelines))

    for pipeline in config.pipelines:
        if dry_run:
            logger.info("[dry-run] would run pipeline '%s'", pipeline.name)
            continue

        logger.info("Running pipeline '%s'", pipeline.name)
        result = run_with_retries(pipeline)
        report.results.append(result)

        if result.success:
            report.succeeded += 1
            logger.info("Pipeline '%s' succeeded.", pipeline.name)
        else:
            report.failed += 1
            logger.warning(
                "Pipeline '%s' failed after %d attempt(s).",
                pipeline.name,
                result.attempts,
            )
            if config.smtp and pipeline.recipients:
                notify_recipients(
                    smtp_cfg=config.smtp,
                    pipeline=pipeline,
                    result=result,
                )

    return report
