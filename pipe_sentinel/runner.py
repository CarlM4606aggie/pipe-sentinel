"""Pipeline runner with retry logic for pipe-sentinel."""

import subprocess
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pipe_sentinel.config import PipelineConfig

logger = logging.getLogger(__name__)


@dataclass
class RunResult:
    """Result of a single pipeline run attempt."""

    pipeline_name: str
    attempt: int
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    started_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    duration_seconds: float = 0.0

    def __post_init__(self):
        if self.finished_at is None:
            self.finished_at = datetime.utcnow()
        delta = self.finished_at - self.started_at
        self.duration_seconds = delta.total_seconds()


def run_pipeline(config: PipelineConfig) -> RunResult:
    """Execute a pipeline command once and return the result."""
    started_at = datetime.utcnow()
    logger.info("Running pipeline '%s': %s", config.name, config.command)

    try:
        proc = subprocess.run(
            config.command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=config.timeout_seconds,
        )
        finished_at = datetime.utcnow()
        success = proc.returncode == 0
        return RunResult(
            pipeline_name=config.name,
            attempt=1,
            success=success,
            exit_code=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
            started_at=started_at,
            finished_at=finished_at,
        )
    except subprocess.TimeoutExpired as exc:
        finished_at = datetime.utcnow()
        logger.error("Pipeline '%s' timed out after %ss", config.name, config.timeout_seconds)
        return RunResult(
            pipeline_name=config.name,
            attempt=1,
            success=False,
            exit_code=-1,
            stdout="",
            stderr=f"Timed out after {config.timeout_seconds}s",
            started_at=started_at,
            finished_at=finished_at,
        )


def run_with_retries(config: PipelineConfig) -> list[RunResult]:
    """Run a pipeline with configured retry logic. Returns all attempt results."""
    results: list[RunResult] = []

    for attempt in range(1, config.max_retries + 2):  # +1 for the initial run
        result = run_pipeline(config)
        result.attempt = attempt
        results.append(result)

        if result.success:
            logger.info(
                "Pipeline '%s' succeeded on attempt %d", config.name, attempt
            )
            break

        if attempt <= config.max_retries:
            logger.warning(
                "Pipeline '%s' failed (attempt %d/%d). Retrying in %ds...",
                config.name,
                attempt,
                config.max_retries + 1,
                config.retry_delay_seconds,
            )
            time.sleep(config.retry_delay_seconds)
        else:
            logger.error(
                "Pipeline '%s' failed after %d attempt(s).", config.name, attempt
            )

    return results
