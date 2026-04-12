"""Health check module for verifying pipeline connectivity and config validity."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List
import subprocess
import shutil

from pipe_sentinel.config import PipelineConfig, SentinelConfig


@dataclass
class HealthResult:
    pipeline_name: str
    checks: dict = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return len(self.errors) == 0


def check_command_exists(pipeline: PipelineConfig) -> HealthResult:
    """Verify that the command executable is available on PATH."""
    result = HealthResult(pipeline_name=pipeline.name)
    parts = pipeline.command.split()
    executable = parts[0] if parts else ""
    if not executable:
        result.errors.append("Command is empty.")
        return result

    found = shutil.which(executable)
    result.checks["command_on_path"] = found is not None
    if not found:
        result.errors.append(f"Executable '{executable}' not found on PATH.")
    return result


def check_timeout_positive(pipeline: PipelineConfig) -> HealthResult:
    """Verify that the pipeline timeout is a positive number."""
    result = HealthResult(pipeline_name=pipeline.name)
    ok = pipeline.timeout_seconds > 0
    result.checks["timeout_positive"] = ok
    if not ok:
        result.errors.append(
            f"timeout_seconds must be > 0, got {pipeline.timeout_seconds}."
        )
    return result


def run_health_checks(config: SentinelConfig) -> List[HealthResult]:
    """Run all health checks for every pipeline in the config."""
    results: List[HealthResult] = []
    for pipeline in config.pipelines:
        combined = HealthResult(pipeline_name=pipeline.name)
        for checker in (check_command_exists, check_timeout_positive):
            r = checker(pipeline)
            combined.checks.update(r.checks)
            combined.errors.extend(r.errors)
        results.append(combined)
    return results


def print_health_report(results: List[HealthResult]) -> None:
    """Print a human-readable health report to stdout."""
    for r in results:
        status = "OK" if r.healthy else "FAIL"
        print(f"[{status}] {r.pipeline_name}")
        for check, passed in r.checks.items():
            symbol = "✓" if passed else "✗"
            print(f"      {symbol} {check}")
        for err in r.errors:
            print(f"      ! {err}")
