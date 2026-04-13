"""Alert gate: combines throttle state with notifier to send deduplicated alerts."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from pipe_sentinel.notifier import NotificationResult, notify_recipients
from pipe_sentinel.runner import RunResult
from pipe_sentinel.throttle import ThrottleState, mark_alerted, should_alert
from pipe_sentinel.config import SmtpConfig


@dataclass
class GateReport:
    """Summary of alert decisions made by the gate."""

    sent: List[str] = field(default_factory=list)
    suppressed: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.sent) + len(self.suppressed)

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"GateReport(sent={len(self.sent)}, "
            f"suppressed={len(self.suppressed)}, "
            f"errors={len(self.errors)})"
        )


def process_failures(
    results: List[RunResult],
    smtp_cfg: SmtpConfig,
    recipients: List[str],
    *,
    cooldown_seconds: int = 3600,
    state_file: Path = Path(".pipe_sentinel_throttle.json"),
    dry_run: bool = False,
) -> GateReport:
    """Send alerts for failed *results*, skipping any within the cooldown window.

    Parameters
    ----------
    results:
        Run results to evaluate — only failures trigger alerts.
    smtp_cfg:
        SMTP configuration forwarded to :func:`notify_recipients`.
    recipients:
        E-mail addresses to notify.
    cooldown_seconds:
        Minimum seconds between repeated alerts for the same pipeline.
    state_file:
        Path used to persist throttle state across invocations.
    dry_run:
        When *True* alerts are evaluated but never sent or persisted.
    """
    state = ThrottleState(cooldown_seconds=cooldown_seconds, state_file=state_file)
    state.load()

    report = GateReport()

    for result in results:
        if result.success:
            continue

        name = result.pipeline_name

        if not should_alert(state, name):
            report.suppressed.append(name)
            continue

        if dry_run:
            report.sent.append(name)
            continue

        notification: NotificationResult = notify_recipients(
            result, smtp_cfg, recipients
        )
        if notification.success:
            mark_alerted(state, name, persist=True)
            report.sent.append(name)
        else:
            report.errors.append(name)

    return report
