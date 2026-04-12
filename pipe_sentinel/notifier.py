"""Email notification module for pipeline failure alerts."""

import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dataclasses import dataclass
from typing import Optional

from pipe_sentinel.config import SmtpConfig
from pipe_sentinel.runner import RunResult

logger = logging.getLogger(__name__)


@dataclass
class NotificationResult:
    success: bool
    recipient: str
    error: Optional[str] = None


def build_failure_message(pipeline_name: str, result: RunResult) -> tuple[str, str]:
    """Build subject and body for a pipeline failure email."""
    subject = f"[pipe-sentinel] Pipeline FAILED: {pipeline_name}"
    body = (
        f"Pipeline '{pipeline_name}' has failed after {result.attempts} attempt(s).\n\n"
        f"Exit code : {result.exit_code}\n"
        f"Duration  : {result.duration:.2f}s\n\n"
        f"--- stdout ---\n{result.stdout or '(empty)'}\n\n"
        f"--- stderr ---\n{result.stderr or '(empty)'}\n"
    )
    return subject, body


def send_alert(
    smtp_cfg: SmtpConfig,
    pipeline_name: str,
    result: RunResult,
    recipient: str,
) -> NotificationResult:
    """Send a failure alert email for the given pipeline run result."""
    subject, body = build_failure_message(pipeline_name, result)

    msg = MIMEMultipart()
    msg["From"] = smtp_cfg.sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(smtp_cfg.host, smtp_cfg.port, timeout=10) as server:
            if smtp_cfg.use_tls:
                server.starttls()
            if smtp_cfg.username and smtp_cfg.password:
                server.login(smtp_cfg.username, smtp_cfg.password)
            server.sendmail(smtp_cfg.sender, [recipient], msg.as_string())
        logger.info("Alert sent to %s for pipeline '%s'", recipient, pipeline_name)
        return NotificationResult(success=True, recipient=recipient)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send alert to %s: %s", recipient, exc)
        return NotificationResult(success=False, recipient=recipient, error=str(exc))


def notify_recipients(
    smtp_cfg: SmtpConfig,
    pipeline_name: str,
    result: RunResult,
    recipients: list[str],
) -> list[NotificationResult]:
    """Send alerts to all configured recipients and return results."""
    return [
        send_alert(smtp_cfg, pipeline_name, result, recipient)
        for recipient in recipients
    ]
