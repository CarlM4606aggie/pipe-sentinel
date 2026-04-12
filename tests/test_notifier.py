"""Tests for the notifier module."""

import pytest
from unittest.mock import MagicMock, patch

from pipe_sentinel.config import SmtpConfig
from pipe_sentinel.runner import RunResult
from pipe_sentinel.notifier import (
    build_failure_message,
    send_alert,
    notify_recipients,
    NotificationResult,
)


@pytest.fixture
def smtp_cfg() -> SmtpConfig:
    return SmtpConfig(
        host="smtp.example.com",
        port=587,
        sender="sentinel@example.com",
        username="user",
        password="secret",
        use_tls=True,
    )


@pytest.fixture
def failed_result() -> RunResult:
    return RunResult(
        success=False,
        exit_code=1,
        stdout="",
        stderr="Something went wrong",
        duration=3.5,
        attempts=3,
    )


def test_build_failure_message_subject(failed_result):
    subject, _ = build_failure_message("my_pipeline", failed_result)
    assert "my_pipeline" in subject
    assert "FAILED" in subject


def test_build_failure_message_body_contains_details(failed_result):
    _, body = build_failure_message("my_pipeline", failed_result)
    assert "3 attempt(s)" in body
    assert "exit code" in body.lower()
    assert "Something went wrong" in body


def test_send_alert_success(smtp_cfg, failed_result):
    with patch("pipe_sentinel.notifier.smtplib.SMTP") as mock_smtp_cls:
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_server

        result = send_alert(smtp_cfg, "my_pipeline", failed_result, "ops@example.com")

    assert result.success is True
    assert result.recipient == "ops@example.com"
    assert result.error is None
    mock_server.starttls.assert_called_once()
    mock_server.login.assert_called_once_with("user", "secret")
    mock_server.sendmail.assert_called_once()


def test_send_alert_no_tls_skips_starttls(smtp_cfg, failed_result):
    """When use_tls is False, starttls should not be called."""
    smtp_cfg_no_tls = SmtpConfig(
        host="smtp.example.com",
        port=25,
        sender="sentinel@example.com",
        username="user",
        password="secret",
        use_tls=False,
    )
    with patch("pipe_sentinel.notifier.smtplib.SMTP") as mock_smtp_cls:
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_server

        result = send_alert(smtp_cfg_no_tls, "my_pipeline", failed_result, "ops@example.com")

    assert result.success is True
    mock_server.starttls.assert_not_called()


def test_send_alert_failure_returns_error(smtp_cfg, failed_result):
    with patch("pipe_sentinel.notifier.smtplib.SMTP", side_effect=OSError("conn refused")):
        result = send_alert(smtp_cfg, "my_pipeline", failed_result, "ops@example.com")

    assert result.success is False
    assert "conn refused" in result.error


def test_notify_recipients_returns_all_results(smtp_cfg, failed_result):
    recipients = ["a@example.com", "b@example.com"]
    with patch("pipe_sentinel.notifier.smtplib.SMTP") as mock_smtp_cls:
        mock_smtp_cls.return_value.__enter__.return_value = MagicMock()
        results = notify_recipients(smtp_cfg, "pipe", failed_result, recipients)

    assert len(results) == 2
    assert all(isinstance(r, NotificationResult) for r in results)
    assert all(r.success for r in results)


def test_notify_recipients_empty_list(smtp_cfg, failed_result):
    results = notify_recipients(smtp_cfg, "pipe", failed_result, [])
    assert results == []
