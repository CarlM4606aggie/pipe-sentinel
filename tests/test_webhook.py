"""Tests for pipe_sentinel.webhook and pipe_sentinel.webhook_report."""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from unittest.mock import MagicMock, patch

import pytest

from pipe_sentinel.runner import RunResult
from pipe_sentinel.webhook import (
    WebhookConfig,
    WebhookResult,
    build_payload,
    notify_webhook,
    send_webhook,
)
from pipe_sentinel.webhook_report import build_webhook_report, format_webhook_result


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def success_result() -> RunResult:
    return RunResult(
        pipeline_name="etl_daily",
        returncode=0,
        stdout="ok",
        stderr="",
        duration=1.23,
        attempts=1,
    )


@pytest.fixture()
def failure_result() -> RunResult:
    return RunResult(
        pipeline_name="etl_daily",
        returncode=1,
        stdout="",
        stderr="boom",
        duration=0.5,
        attempts=3,
    )


@pytest.fixture()
def cfg() -> WebhookConfig:
    return WebhookConfig(url="http://example.com/hook")


# ---------------------------------------------------------------------------
# build_payload
# ---------------------------------------------------------------------------

def test_build_payload_failure_status(failure_result):
    p = build_payload(failure_result)
    assert p["status"] == "failure"
    assert p["returncode"] == 1
    assert p["pipeline"] == "etl_daily"
    assert p["attempts"] == 3


def test_build_payload_success_status(success_result):
    p = build_payload(success_result)
    assert p["status"] == "success"
    assert p["returncode"] == 0


def test_build_payload_duration_rounded(success_result):
    p = build_payload(success_result)
    assert p["duration_seconds"] == 1.23


# ---------------------------------------------------------------------------
# send_webhook — mocked
# ---------------------------------------------------------------------------

def test_send_webhook_success(cfg):
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    with patch("urllib.request.urlopen", return_value=mock_resp):
        result = send_webhook(cfg, {"key": "value"})
    assert result.success is True
    assert result.status_code == 200


def test_send_webhook_http_error(cfg):
    import urllib.error
    with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
        cfg.url, 500, "Server Error", {}, None
    )):
        result = send_webhook(cfg, {})
    assert result.success is False
    assert result.status_code == 500


def test_send_webhook_connection_error(cfg):
    with patch("urllib.request.urlopen", side_effect=OSError("refused")):
        result = send_webhook(cfg, {})
    assert result.success is False
    assert result.status_code is None
    assert "refused" in result.error


# ---------------------------------------------------------------------------
# notify_webhook
# ---------------------------------------------------------------------------

def test_notify_webhook_calls_send(cfg, failure_result):
    with patch("pipe_sentinel.webhook.send_webhook") as mock_send:
        mock_send.return_value = WebhookResult(url=cfg.url, status_code=200, success=True)
        res = notify_webhook(cfg, failure_result)
    mock_send.assert_called_once()
    assert res.success is True


# ---------------------------------------------------------------------------
# webhook_report
# ---------------------------------------------------------------------------

def test_format_webhook_result_success():
    r = WebhookResult(url="http://x.com", status_code=200, success=True)
    line = format_webhook_result(r)
    assert "\u2705" in line
    assert "200" in line


def test_format_webhook_result_failure():
    r = WebhookResult(url="http://x.com", status_code=500, success=False, error="err")
    line = format_webhook_result(r)
    assert "\u274c" in line
    assert "err" in line


def test_build_webhook_report_empty():
    assert build_webhook_report([]) == "No webhooks configured."


def test_build_webhook_report_counts():
    results = [
        WebhookResult(url="http://a.com", status_code=200, success=True),
        WebhookResult(url="http://b.com", status_code=500, success=False, error="x"),
    ]
    report = build_webhook_report(results)
    assert "1/2" in report
