"""Tests for pipe_sentinel.alert_gate."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipe_sentinel.alert_gate import GateReport, process_failures
from pipe_sentinel.config import SmtpConfig
from pipe_sentinel.notifier import NotificationResult
from pipe_sentinel.runner import RunResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def smtp_cfg() -> SmtpConfig:
    return SmtpConfig(host="localhost", port=25, sender="sentinel@example.com")


@pytest.fixture()
default_state_file(tmp_path: Path) -> Path:
    return tmp_path / "throttle.json"


def _failed(name: str = "pipe_a") -> RunResult:
    return RunResult(pipeline_name=name, success=False, returncode=1, stdout="", stderr="err", duration=1.0)


def _passed(name: str = "pipe_ok") -> RunResult:
    return RunResult(pipeline_name=name, success=True, returncode=0, stdout="ok", stderr="", duration=0.5)


# ---------------------------------------------------------------------------
# GateReport
# ---------------------------------------------------------------------------

def test_gate_report_total() -> None:
    r = GateReport(sent=["a", "b"], suppressed=["c"])
    assert r.total == 3


def test_gate_report_empty() -> None:
    r = GateReport()
    assert r.total == 0


# ---------------------------------------------------------------------------
# process_failures — dry_run
# ---------------------------------------------------------------------------

def test_dry_run_sends_no_smtp(smtp_cfg: SmtpConfig, tmp_path: Path) -> None:
    sf = tmp_path / "t.json"
    with patch("pipe_sentinel.alert_gate.notify_recipients") as mock_notify:
        report = process_failures([_failed()], smtp_cfg, ["a@b.com"], dry_run=True, state_file=sf)
    mock_notify.assert_not_called()
    assert "pipe_a" in report.sent


def test_dry_run_does_not_persist_state(smtp_cfg: SmtpConfig, tmp_path: Path) -> None:
    sf = tmp_path / "t.json"
    process_failures([_failed()], smtp_cfg, ["a@b.com"], dry_run=True, state_file=sf)
    assert not sf.exists()


# ---------------------------------------------------------------------------
# process_failures — success results skipped
# ---------------------------------------------------------------------------

def test_passing_results_are_ignored(smtp_cfg: SmtpConfig, tmp_path: Path) -> None:
    sf = tmp_path / "t.json"
    with patch("pipe_sentinel.alert_gate.notify_recipients") as mock_notify:
        report = process_failures([_passed()], smtp_cfg, ["a@b.com"], state_file=sf)
    mock_notify.assert_not_called()
    assert report.total == 0


# ---------------------------------------------------------------------------
# process_failures — throttle suppression
# ---------------------------------------------------------------------------

def test_suppressed_within_cooldown(smtp_cfg: SmtpConfig, tmp_path: Path) -> None:
    sf = tmp_path / "t.json"
    ok = NotificationResult(success=True, recipients=["a@b.com"], error=None)
    with patch("pipe_sentinel.alert_gate.notify_recipients", return_value=ok):
        # First call — should send
        r1 = process_failures([_failed()], smtp_cfg, ["a@b.com"], cooldown_seconds=3600, state_file=sf)
        # Second call — should be suppressed
        r2 = process_failures([_failed()], smtp_cfg, ["a@b.com"], cooldown_seconds=3600, state_file=sf)

    assert "pipe_a" in r1.sent
    assert "pipe_a" in r2.suppressed


# ---------------------------------------------------------------------------
# process_failures — notification error
# ---------------------------------------------------------------------------

def test_notification_error_recorded(smtp_cfg: SmtpConfig, tmp_path: Path) -> None:
    sf = tmp_path / "t.json"
    bad = NotificationResult(success=False, recipients=[], error="SMTP timeout")
    with patch("pipe_sentinel.alert_gate.notify_recipients", return_value=bad):
        report = process_failures([_failed()], smtp_cfg, ["a@b.com"], state_file=sf)
    assert "pipe_a" in report.errors
    assert "pipe_a" not in report.sent
