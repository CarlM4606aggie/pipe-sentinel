"""Tests for pipe_sentinel.replay and pipe_sentinel.replay_report."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipe_sentinel.audit import init_db
from pipe_sentinel.config import PipelineConfig, SentinelConfig, SmtpConfig
from pipe_sentinel.replay import ReplayReport, _find_config, replay_failures
from pipe_sentinel.replay_report import format_replay_report
from pipe_sentinel.runner import RunResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path: Path) -> str:
    path = str(tmp_path / "audit.db")
    init_db(path)
    return path


def _insert(db: str, pipeline: str, status: str, ts: str = "2024-01-01T00:00:00") -> None:
    con = sqlite3.connect(db)
    con.execute(
        "INSERT INTO runs (pipeline, status, duration, stdout, stderr, timestamp)"
        " VALUES (?, ?, 1.0, '', '', ?)",
        (pipeline, status, ts),
    )
    con.commit()
    con.close()


def _smtp() -> SmtpConfig:
    return SmtpConfig(host="localhost", port=25, sender="a@b.com", recipients=["x@y.com"])


def _pipeline(name: str) -> PipelineConfig:
    return PipelineConfig(name=name, command=f"echo {name}", retries=0, timeout=30)


def _sentinel(*names: str) -> SentinelConfig:
    return SentinelConfig(smtp=_smtp(), pipelines=[_pipeline(n) for n in names])


# ---------------------------------------------------------------------------
# ReplayReport
# ---------------------------------------------------------------------------

def test_replay_report_totals() -> None:
    r = ReplayReport()
    r.replayed.append(RunResult(pipeline_name="a", success=True, returncode=0, stdout="", stderr="", duration=1.0))
    r.replayed.append(RunResult(pipeline_name="b", success=False, returncode=1, stdout="", stderr="err", duration=0.5))
    r.skipped.append("c")
    assert r.total == 3
    assert r.succeeded == 1
    assert r.failed == 1


def test_find_config_returns_match() -> None:
    pipelines = [_pipeline("alpha"), _pipeline("beta")]
    result = _find_config("beta", pipelines)
    assert result is not None
    assert result.name == "beta"


def test_find_config_returns_none_for_missing() -> None:
    assert _find_config("ghost", [_pipeline("alpha")]) is None


# ---------------------------------------------------------------------------
# replay_failures
# ---------------------------------------------------------------------------

def test_replay_failures_skips_unknown_pipeline(db_path: str) -> None:
    _insert(db_path, "unknown_pipe", "failure")
    cfg = _sentinel("other_pipe")
    report = replay_failures(cfg, db_path)
    assert "unknown_pipe" in report.skipped
    assert report.replayed == []


def test_replay_failures_dry_run_skips_all(db_path: str) -> None:
    _insert(db_path, "etl_load", "failure")
    cfg = _sentinel("etl_load")
    report = replay_failures(cfg, db_path, dry_run=True)
    assert "etl_load" in report.skipped
    assert report.replayed == []


def test_replay_failures_reruns_known_pipeline(db_path: str) -> None:
    _insert(db_path, "etl_load", "failure")
    cfg = _sentinel("etl_load")
    fake_result = RunResult(pipeline_name="etl_load", success=True, returncode=0, stdout="ok", stderr="", duration=0.1)
    with patch("pipe_sentinel.replay.run_with_retries", return_value=fake_result) as mock_run:
        report = replay_failures(cfg, db_path)
    mock_run.assert_called_once()
    assert len(report.replayed) == 1
    assert report.replayed[0].pipeline_name == "etl_load"


def test_replay_failures_ignores_success_records(db_path: str) -> None:
    _insert(db_path, "etl_load", "success")
    cfg = _sentinel("etl_load")
    report = replay_failures(cfg, db_path)
    assert report.replayed == []
    assert report.skipped == []


# ---------------------------------------------------------------------------
# format_replay_report
# ---------------------------------------------------------------------------

def test_format_replay_report_dry_run_lists_skipped() -> None:
    r = ReplayReport(skipped=["pipe_a", "pipe_b"])
    out = format_replay_report(r, dry_run=True)
    assert "dry-run" in out
    assert "pipe_a" in out
    assert "pipe_b" in out


def test_format_replay_report_shows_results() -> None:
    r = ReplayReport()
    r.replayed.append(RunResult(pipeline_name="x", success=True, returncode=0, stdout="", stderr="", duration=2.5))
    out = format_replay_report(r)
    assert "Replay Report" in out
    assert "x" in out
    assert "2.50s" in out


def test_format_replay_report_shows_stderr_snippet_on_failure() -> None:
    r = ReplayReport()
    r.replayed.append(RunResult(pipeline_name="y", success=False, returncode=1, stdout="", stderr="connection refused", duration=0.3))
    out = format_replay_report(r)
    assert "connection refused" in out
