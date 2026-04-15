"""Tests for pipe_sentinel.signal."""
from __future__ import annotations

from datetime import datetime
from typing import List

import pytest

from pipe_sentinel.runner import RunResult
from pipe_sentinel.signal import (
    PipelineSignal,
    SignalReport,
    build_signal_report,
)


def _res(name: str, success: bool, duration: float = 1.0) -> RunResult:
    return RunResult(
        pipeline_name=name,
        success=success,
        returncode=0 if success else 1,
        stdout="",
        stderr="" if success else "error",
        duration=duration,
        attempts=1,
        started_at=datetime(2024, 1, 1, 12, 0, 0),
    )


# ---------------------------------------------------------------------------
# PipelineSignal
# ---------------------------------------------------------------------------

class TestPipelineSignal:
    def test_needs_alert_when_failed(self):
        sig = PipelineSignal("etl", _res("etl", False), consecutive_failures=1)
        assert sig.needs_alert is True

    def test_no_alert_when_success(self):
        sig = PipelineSignal("etl", _res("etl", True), consecutive_failures=0)
        assert sig.needs_alert is False

    def test_no_alert_when_suppressed(self):
        sig = PipelineSignal("etl", _res("etl", False), consecutive_failures=2, suppressed=True)
        assert sig.needs_alert is False

    def test_no_alert_when_paused(self):
        sig = PipelineSignal("etl", _res("etl", False), consecutive_failures=1, paused=True)
        assert sig.needs_alert is False

    def test_no_alert_when_no_result(self):
        sig = PipelineSignal("etl", None)
        assert sig.needs_alert is False

    def test_severity_ok_on_success(self):
        sig = PipelineSignal("etl", _res("etl", True))
        assert sig.severity == "ok"

    def test_severity_warning_on_low_failures(self):
        sig = PipelineSignal("etl", _res("etl", False), consecutive_failures=2)
        assert sig.severity == "warning"

    def test_severity_critical_at_threshold(self):
        sig = PipelineSignal("etl", _res("etl", False), consecutive_failures=3)
        assert sig.severity == "critical"

    def test_str_contains_pipeline_name(self):
        sig = PipelineSignal("my_pipe", _res("my_pipe", False), consecutive_failures=1)
        assert "my_pipe" in str(sig)

    def test_str_contains_severity(self):
        sig = PipelineSignal("p", _res("p", False), consecutive_failures=4)
        assert "critical" in str(sig)


# ---------------------------------------------------------------------------
# SignalReport
# ---------------------------------------------------------------------------

class TestSignalReport:
    def _report(self) -> SignalReport:
        return SignalReport(
            signals=[
                PipelineSignal("a", _res("a", False), consecutive_failures=1),
                PipelineSignal("b", _res("b", True), consecutive_failures=0),
                PipelineSignal("c", _res("c", False), consecutive_failures=3),
            ]
        )

    def test_alerts_excludes_passing(self):
        report = self._report()
        names = [s.pipeline_name for s in report.alerts]
        assert "b" not in names
        assert "a" in names

    def test_critical_returns_only_critical(self):
        report = self._report()
        names = [s.pipeline_name for s in report.critical]
        assert names == ["c"]

    def test_all_ok_false_when_failures(self):
        assert self._report().all_ok is False

    def test_all_ok_true_when_no_failures(self):
        report = SignalReport(
            signals=[PipelineSignal("a", _res("a", True))]
        )
        assert report.all_ok is True

    def test_summary_contains_counts(self):
        s = self._report().summary()
        assert "3 pipeline" in s
        assert "2 alert" in s


# ---------------------------------------------------------------------------
# build_signal_report
# ---------------------------------------------------------------------------

class TestBuildSignalReport:
    def test_single_pipeline_success(self):
        report = build_signal_report([_res("etl", True)])
        assert len(report.signals) == 1
        assert report.all_ok

    def test_consecutive_failures_counted(self):
        results = [
            _res("etl", True),
            _res("etl", False),
            _res("etl", False),
        ]
        report = build_signal_report(results)
        sig = report.signals[0]
        assert sig.consecutive_failures == 2

    def test_consecutive_failures_reset_by_success(self):
        results = [
            _res("etl", False),
            _res("etl", False),
            _res("etl", True),
        ]
        report = build_signal_report(results)
        assert report.signals[0].consecutive_failures == 0

    def test_suppressed_flag_set(self):
        report = build_signal_report(
            [_res("etl", False)], suppressed_names=["etl"]
        )
        assert report.signals[0].suppressed is True
        assert report.signals[0].needs_alert is False

    def test_paused_flag_set(self):
        report = build_signal_report(
            [_res("etl", False)], paused_names=["etl"]
        )
        assert report.signals[0].paused is True

    def test_multiple_pipelines_grouped_correctly(self):
        results = [
            _res("a", True),
            _res("b", False),
            _res("a", False),
        ]
        report = build_signal_report(results)
        names = {s.pipeline_name for s in report.signals}
        assert names == {"a", "b"}
