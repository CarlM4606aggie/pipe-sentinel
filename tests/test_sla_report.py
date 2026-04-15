"""Tests for pipe_sentinel.sla_report."""
from __future__ import annotations

from pipe_sentinel.sla import SLAResult
from pipe_sentinel.sla_report import (
    _icon,
    format_sla_result,
    build_sla_report,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _ok() -> SLAResult:
    return SLAResult(
        pipeline_name="pipe_ok", duration=20.0, max_duration=60.0,
        breached=False, warned=False,
    )


def _warn() -> SLAResult:
    return SLAResult(
        pipeline_name="pipe_warn", duration=50.0, max_duration=60.0,
        breached=False, warned=True,
    )


def _breach() -> SLAResult:
    return SLAResult(
        pipeline_name="pipe_breach", duration=90.0, max_duration=60.0,
        breached=True, warned=False,
    )


# ---------------------------------------------------------------------------
# _icon
# ---------------------------------------------------------------------------

def test_icon_ok():
    assert _icon(_ok()) == "✓"


def test_icon_warn():
    assert _icon(_warn()) == "!"


def test_icon_breach():
    assert _icon(_breach()) == "✗"


# ---------------------------------------------------------------------------
# format_sla_result
# ---------------------------------------------------------------------------

def test_format_sla_result_ok_contains_ok():
    assert "OK" in format_sla_result(_ok())


def test_format_sla_result_warn_contains_warning():
    assert "WARNING" in format_sla_result(_warn())


def test_format_sla_result_breach_contains_breached():
    assert "BREACHED" in format_sla_result(_breach())


def test_format_sla_result_contains_pipeline_name():
    assert "pipe_ok" in format_sla_result(_ok())


def test_format_sla_result_contains_duration():
    line = format_sla_result(_ok())
    assert "20.0" in line


# ---------------------------------------------------------------------------
# build_sla_report
# ---------------------------------------------------------------------------

def test_build_sla_report_empty():
    report = build_sla_report([])
    assert "No SLA data" in report


def test_build_sla_report_shows_counts():
    report = build_sla_report([_ok(), _warn(), _breach()])
    assert "OK: 1" in report
    assert "Warnings: 1" in report
    assert "Breached: 1" in report


def test_build_sla_report_sections_present():
    report = build_sla_report([_ok(), _warn(), _breach()])
    assert "BREACHED" in report
    assert "WARNING" in report
    assert "OK" in report


def test_build_sla_report_all_ok_no_breached_section():
    report = build_sla_report([_ok()])
    # BREACHED section header should not appear
    assert "--- BREACHED ---" not in report


def test_build_sla_report_pipeline_names_present():
    report = build_sla_report([_ok(), _breach()])
    assert "pipe_ok" in report
    assert "pipe_breach" in report
