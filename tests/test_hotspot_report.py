"""Tests for pipe_sentinel.hotspot_report."""
from __future__ import annotations

import pytest

from pipe_sentinel.hotspot import HotspotResult
from pipe_sentinel.hotspot_report import (
    _icon,
    build_hotspot_report,
    format_hotspot_result,
)


def _make(rate: float, failures: int = 3, total: int = 10) -> HotspotResult:
    return HotspotResult(
        pipeline="test_pipe",
        total_runs=total,
        failures=failures,
        failure_rate=rate,
    )


# --- _icon ---

def test_icon_critical():
    assert _icon(_make(0.80)) == "\U0001f525"


def test_icon_warning():
    assert _icon(_make(0.50)) == "\u26a0\ufe0f "


def test_icon_low():
    assert _icon(_make(0.20)) == "\U0001f7e1"


def test_icon_boundary_warning():
    assert _icon(_make(0.40)) == "\u26a0\ufe0f "


# --- format_hotspot_result ---

def test_format_contains_pipeline_name():
    r = _make(0.5)
    assert "test_pipe" in format_hotspot_result(r)


def test_format_contains_fraction():
    r = _make(0.5, failures=5, total=10)
    assert "5/10" in format_hotspot_result(r)


def test_format_contains_percentage():
    r = _make(0.5, failures=5, total=10)
    assert "50.0%" in format_hotspot_result(r)


# --- build_hotspot_report ---

def test_build_report_empty():
    report = build_hotspot_report([])
    assert "No hotspots" in report


def test_build_report_shows_count():
    results = [_make(0.6), _make(0.3)]
    results[1] = HotspotResult(
        pipeline="other_pipe", total_runs=10, failures=3, failure_rate=0.3
    )
    report = build_hotspot_report(results)
    assert "2 pipeline" in report


def test_build_report_lists_all_pipelines():
    r1 = HotspotResult("alpha", 10, 7, 0.7)
    r2 = HotspotResult("beta", 5, 2, 0.4)
    report = build_hotspot_report([r1, r2])
    assert "alpha" in report
    assert "beta" in report
