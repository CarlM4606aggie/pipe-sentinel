"""Tests for pipe_sentinel.burndown_report."""
from __future__ import annotations

from pipe_sentinel.burndown import BurndownResult
from pipe_sentinel.burndown_report import (
    _bar,
    _icon,
    build_burndown_report,
    format_burndown_result,
)


def _make(
    pipeline: str = "pipe",
    total: int = 4,
    resolved: int = 2,
) -> BurndownResult:
    remaining = total - resolved
    rate = resolved / total if total else 0.0
    return BurndownResult(
        pipeline=pipeline,
        total_failures=total,
        resolved=resolved,
        remaining=remaining,
        burn_rate=rate,
        is_clear=(remaining == 0),
    )


# ── _bar ──────────────────────────────────────────────────────────────────────

def test_bar_empty():
    assert _bar(0.0, width=10) == "[----------]"


def test_bar_full():
    assert _bar(1.0, width=10) == "[##########]"


def test_bar_half():
    result = _bar(0.5, width=10)
    assert result == "[#####-----]"


# ── _icon ─────────────────────────────────────────────────────────────────────

def test_icon_clear():
    r = _make(resolved=4)
    assert _icon(r) == "✅"


def test_icon_half_resolved():
    r = _make(total=4, resolved=2)
    assert _icon(r) == "🔶"


def test_icon_unresolved():
    r = _make(total=4, resolved=0)
    assert _icon(r) == "🔴"


# ── format_burndown_result ────────────────────────────────────────────────────

def test_format_contains_pipeline_name():
    r = _make(pipeline="etl_daily")
    assert "etl_daily" in format_burndown_result(r)


def test_format_contains_percentage():
    r = _make(total=4, resolved=2)
    text = format_burndown_result(r)
    assert "50.0%" in text


def test_format_contains_counts():
    r = _make(total=6, resolved=3)
    text = format_burndown_result(r)
    assert "6" in text and "3" in text


# ── build_burndown_report ─────────────────────────────────────────────────────

def test_build_report_empty():
    report = build_burndown_report([])
    assert "No pipeline failures" in report


def test_build_report_contains_header():
    results = [_make(pipeline="p1"), _make(pipeline="p2")]
    report = build_burndown_report(results)
    assert "Burndown Report" in report
    assert "2 pipeline(s)" in report


def test_build_report_contains_each_pipeline():
    results = [_make(pipeline="alpha"), _make(pipeline="beta")]
    report = build_burndown_report(results)
    assert "alpha" in report
    assert "beta" in report
