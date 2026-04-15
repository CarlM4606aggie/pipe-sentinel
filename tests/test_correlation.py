"""Tests for pipe_sentinel.correlation and correlation_report."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest

from pipe_sentinel.correlation import (
    CorrelationPair,
    CorrelationReport,
    _failed_names,
    _group_by_window,
    detect_correlations,
)
from pipe_sentinel.correlation_report import (
    build_correlation_report,
    format_pair,
)


@dataclass
class _Rec:
    pipeline: str
    status: str
    started_at: float
    duration: float = 1.0
    exit_code: int = 0
    error: Optional[str] = None


def _recs(*items):
    return [_Rec(*i) for i in items]


# ---------------------------------------------------------------------------
# _failed_names
# ---------------------------------------------------------------------------

def test_failed_names_returns_only_failures():
    window = [
        _Rec("a", "success", 0),
        _Rec("b", "failure", 1),
        _Rec("c", "failure", 2),
    ]
    assert sorted(_failed_names(window)) == ["b", "c"]


def test_failed_names_empty_window():
    assert _failed_names([]) == []


# ---------------------------------------------------------------------------
# _group_by_window
# ---------------------------------------------------------------------------

def test_group_by_window_single_bucket():
    recs = _recs(("a", "failure", 0), ("b", "failure", 100))
    groups = _group_by_window(recs, window_seconds=200)
    assert len(groups) == 1
    assert len(groups[0]) == 2


def test_group_by_window_splits_on_gap():
    recs = _recs(("a", "failure", 0), ("b", "failure", 1000))
    groups = _group_by_window(recs, window_seconds=200)
    assert len(groups) == 2


def test_group_by_window_empty():
    assert _group_by_window([], 300) == []


# ---------------------------------------------------------------------------
# detect_correlations
# ---------------------------------------------------------------------------

def test_detect_correlations_no_records():
    report = detect_correlations([])
    assert report.pairs == []
    assert report.significant == []


def test_detect_correlations_single_co_failure():
    recs = _recs(
        ("pipe_a", "failure", 0),
        ("pipe_b", "failure", 10),
    )
    report = detect_correlations(recs, window_seconds=60, threshold=0.5)
    assert len(report.pairs) == 1
    pair = report.pairs[0]
    assert pair.pipeline_a == "pipe_a"
    assert pair.pipeline_b == "pipe_b"
    assert pair.co_failures == 1
    assert pair.rate == 1.0


def test_detect_correlations_no_co_failure_when_separate_windows():
    recs = _recs(
        ("pipe_a", "failure", 0),
        ("pipe_b", "failure", 10000),
    )
    report = detect_correlations(recs, window_seconds=60)
    assert report.pairs == []


def test_detect_correlations_significant_filter():
    recs = _recs(
        ("a", "failure", 0), ("b", "failure", 5),
        ("a", "failure", 1000), ("c", "failure", 1005),
    )
    report = detect_correlations(recs, window_seconds=60, threshold=0.9)
    # a+b and a+c each appear once out of 2 windows => rate 0.5 < 0.9
    assert report.significant == []


def test_detect_correlations_rate_calculation():
    recs = _recs(
        ("x", "failure", 0), ("y", "failure", 1),
        ("x", "failure", 1000), ("y", "failure", 1001),
    )
    report = detect_correlations(recs, window_seconds=60, threshold=0.5)
    assert len(report.pairs) == 1
    assert report.pairs[0].rate == pytest.approx(1.0)
    assert len(report.significant) == 1


# ---------------------------------------------------------------------------
# CorrelationPair helpers
# ---------------------------------------------------------------------------

def test_pair_rate_zero_when_no_windows():
    pair = CorrelationPair("a", "b", co_failures=0, total_windows=0)
    assert pair.rate == 0.0


def test_pair_str_includes_names():
    pair = CorrelationPair("alpha", "beta", co_failures=3, total_windows=5)
    s = str(pair)
    assert "alpha" in s
    assert "beta" in s


# ---------------------------------------------------------------------------
# correlation_report
# ---------------------------------------------------------------------------

def test_format_pair_contains_names():
    pair = CorrelationPair("p1", "p2", co_failures=2, total_windows=4)
    text = format_pair(pair, threshold=0.5)
    assert "p1" in text
    assert "p2" in text
    assert "50.0%" in text


def test_build_correlation_report_empty():
    report = CorrelationReport(pairs=[], threshold=0.5)
    text = build_correlation_report(report)
    assert "No co-failure data" in text


def test_build_correlation_report_shows_significant():
    pair = CorrelationPair("a", "b", co_failures=4, total_windows=4)
    report = CorrelationReport(pairs=[pair], threshold=0.5)
    text = build_correlation_report(report)
    assert "Significant" in text
    assert "a/b" in text


def test_build_correlation_report_no_significant_message():
    pair = CorrelationPair("a", "b", co_failures=1, total_windows=10)
    report = CorrelationReport(pairs=[pair], threshold=0.5)
    text = build_correlation_report(report)
    assert "No significant" in text
