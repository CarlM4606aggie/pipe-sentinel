"""Tests for pipe_sentinel.cascade."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from pipe_sentinel.cascade import (
    CascadeResult,
    CascadeReport,
    _failed_names,
    detect_cascade,
    scan_cascades,
)
from pipe_sentinel.dependency import DependencyGraph


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rec(pipeline: str, status: str, ts: str = "2024-01-01T00:00:00"):
    r = MagicMock()
    r.pipeline = pipeline
    r.status = status
    r.timestamp = ts
    return r


def _graph(*edges: tuple[str, str]) -> DependencyGraph:
    """Build a DependencyGraph from (upstream, downstream) pairs."""
    g = DependencyGraph()
    names: set[str] = set()
    for u, d in edges:
        names.add(u)
        names.add(d)
    for name in names:
        g.add(MagicMock(name=name, depends_on=[]))
    for u, d in edges:
        downstream = MagicMock(name=d, depends_on=[u])
        g.add(downstream)
    return g


# ---------------------------------------------------------------------------
# _failed_names
# ---------------------------------------------------------------------------

def test_failed_names_all_success():
    records = [_rec("a", "success"), _rec("b", "success")]
    result = _failed_names(records)
    assert result == {"a": False, "b": False}


def test_failed_names_mixed():
    records = [_rec("a", "success"), _rec("b", "failure")]
    result = _failed_names(records)
    assert result["b"] is True
    assert result["a"] is False


def test_failed_names_uses_latest_record():
    records = [
        _rec("a", "failure", "2024-01-01T00:00:00"),
        _rec("a", "success", "2024-01-02T00:00:00"),
    ]
    result = _failed_names(records)
    assert result["a"] is False  # latest was success


# ---------------------------------------------------------------------------
# CascadeResult
# ---------------------------------------------------------------------------

def test_is_cascade_true_when_failed_and_upstream_failed():
    r = CascadeResult(pipeline="b", failed=True, upstream_failures=["a"])
    assert r.is_cascade is True


def test_is_cascade_false_when_not_failed():
    r = CascadeResult(pipeline="b", failed=False, upstream_failures=["a"])
    assert r.is_cascade is False


def test_is_cascade_false_when_no_upstream_failures():
    r = CascadeResult(pipeline="b", failed=True, upstream_failures=[])
    assert r.is_cascade is False


def test_str_cascade():
    r = CascadeResult(pipeline="b", failed=True, upstream_failures=["a"])
    assert "CASCADE" in str(r)
    assert "a" in str(r)


def test_str_isolated():
    r = CascadeResult(pipeline="b", failed=True, upstream_failures=[])
    assert "ISOLATED" in str(r)


def test_str_ok():
    r = CascadeResult(pipeline="b", failed=False)
    assert "OK" in str(r)


# ---------------------------------------------------------------------------
# detect_cascade
# ---------------------------------------------------------------------------

def test_detect_cascade_identifies_cascade():
    g = DependencyGraph()
    a = MagicMock(name="a", depends_on=[])
    b = MagicMock(name="b", depends_on=["a"])
    g.add(a)
    g.add(b)
    failed_map = {"a": True, "b": True}
    result = detect_cascade("b", g, failed_map)
    assert result.is_cascade
    assert "a" in result.upstream_failures


def test_detect_cascade_no_cascade_when_upstream_ok():
    g = DependencyGraph()
    a = MagicMock(name="a", depends_on=[])
    b = MagicMock(name="b", depends_on=["a"])
    g.add(a)
    g.add(b)
    failed_map = {"a": False, "b": True}
    result = detect_cascade("b", g, failed_map)
    assert not result.is_cascade
    assert result.failed


# ---------------------------------------------------------------------------
# scan_cascades
# ---------------------------------------------------------------------------

def test_scan_cascades_returns_report_for_all_nodes():
    g = DependencyGraph()
    a = MagicMock(name="a", depends_on=[])
    b = MagicMock(name="b", depends_on=["a"])
    g.add(a)
    g.add(b)
    records = [_rec("a", "failure"), _rec("b", "failure")]
    report = scan_cascades(g, records)
    names = {r.pipeline for r in report.results}
    assert "a" in names
    assert "b" in names


def test_scan_cascades_has_cascades_flag():
    g = DependencyGraph()
    a = MagicMock(name="a", depends_on=[])
    b = MagicMock(name="b", depends_on=["a"])
    g.add(a)
    g.add(b)
    records = [_rec("a", "failure"), _rec("b", "failure")]
    report = scan_cascades(g, records)
    assert report.has_cascades


def test_scan_cascades_no_cascades_when_all_pass():
    g = DependencyGraph()
    a = MagicMock(name="a", depends_on=[])
    b = MagicMock(name="b", depends_on=["a"])
    g.add(a)
    g.add(b)
    records = [_rec("a", "success"), _rec("b", "success")]
    report = scan_cascades(g, records)
    assert not report.has_cascades
