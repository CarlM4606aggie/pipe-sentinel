"""Tests for pipe_sentinel.cascade_report."""
from __future__ import annotations

from pipe_sentinel.cascade import CascadeResult, CascadeReport
from pipe_sentinel.cascade_report import (
    format_cascade_result,
    build_cascade_report,
)


def _cascade(pipeline: str, failed: bool, upstream: list[str] | None = None) -> CascadeResult:
    return CascadeResult(
        pipeline=pipeline,
        failed=failed,
        upstream_failures=upstream or [],
    )


# ---------------------------------------------------------------------------
# format_cascade_result
# ---------------------------------------------------------------------------

def test_format_cascade_result_shows_pipeline_name():
    r = _cascade("etl_load", failed=True, upstream=["etl_extract"])
    text = format_cascade_result(r)
    assert "etl_load" in text


def test_format_cascade_result_shows_upstream():
    r = _cascade("etl_load", failed=True, upstream=["etl_extract"])
    text = format_cascade_result(r)
    assert "etl_extract" in text


def test_format_cascade_result_isolated_label():
    r = _cascade("etl_load", failed=True, upstream=[])
    text = format_cascade_result(r)
    assert "isolated" in text.lower()


def test_format_cascade_result_ok_no_failure_label():
    r = _cascade("etl_load", failed=False)
    text = format_cascade_result(r)
    assert "isolated" not in text.lower()
    assert "cascade" not in text.lower()


# ---------------------------------------------------------------------------
# build_cascade_report
# ---------------------------------------------------------------------------

def test_build_cascade_report_header_present():
    report = CascadeReport(results=[])
    text = build_cascade_report(report)
    assert "Cascade" in text


def test_build_cascade_report_counts_in_summary():
    results = [
        _cascade("a", failed=True, upstream=["b"]),
        _cascade("b", failed=True),
        _cascade("c", failed=False),
    ]
    report = CascadeReport(results=results)
    text = build_cascade_report(report)
    assert "Cascades: 1" in text
    assert "Isolated failures: 1" in text


def test_build_cascade_report_shows_cascade_section():
    results = [_cascade("a", failed=True, upstream=["b"])]
    report = CascadeReport(results=results)
    text = build_cascade_report(report)
    assert "Cascade failures" in text


def test_build_cascade_report_shows_isolated_section():
    results = [_cascade("a", failed=True, upstream=[])]
    report = CascadeReport(results=results)
    text = build_cascade_report(report)
    assert "Isolated failures" in text


def test_build_cascade_report_shows_passing_section():
    results = [_cascade("a", failed=False)]
    report = CascadeReport(results=results)
    text = build_cascade_report(report)
    assert "Passing" in text


def test_build_cascade_report_empty_report():
    report = CascadeReport(results=[])
    text = build_cascade_report(report)
    assert "Pipelines: 0" in text
