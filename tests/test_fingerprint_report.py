"""Tests for pipe_sentinel.fingerprint_report."""
import pytest
from pipe_sentinel.fingerprint import FingerprintResult, FingerprintReport
from pipe_sentinel.fingerprint_report import (
    _icon,
    format_fingerprint_result,
    build_fingerprint_report,
)


def _make(pipeline="pipe", recurring=False, occurrences=1, stderr="err") -> FingerprintResult:
    return FingerprintResult(
        pipeline=pipeline,
        fingerprint="abcdef1234567890" * 4,
        sample_stderr=stderr,
        occurrences=occurrences,
        is_recurring=recurring,
    )


# ---------------------------------------------------------------------------
# _icon
# ---------------------------------------------------------------------------

def test_icon_recurring():
    assert _icon(_make(recurring=True)) == "🔁"


def test_icon_new():
    assert _icon(_make(recurring=False)) == "🆕"


# ---------------------------------------------------------------------------
# format_fingerprint_result
# ---------------------------------------------------------------------------

def test_format_contains_pipeline_name():
    text = format_fingerprint_result(_make(pipeline="my_pipe"))
    assert "my_pipe" in text


def test_format_contains_truncated_fingerprint():
    r = _make()
    text = format_fingerprint_result(r)
    assert r.fingerprint[:16] in text


def test_format_contains_occurrences():
    text = format_fingerprint_result(_make(occurrences=5))
    assert "5" in text


def test_format_contains_stderr_snippet():
    text = format_fingerprint_result(_make(stderr="division by zero"))
    assert "division by zero" in text


def test_format_empty_stderr_omits_line():
    text = format_fingerprint_result(_make(stderr=""))
    assert "stderr" not in text


# ---------------------------------------------------------------------------
# build_fingerprint_report
# ---------------------------------------------------------------------------

def test_build_report_empty():
    report = FingerprintReport(results=[])
    text = build_fingerprint_report(report)
    assert "No failure fingerprints" in text


def test_build_report_shows_total():
    report = FingerprintReport(results=[_make(), _make(pipeline="b", recurring=True)])
    text = build_fingerprint_report(report)
    assert "total=2" in text


def test_build_report_shows_recurring_count():
    report = FingerprintReport(results=[_make(recurring=True), _make()])
    text = build_fingerprint_report(report)
    assert "recurring=1" in text


def test_build_report_shows_new_count():
    report = FingerprintReport(results=[_make(recurring=True), _make()])
    text = build_fingerprint_report(report)
    assert "new=1" in text


def test_build_report_contains_each_pipeline():
    report = FingerprintReport(results=[_make(pipeline="alpha"), _make(pipeline="beta")])
    text = build_fingerprint_report(report)
    assert "alpha" in text
    assert "beta" in text
