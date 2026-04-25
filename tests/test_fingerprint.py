"""Tests for pipe_sentinel.fingerprint."""
import pytest
from pipe_sentinel.fingerprint import (
    _normalise,
    compute_fingerprint,
    detect_fingerprint,
    scan_fingerprints,
    FingerprintResult,
    FingerprintReport,
)


STDERR_A = "Error on line 42: division by zero"
STDERR_A2 = "Error on line 99: division by zero"  # same after normalise
STDERR_B = "KeyError: 'missing_key' at 0xDEADBEEF"


# ---------------------------------------------------------------------------
# _normalise
# ---------------------------------------------------------------------------

def test_normalise_strips_line_numbers():
    assert "line N" in _normalise("Error on line 42")


def test_normalise_strips_hex_addresses():
    assert "0xADDR" in _normalise("ptr=0xDEADBEEF")


def test_normalise_strips_timestamps():
    result = _normalise("2024-01-15T08:30:00 something failed")
    assert "TIMESTAMP" in result
    assert "2024" not in result


# ---------------------------------------------------------------------------
# compute_fingerprint
# ---------------------------------------------------------------------------

def test_fingerprint_same_for_normalised_equal_strings():
    fp1 = compute_fingerprint(STDERR_A)
    fp2 = compute_fingerprint(STDERR_A2)
    assert fp1 == fp2


def test_fingerprint_differs_for_different_errors():
    fp1 = compute_fingerprint(STDERR_A)
    fp2 = compute_fingerprint(STDERR_B)
    assert fp1 != fp2


def test_fingerprint_is_hex_string():
    fp = compute_fingerprint("some error")
    assert len(fp) == 64
    int(fp, 16)  # must be valid hex


# ---------------------------------------------------------------------------
# detect_fingerprint
# ---------------------------------------------------------------------------

def test_detect_fingerprint_new_when_no_history():
    result = detect_fingerprint("pipe_a", STDERR_A, {})
    assert result.occurrences == 1
    assert not result.is_recurring


def test_detect_fingerprint_recurring_at_threshold():
    fp = compute_fingerprint(STDERR_A)
    result = detect_fingerprint("pipe_a", STDERR_A, {fp: 1}, recurrence_threshold=2)
    assert result.is_recurring
    assert result.occurrences == 2


def test_detect_fingerprint_stores_pipeline_name():
    result = detect_fingerprint("my_pipeline", STDERR_A, {})
    assert result.pipeline == "my_pipeline"


def test_detect_fingerprint_truncates_sample_stderr():
    long_stderr = "x" * 500
    result = detect_fingerprint("p", long_stderr, {})
    assert len(result.sample_stderr) <= 200


# ---------------------------------------------------------------------------
# scan_fingerprints
# ---------------------------------------------------------------------------

def test_scan_fingerprints_empty_input():
    report = scan_fingerprints([])
    assert isinstance(report, FingerprintReport)
    assert report.results == []


def test_scan_fingerprints_counts_results():
    failures = [
        {"pipeline": "a", "stderr": STDERR_A},
        {"pipeline": "b", "stderr": STDERR_B},
    ]
    report = scan_fingerprints(failures)
    assert len(report.results) == 2


def test_scan_fingerprints_recurring_group():
    fp = compute_fingerprint(STDERR_A)
    failures = [
        {"pipeline": "a", "stderr": STDERR_A},
    ]
    report = scan_fingerprints(failures, history={fp: 1}, recurrence_threshold=2)
    assert len(report.recurring) == 1
    assert len(report.new_failures) == 0


def test_fingerprint_report_str_contains_tag():
    r = FingerprintResult(
        pipeline="p", fingerprint="abc123", sample_stderr="err",
        occurrences=3, is_recurring=True,
    )
    assert "RECURRING" in str(r)


def test_fingerprint_report_str_new_tag():
    r = FingerprintResult(
        pipeline="p", fingerprint="abc123", sample_stderr="err",
        occurrences=1, is_recurring=False,
    )
    assert "NEW" in str(r)
