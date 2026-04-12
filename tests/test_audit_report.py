"""Tests for pipe_sentinel.audit_report."""

import pytest
from pipe_sentinel.audit import AuditRecord
from pipe_sentinel.audit_report import (
    _status_symbol,
    format_record,
    build_report,
    print_report,
)


@pytest.fixture
def success_record() -> AuditRecord:
    return AuditRecord(
        id=1,
        pipeline_name="ingest_orders",
        status="success",
        ran_at="2024-06-01T10:00:00",
        duration=3.14,
        retries=0,
        error=None,
    )


@pytest.fixture
def failure_record() -> AuditRecord:
    return AuditRecord(
        id=2,
        pipeline_name="transform_users",
        status="failure",
        ran_at="2024-06-01T10:05:00",
        duration=1.0,
        retries=2,
        error="Connection refused",
    )


def test_status_symbol_success():
    assert _status_symbol("success") == "✓"


def test_status_symbol_failure():
    assert _status_symbol("failure") == "✗"


def test_status_symbol_unknown():
    assert _status_symbol("unknown") == "✗"


def test_format_record_success(success_record):
    result = format_record(success_record)
    assert "✓" in result
    assert "ingest_orders" in result
    assert "3.14s" in result
    assert "retries=0" in result
    assert "error" not in result


def test_format_record_failure(failure_record):
    result = format_record(failure_record)
    assert "✗" in result
    assert "transform_users" in result
    assert "retries=2" in result
    assert "Connection refused" in result


def test_format_record_no_duration():
    record = AuditRecord(
        id=3,
        pipeline_name="load_events",
        status="failure",
        ran_at="2024-06-01T11:00:00",
        duration=None,
        retries=1,
        error="Timeout",
    )
    result = format_record(record)
    assert "N/A" in result


def test_build_report_empty():
    assert build_report([]) == "No audit records found."


def test_build_report_summary(success_record, failure_record):
    report = build_report([success_record, failure_record])
    assert "Total: 2" in report
    assert "Passed: 1" in report
    assert "Failed: 1" in report


def test_build_report_contains_records(success_record, failure_record):
    report = build_report([success_record, failure_record])
    assert "ingest_orders" in report
    assert "transform_users" in report


def test_print_report_outputs(capsys, success_record):
    print_report([success_record])
    captured = capsys.readouterr()
    assert "ingest_orders" in captured.out
    assert "Pipe Sentinel Audit Report" in captured.out
