"""Tests for incident detection and reporting."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List

import pytest

from pipe_sentinel.incident import Incident, detect_incidents, scan_all_incidents
from pipe_sentinel.incident_report import build_incident_report, format_incident


class _Rec:
    def __init__(self, pipeline: str, status: str, ts: datetime, error: str = ""):
        self.pipeline = pipeline
        self.status = status
        self.timestamp = ts.isoformat()
        self.error = error


BASE = datetime(2024, 1, 1, 12, 0, 0)


def _recs(specs):
    return [
        _Rec(pipeline=p, status=s, ts=BASE + timedelta(minutes=i), error=e)
        for i, (p, s, e) in enumerate(specs)
    ]


def test_no_records_returns_empty():
    assert detect_incidents([]) == []


def test_single_failure_creates_open_incident():
    recs = _recs([("etl", "failure", "oops")])
    incidents = detect_incidents(recs)
    assert len(incidents) == 1
    assert incidents[0].is_open
    assert incidents[0].failure_count == 1
    assert incidents[0].last_error == "oops"


def test_failure_then_success_closes_incident():
    recs = _recs([("etl", "failure", "err"), ("etl", "success", "")])
    incidents = detect_incidents(recs)
    assert len(incidents) == 1
    assert not incidents[0].is_open
    assert incidents[0].duration_seconds is not None
    assert incidents[0].duration_seconds == pytest.approx(60.0)


def test_consecutive_failures_merged_into_one_incident():
    recs = _recs(
        [("etl", "failure", "e1"), ("etl", "failure", "e2"), ("etl", "failure", "e3")]
    )
    incidents = detect_incidents(recs)
    assert len(incidents) == 1
    assert incidents[0].failure_count == 3
    assert incidents[0].last_error == "e3"


def test_two_separate_incidents():
    recs = _recs(
        [
            ("etl", "failure", "e1"),
            ("etl", "success", ""),
            ("etl", "failure", "e2"),
        ]
    )
    incidents = detect_incidents(recs)
    assert len(incidents) == 2


def test_scan_all_groups_by_pipeline():
    recs = _recs(
        [
            ("a", "failure", "ea"),
            ("b", "failure", "eb"),
        ]
    )
    incidents = scan_all_incidents(recs)
    pipelines = {i.pipeline for i in incidents}
    assert pipelines == {"a", "b"}


def test_incident_str_open():
    inc = Incident(
        pipeline="etl",
        started_at=BASE,
        ended_at=None,
        failure_count=2,
        last_error="timeout",
    )
    s = str(inc)
    assert "OPEN" in s
    assert "etl" in s


def test_build_report_no_incidents():
    report = build_incident_report([])
    assert "No incidents" in report


def test_build_report_shows_open_and_resolved():
    open_inc = Incident("p1", BASE, None, 1, "err")
    closed_inc = Incident("p2", BASE, BASE + timedelta(minutes=5), 2, "err2")
    report = build_incident_report([open_inc, closed_inc])
    assert "OPEN" in report
    assert "RESOLVED" in report
    assert "p1" in report
    assert "p2" in report
