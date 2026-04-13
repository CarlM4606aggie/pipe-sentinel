"""Tests for pipe_sentinel.anomaly and pipe_sentinel.anomaly_report."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pytest

from pipe_sentinel.anomaly import (
    AnomalyResult,
    _failure_rate,
    detect_anomaly,
    scan_anomalies,
)
from pipe_sentinel.anomaly_report import build_anomaly_report, format_anomaly_result


# ---------------------------------------------------------------------------
# Minimal stub for AuditRecord so we don't need a real DB
# ---------------------------------------------------------------------------
@dataclass
class _Rec:
    pipeline_name: str
    status: str


def _recs(name: str, statuses: List[str]) -> List[_Rec]:
    return [_Rec(pipeline_name=name, status=s) for s in statuses]


# ---------------------------------------------------------------------------
# _failure_rate
# ---------------------------------------------------------------------------

def test_failure_rate_empty():
    assert _failure_rate([]) == 0.0


def test_failure_rate_all_success():
    records = _recs("p", ["success"] * 4)
    assert _failure_rate(records) == 0.0


def test_failure_rate_all_failed():
    records = _recs("p", ["failure"] * 4)
    assert _failure_rate(records) == 1.0


def test_failure_rate_mixed():
    records = _recs("p", ["success", "failure", "success", "failure"])
    assert _failure_rate(records) == 0.5


# ---------------------------------------------------------------------------
# detect_anomaly
# ---------------------------------------------------------------------------

def test_detect_anomaly_returns_none_when_insufficient_records():
    records = _recs("etl", ["failure"] * 3)
    result = detect_anomaly("etl", records, short_window=5)
    assert result is None


def test_detect_anomaly_no_anomaly_when_rates_stable():
    # 20 records, 2 failures spread evenly — no spike
    statuses = ["success"] * 18 + ["failure"] * 2
    records = _recs("etl", statuses)
    result = detect_anomaly("etl", records, short_window=5, long_window=20, spike_threshold=2.0)
    assert result is not None
    assert result.is_anomaly is False


def test_detect_anomaly_flags_spike():
    # Long history mostly passing, then 5 recent failures
    good = _recs("etl", ["success"] * 15)
    bad = _recs("etl", ["failure"] * 5)
    records = good + bad
    result = detect_anomaly("etl", records, short_window=5, long_window=20, spike_threshold=2.0)
    assert result is not None
    assert result.is_anomaly is True
    assert result.recent_failure_rate == 1.0


def test_detect_anomaly_spike_ratio_inf_when_baseline_zero():
    # Baseline all success, recent has failures
    good = _recs("etl", ["success"] * 15)
    bad = _recs("etl", ["failure"] * 5)
    records = good + bad
    result = detect_anomaly("etl", records, short_window=5, long_window=20)
    assert result is not None
    assert result.spike_ratio == float("inf")


# ---------------------------------------------------------------------------
# scan_anomalies
# ---------------------------------------------------------------------------

def test_scan_anomalies_multiple_pipelines():
    healthy = _recs("load", ["success"] * 20)
    spiking_good = _recs("transform", ["success"] * 15)
    spiking_bad = _recs("transform", ["failure"] * 5)
    all_records = healthy + spiking_good + spiking_bad
    results = scan_anomalies(all_records, short_window=5, long_window=20)
    names = {r.pipeline_name for r in results}
    assert "load" in names
    assert "transform" in names
    transform_result = next(r for r in results if r.pipeline_name == "transform")
    assert transform_result.is_anomaly is True


# ---------------------------------------------------------------------------
# anomaly_report
# ---------------------------------------------------------------------------

def test_build_report_no_anomalies():
    results = [
        AnomalyResult("p", 0.0, 0.1, 0.0, False),
    ]
    report = build_anomaly_report(results)
    assert "Anomalies flagged : 0" in report
    assert "Healthy" in report


def test_build_report_with_anomaly():
    results = [
        AnomalyResult("p", 1.0, 0.1, 10.0, True),
    ]
    report = build_anomaly_report(results)
    assert "Anomalies flagged : 1" in report
    assert "Anomalies" in report


def test_build_report_empty_list():
    report = build_anomaly_report([])
    assert report == "No anomalies detected."


def test_format_anomaly_result_anomaly_symbol():
    r = AnomalyResult("pipe", 0.8, 0.1, 8.0, True)
    text = format_anomaly_result(r)
    assert "🚨" in text
    assert "pipe" in text


def test_format_anomaly_result_ok_symbol():
    r = AnomalyResult("pipe", 0.1, 0.1, 1.0, False)
    text = format_anomaly_result(r)
    assert "✅" in text
