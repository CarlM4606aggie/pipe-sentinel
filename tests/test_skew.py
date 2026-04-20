"""Tests for pipe_sentinel.skew."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import List

import pytest

from pipe_sentinel.skew import SkewResult, _timestamps, detect_skew, scan_skew


class _Rec:
    """Minimal AuditRecord stand-in."""

    def __init__(self, ts: str, status: str = "success"):
        self.timestamp = ts
        self.status = status
        self.pipeline_name = "p"
        self.duration = 1.0
        self.message = ""


def _ts(offset_seconds: float) -> str:
    """Return ISO timestamp offset from a fixed base."""
    base = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return (base + timedelta(seconds=offset_seconds)).isoformat()


# ---------------------------------------------------------------------------
# _timestamps
# ---------------------------------------------------------------------------

def test_timestamps_empty():
    assert _timestamps([]) == []


def test_timestamps_sorted():
    recs = [_Rec(_ts(300)), _Rec(_ts(0)), _Rec(_ts(150))]
    ts = _timestamps(recs)
    assert ts == sorted(ts)
    assert len(ts) == 3


def test_timestamps_skips_invalid():
    recs = [_Rec("not-a-date"), _Rec(_ts(0))]
    ts = _timestamps(recs)
    assert len(ts) == 1


# ---------------------------------------------------------------------------
# detect_skew
# ---------------------------------------------------------------------------

def test_detect_skew_returns_none_for_single_record():
    assert detect_skew("p", [_Rec(_ts(0))], 3600) is None


def test_detect_skew_returns_none_for_empty():
    assert detect_skew("p", [], 3600) is None


def test_detect_skew_on_time():
    recs = [_Rec(_ts(0)), _Rec(_ts(3600))]
    result = detect_skew("p", recs, expected_interval_seconds=3600, tolerance_fraction=0.1)
    assert result is not None
    assert result.is_skewed is False
    assert abs(result.skew_seconds) < 1


def test_detect_skew_late():
    # 5400s actual vs 3600s expected → 1800s late, >10% tolerance
    recs = [_Rec(_ts(0)), _Rec(_ts(5400))]
    result = detect_skew("p", recs, expected_interval_seconds=3600, tolerance_fraction=0.1)
    assert result is not None
    assert result.is_skewed is True
    assert result.skew_seconds > 0


def test_detect_skew_early():
    # 1800s actual vs 3600s expected → 1800s early
    recs = [_Rec(_ts(0)), _Rec(_ts(1800))]
    result = detect_skew("p", recs, expected_interval_seconds=3600, tolerance_fraction=0.1)
    assert result is not None
    assert result.is_skewed is True
    assert result.skew_seconds < 0


def test_detect_skew_uses_most_recent_gap():
    # Three records; only the last gap should be used
    recs = [_Rec(_ts(0)), _Rec(_ts(100)), _Rec(_ts(3700))]
    result = detect_skew("p", recs, expected_interval_seconds=3600, tolerance_fraction=0.05)
    assert result is not None
    assert pytest.approx(result.actual_interval_seconds, abs=1) == 3600
    assert result.is_skewed is False


def test_detect_skew_invalid_interval_raises():
    with pytest.raises(ValueError, match="expected_interval_seconds"):
        detect_skew("p", [], expected_interval_seconds=0)


def test_detect_skew_invalid_tolerance_raises():
    with pytest.raises(ValueError, match="tolerance_fraction"):
        detect_skew("p", [], expected_interval_seconds=60, tolerance_fraction=0.0)


# ---------------------------------------------------------------------------
# scan_skew
# ---------------------------------------------------------------------------

class _FakePipeline:
    def __init__(self, name: str, interval: float | None):
        self.name = name
        self.expected_interval_seconds = interval


def test_scan_skew_skips_pipelines_without_interval():
    pipelines = [_FakePipeline("no_interval", None)]
    results = scan_skew(pipelines, {})
    assert results == []


def test_scan_skew_skips_pipelines_with_insufficient_records():
    pipelines = [_FakePipeline("p", 3600)]
    results = scan_skew(pipelines, {"p": [_Rec(_ts(0))]})
    assert results == []


def test_scan_skew_returns_results_for_valid_pipelines():
    pipelines = [
        _FakePipeline("on_time", 3600),
        _FakePipeline("late", 3600),
    ]
    records = {
        "on_time": [_Rec(_ts(0)), _Rec(_ts(3600))],
        "late": [_Rec(_ts(0)), _Rec(_ts(7200))],
    }
    results = scan_skew(pipelines, records, tolerance_fraction=0.1)
    assert len(results) == 2
    names = {r.pipeline_name for r in results}
    assert names == {"on_time", "late"}
    late = next(r for r in results if r.pipeline_name == "late")
    assert late.is_skewed is True
