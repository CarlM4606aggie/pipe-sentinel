"""Tests for pipe_sentinel.flap and pipe_sentinel.flap_report."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List

import pytest

from pipe_sentinel.flap import (
    FlapResult,
    _count_transitions,
    detect_flap,
    scan_flaps,
)
from pipe_sentinel.flap_report import (
    format_flap_result,
    build_flap_report,
)


@dataclass
class _Rec:
    pipeline_name: str
    status: str
    started_at: float


def _recs(pattern: str, name: str = "pipe") -> List[_Rec]:
    """Build records from a string like 'PFFPFP'."""
    return [
        _Rec(pipeline_name=name, status="success" if c == "P" else "failure", started_at=float(i))
        for i, c in enumerate(pattern)
    ]


def test_count_transitions_empty():
    assert _count_transitions([]) == 0


def test_count_transitions_single():
    assert _count_transitions(_recs("P")) == 0


def test_count_transitions_all_same():
    assert _count_transitions(_recs("PPPP")) == 0


def test_count_transitions_alternating():
    assert _count_transitions(_recs("PFPFPF")) == 5


def test_count_transitions_one_change():
    assert _count_transitions(_recs("PPPFFF")) == 1


def test_detect_flap_not_flapping():
    result = detect_flap("pipe", _recs("PPPPPP"), window=10, threshold=4)
    assert not result.is_flapping
    assert result.transitions == 0


def test_detect_flap_is_flapping():
    result = detect_flap("pipe", _recs("PFPFPFPF"), window=10, threshold=4)
    assert result.is_flapping
    assert result.transitions == 7


def test_detect_flap_respects_window():
    # Only last 4 records: PPPP — stable
    recs = _recs("PFPFPPPP")
    result = detect_flap("pipe", recs, window=4, threshold=2)
    assert not result.is_flapping
    assert result.window_size == 4


def test_scan_flaps_groups_by_pipeline():
    recs = _recs("PFPFPF", "alpha") + _recs("PPPPPP", "beta")
    results = scan_flaps(recs, window=10, threshold=4)
    assert len(results) == 2
    by_name = {r.pipeline_name: r for r in results}
    assert by_name["alpha"].is_flapping
    assert not by_name["beta"].is_flapping


def test_scan_flaps_empty():
    assert scan_flaps([]) == []


def test_format_flap_result_flapping():
    r = FlapResult("my_pipe", transitions=5, window_size=8, is_flapping=True, threshold=4)
    out = format_flap_result(r)
    assert "FLAPPING" in out
    assert "my_pipe" in out
    assert "5 transitions" in out


def test_format_flap_result_stable():
    r = FlapResult("my_pipe", transitions=1, window_size=8, is_flapping=False, threshold=4)
    out = format_flap_result(r)
    assert "stable" in out


def test_build_flap_report_no_data():
    assert "No pipeline data" in build_flap_report([])


def test_build_flap_report_summary_count():
    results = [
        FlapResult("a", 5, 8, True, 4),
        FlapResult("b", 1, 8, False, 4),
    ]
    report = build_flap_report(results)
    assert "1/2 flapping" in report
