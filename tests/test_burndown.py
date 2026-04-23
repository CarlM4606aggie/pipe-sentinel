"""Tests for pipe_sentinel.burndown."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest

from pipe_sentinel.burndown import (
    BurndownResult,
    _resolved_failures,
    compute_burndown,
    scan_burndowns,
)


@dataclass
class _Rec:
    pipeline: str
    status: str


def _recs(statuses: list[str], name: str = "p") -> list[_Rec]:
    return [_Rec(pipeline=name, status=s) for s in statuses]


# ── _resolved_failures ────────────────────────────────────────────────────────

def test_resolved_failures_empty():
    assert _resolved_failures([]) == 0


def test_resolved_failures_no_success():
    assert _resolved_failures(_recs(["failure", "failure"])) == 0


def test_resolved_failures_all_resolved():
    # F F S — both failures precede a success
    assert _resolved_failures(_recs(["failure", "failure", "success"])) == 2


def test_resolved_failures_partial():
    # S F F — first failure resolved, second is trailing
    assert _resolved_failures(_recs(["success", "failure", "failure"])) == 1


def test_resolved_failures_interleaved():
    # F S F S — two failures, both resolved
    assert _resolved_failures(_recs(["failure", "success", "failure", "success"])) == 2


# ── compute_burndown ──────────────────────────────────────────────────────────

def test_compute_burndown_returns_none_when_no_failures():
    result = compute_burndown("p", _recs(["success", "success"]))
    assert result is None


def test_compute_burndown_all_unresolved():
    result = compute_burndown("p", _recs(["failure", "failure"]))
    assert result is not None
    assert result.total_failures == 2
    assert result.resolved == 0
    assert result.remaining == 2
    assert result.is_clear is False
    assert result.burn_rate == pytest.approx(0.0)


def test_compute_burndown_fully_resolved():
    result = compute_burndown("p", _recs(["failure", "failure", "success"]))
    assert result is not None
    assert result.resolved == 2
    assert result.remaining == 0
    assert result.is_clear is True
    assert result.burn_rate == pytest.approx(1.0)


def test_compute_burndown_partial():
    result = compute_burndown("p", _recs(["failure", "success", "failure"]))
    assert result is not None
    assert result.total_failures == 2
    assert result.resolved == 1
    assert result.remaining == 1
    assert result.burn_rate == pytest.approx(0.5)


def test_compute_burndown_str_contains_pipeline():
    result = compute_burndown("my_pipe", _recs(["failure", "success"]))
    assert "my_pipe" in str(result)


# ── scan_burndowns ────────────────────────────────────────────────────────────

def test_scan_burndowns_empty_dict():
    assert scan_burndowns({}) == []


def test_scan_burndowns_skips_pipelines_without_failures():
    groups = {"ok": _recs(["success", "success"])}
    assert scan_burndowns(groups) == []


def test_scan_burndowns_returns_results_sorted_by_remaining():
    groups = {
        "a": _recs(["failure", "success"]),          # remaining=0
        "b": _recs(["failure", "failure", "failure"]),  # remaining=3
        "c": _recs(["failure", "failure"]),            # remaining=2 (no success)
    }
    results = scan_burndowns(groups)
    assert [r.pipeline for r in results] == ["b", "c", "a"]
