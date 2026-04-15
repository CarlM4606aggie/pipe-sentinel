"""Tests for pipe_sentinel.sla."""
from __future__ import annotations

import pytest

from pipe_sentinel.sla import SLAConfig, SLAResult, check_sla, scan_sla


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeResult:
    """Minimal stand-in for RunResult."""
    def __init__(self, name: str, duration: float, success: bool = True):
        self.pipeline_name = name
        self.duration = duration
        self.success = success
        self.returncode = 0 if success else 1
        self.stdout = ""
        self.stderr = ""


# ---------------------------------------------------------------------------
# SLAConfig validation
# ---------------------------------------------------------------------------

def test_sla_config_invalid_max():
    with pytest.raises(ValueError, match="max_duration_seconds"):
        SLAConfig(pipeline_name="p", max_duration_seconds=0)


def test_sla_config_invalid_warn_fraction_zero():
    with pytest.raises(ValueError, match="warn_fraction"):
        SLAConfig(pipeline_name="p", max_duration_seconds=60, warn_fraction=0.0)


def test_sla_config_invalid_warn_fraction_one():
    with pytest.raises(ValueError, match="warn_fraction"):
        SLAConfig(pipeline_name="p", max_duration_seconds=60, warn_fraction=1.0)


def test_warn_threshold_computed():
    cfg = SLAConfig(pipeline_name="p", max_duration_seconds=100, warn_fraction=0.75)
    assert cfg.warn_threshold == pytest.approx(75.0)


# ---------------------------------------------------------------------------
# check_sla
# ---------------------------------------------------------------------------

def test_check_sla_ok():
    cfg = SLAConfig(pipeline_name="etl", max_duration_seconds=60)
    result = _FakeResult("etl", duration=30.0)
    sla = check_sla(result, cfg)  # type: ignore[arg-type]
    assert not sla.breached
    assert not sla.warned


def test_check_sla_warned():
    cfg = SLAConfig(pipeline_name="etl", max_duration_seconds=60, warn_fraction=0.8)
    result = _FakeResult("etl", duration=50.0)  # 83% of 60
    sla = check_sla(result, cfg)  # type: ignore[arg-type]
    assert not sla.breached
    assert sla.warned


def test_check_sla_breached():
    cfg = SLAConfig(pipeline_name="etl", max_duration_seconds=60)
    result = _FakeResult("etl", duration=90.0)
    sla = check_sla(result, cfg)  # type: ignore[arg-type]
    assert sla.breached
    assert not sla.warned


def test_check_sla_str_ok():
    cfg = SLAConfig(pipeline_name="etl", max_duration_seconds=60)
    result = _FakeResult("etl", duration=10.0)
    sla = check_sla(result, cfg)  # type: ignore[arg-type]
    assert "OK" in str(sla)


def test_check_sla_str_breached():
    cfg = SLAConfig(pipeline_name="etl", max_duration_seconds=60)
    result = _FakeResult("etl", duration=120.0)
    sla = check_sla(result, cfg)  # type: ignore[arg-type]
    assert "BREACHED" in str(sla)


# ---------------------------------------------------------------------------
# scan_sla
# ---------------------------------------------------------------------------

def test_scan_sla_only_configured_pipelines():
    configs = [SLAConfig(pipeline_name="a", max_duration_seconds=60)]
    results = [
        _FakeResult("a", duration=10.0),
        _FakeResult("b", duration=10.0),  # no config → skipped
    ]
    sla_results = scan_sla(results, configs)  # type: ignore[arg-type]
    assert len(sla_results) == 1
    assert sla_results[0].pipeline_name == "a"


def test_scan_sla_multiple_pipelines():
    configs = [
        SLAConfig(pipeline_name="a", max_duration_seconds=30),
        SLAConfig(pipeline_name="b", max_duration_seconds=60),
    ]
    results = [
        _FakeResult("a", duration=40.0),  # breached
        _FakeResult("b", duration=20.0),  # ok
    ]
    sla_results = scan_sla(results, configs)  # type: ignore[arg-type]
    assert len(sla_results) == 2
    breached = [r for r in sla_results if r.breached]
    assert len(breached) == 1
    assert breached[0].pipeline_name == "a"


def test_scan_sla_empty_results():
    configs = [SLAConfig(pipeline_name="a", max_duration_seconds=60)]
    assert scan_sla([], configs) == []  # type: ignore[arg-type]


def test_scan_sla_empty_configs():
    results = [_FakeResult("a", duration=10.0)]
    assert scan_sla(results, []) == []  # type: ignore[arg-type]
