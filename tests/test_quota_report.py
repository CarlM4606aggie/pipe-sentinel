"""Tests for pipe_sentinel.quota_report."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipe_sentinel.quota import QuotaConfig, QuotaStore
from pipe_sentinel.quota_report import (
    _bar,
    build_quota_report,
    format_quota_state,
)


@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "quota.json"


@pytest.fixture
def cfg() -> QuotaConfig:
    return QuotaConfig(max_runs=5, window_seconds=300)


@pytest.fixture
def store(state_file: Path) -> QuotaStore:
    return QuotaStore(path=state_file)


# ---------------------------------------------------------------------------
# _bar
# ---------------------------------------------------------------------------

def test_bar_empty() -> None:
    assert _bar(0, 10) == "[" + "-" * 20 + "]"


def test_bar_full() -> None:
    assert _bar(10, 10) == "[" + "#" * 20 + "]"


def test_bar_half() -> None:
    result = _bar(5, 10)
    assert result == "[" + "#" * 10 + "-" * 10 + "]"


def test_bar_zero_limit() -> None:
    assert _bar(0, 0) == "[" + "-" * 20 + "]"


# ---------------------------------------------------------------------------
# format_quota_state
# ---------------------------------------------------------------------------

def test_format_quota_state_ok(store: QuotaStore, cfg: QuotaConfig) -> None:
    text = format_quota_state("etl", cfg, store)
    assert "etl" in text
    assert "OK" in text
    assert "0/5" in text


def test_format_quota_state_exceeded(store: QuotaStore, cfg: QuotaConfig) -> None:
    now = time.time()
    for _ in range(cfg.max_runs):
        store.record_run("etl", now=now)
    text = format_quota_state("etl", cfg, store)
    assert "EXCEEDED" in text


# ---------------------------------------------------------------------------
# build_quota_report
# ---------------------------------------------------------------------------

def test_build_quota_report_header(store: QuotaStore, cfg: QuotaConfig) -> None:
    report = build_quota_report(["etl", "load"], cfg, store)
    assert "Quota Report" in report
    assert "5" in report
    assert "300" in report


def test_build_quota_report_lists_all_pipelines(
    store: QuotaStore, cfg: QuotaConfig
) -> None:
    report = build_quota_report(["etl", "load", "transform"], cfg, store)
    for name in ["etl", "load", "transform"]:
        assert name in report


def test_build_quota_report_empty_pipelines(
    store: QuotaStore, cfg: QuotaConfig
) -> None:
    report = build_quota_report([], cfg, store)
    assert "Quota Report" in report
