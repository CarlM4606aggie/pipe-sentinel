"""Tests for pipe_sentinel.quota."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipe_sentinel.quota import QuotaConfig, QuotaStore


@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "quota.json"


@pytest.fixture
def cfg() -> QuotaConfig:
    return QuotaConfig(max_runs=3, window_seconds=60)


@pytest.fixture
def store(state_file: Path) -> QuotaStore:
    return QuotaStore(path=state_file)


# ---------------------------------------------------------------------------
# is_exceeded
# ---------------------------------------------------------------------------

def test_not_exceeded_when_empty(store: QuotaStore, cfg: QuotaConfig) -> None:
    assert store.is_exceeded("etl", cfg) is False


def test_exceeded_after_max_runs(store: QuotaStore, cfg: QuotaConfig) -> None:
    now = time.time()
    for _ in range(cfg.max_runs):
        store.record_run("etl", now=now)
    assert store.is_exceeded("etl", cfg, now=now) is True


def test_not_exceeded_one_below_limit(store: QuotaStore, cfg: QuotaConfig) -> None:
    now = time.time()
    for _ in range(cfg.max_runs - 1):
        store.record_run("etl", now=now)
    assert store.is_exceeded("etl", cfg, now=now) is False


# ---------------------------------------------------------------------------
# window expiry
# ---------------------------------------------------------------------------

def test_not_exceeded_after_window_expires(
    store: QuotaStore, cfg: QuotaConfig
) -> None:
    old_now = time.time() - cfg.window_seconds - 1
    for _ in range(cfg.max_runs):
        store.record_run("etl", now=old_now)
    # querying at current time — old timestamps should be pruned
    assert store.is_exceeded("etl", cfg) is False


def test_runs_in_window_counts_only_recent(store: QuotaStore, cfg: QuotaConfig) -> None:
    old = time.time() - cfg.window_seconds - 1
    recent = time.time()
    store.record_run("etl", now=old)
    store.record_run("etl", now=old)
    store.record_run("etl", now=recent)
    assert store.runs_in_window("etl", cfg.window_seconds) == 1


# ---------------------------------------------------------------------------
# persistence
# ---------------------------------------------------------------------------

def test_state_persisted_across_instances(
    state_file: Path, cfg: QuotaConfig
) -> None:
    s1 = QuotaStore(path=state_file)
    now = time.time()
    s1.record_run("etl", now=now)
    s1.record_run("etl", now=now)

    s2 = QuotaStore(path=state_file)
    assert s2.runs_in_window("etl", cfg.window_seconds, now=now) == 2


def test_multiple_pipelines_tracked_independently(
    store: QuotaStore, cfg: QuotaConfig
) -> None:
    now = time.time()
    for _ in range(cfg.max_runs):
        store.record_run("etl", now=now)
    assert store.is_exceeded("etl", cfg, now=now) is True
    assert store.is_exceeded("load", cfg, now=now) is False
