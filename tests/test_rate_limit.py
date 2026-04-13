"""Tests for pipe_sentinel.rate_limit."""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from pipe_sentinel.rate_limit import RateLimitState, RateLimiter


@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    return tmp_path / "rate_limit.json"


@pytest.fixture
def limiter(state_file: Path) -> RateLimiter:
    return RateLimiter(state_file, window_seconds=60, max_runs=3)


# --- RateLimitState unit tests ---

def test_not_limited_when_empty():
    s = RateLimitState("pipe", 60, 3)
    assert not s.is_limited()


def test_limited_after_max_runs():
    now = time.time()
    s = RateLimitState("pipe", 60, 3)
    for _ in range(3):
        s.record_run(now)
    assert s.is_limited(now)


def test_not_limited_after_window_expires():
    now = time.time()
    s = RateLimitState("pipe", 60, 3)
    old = now - 120
    for _ in range(3):
        s.record_run(old)
    assert not s.is_limited(now)


def test_runs_in_window_counts_only_recent():
    now = time.time()
    s = RateLimitState("pipe", 60, 10)
    s.record_run(now - 120)  # outside window
    s.record_run(now - 30)   # inside
    s.record_run(now - 10)   # inside
    assert s.runs_in_window(now) == 2


def test_roundtrip_serialisation():
    now = time.time()
    s = RateLimitState("pipe", 60, 5)
    s.record_run(now)
    restored = RateLimitState.from_dict(s.to_dict())
    assert restored.pipeline == s.pipeline
    assert restored.window_seconds == s.window_seconds
    assert restored.max_runs == s.max_runs
    assert len(restored.run_timestamps) == 1


# --- RateLimiter integration tests ---

def test_limiter_starts_unlimited(limiter: RateLimiter):
    assert not limiter.is_limited("pipe_a")


def test_limiter_becomes_limited_at_threshold(limiter: RateLimiter):
    now = time.time()
    for _ in range(3):
        limiter.record_run("pipe_a", now)
    assert limiter.is_limited("pipe_a", now)


def test_limiter_persists_to_disk(state_file: Path):
    now = time.time()
    lim = RateLimiter(state_file, window_seconds=60, max_runs=3)
    lim.record_run("pipe_x", now)
    lim.record_run("pipe_x", now)
    # Reload from disk
    lim2 = RateLimiter(state_file, window_seconds=60, max_runs=3)
    assert lim2.runs_in_window("pipe_x", now) == 2


def test_limiter_tracks_multiple_pipelines(limiter: RateLimiter):
    now = time.time()
    for _ in range(3):
        limiter.record_run("pipe_a", now)
    limiter.record_run("pipe_b", now)
    assert limiter.is_limited("pipe_a", now)
    assert not limiter.is_limited("pipe_b", now)


def test_runs_in_window_returns_count(limiter: RateLimiter):
    now = time.time()
    limiter.record_run("pipe_a", now)
    limiter.record_run("pipe_a", now)
    assert limiter.runs_in_window("pipe_a", now) == 2
