"""Tests for pipe_sentinel.backoff."""
from __future__ import annotations

import pytest

from pipe_sentinel.backoff import (
    BackoffConfig,
    BackoffStrategy,
    delay_for,
    delay_schedule,
    parse_backoff_config,
)


# ---------------------------------------------------------------------------
# delay_for — constant
# ---------------------------------------------------------------------------

class TestConstantBackoff:
    def setup_method(self):
        self.cfg = BackoffConfig(strategy=BackoffStrategy.CONSTANT, base_delay=5.0, max_delay=5.0)

    def test_attempt_0_returns_zero(self):
        assert delay_for(self.cfg, 0) == 0.0

    def test_attempt_1_returns_base(self):
        assert delay_for(self.cfg, 1) == 5.0

    def test_attempt_3_still_base(self):
        assert delay_for(self.cfg, 3) == 5.0


# ---------------------------------------------------------------------------
# delay_for — linear
# ---------------------------------------------------------------------------

class TestLinearBackoff:
    def setup_method(self):
        self.cfg = BackoffConfig(strategy=BackoffStrategy.LINEAR, base_delay=2.0, max_delay=100.0)

    def test_attempt_1_equals_base(self):
        assert delay_for(self.cfg, 1) == 2.0

    def test_attempt_3_is_triple_base(self):
        assert delay_for(self.cfg, 3) == 6.0

    def test_capped_at_max(self):
        cfg = BackoffConfig(strategy=BackoffStrategy.LINEAR, base_delay=10.0, max_delay=15.0)
        assert delay_for(cfg, 5) == 15.0


# ---------------------------------------------------------------------------
# delay_for — exponential
# ---------------------------------------------------------------------------

class TestExponentialBackoff:
    def setup_method(self):
        self.cfg = BackoffConfig(strategy=BackoffStrategy.EXPONENTIAL, base_delay=1.0,
                                 multiplier=2.0, max_delay=100.0)

    def test_attempt_1_equals_base(self):
        assert delay_for(self.cfg, 1) == 1.0

    def test_attempt_2_doubles(self):
        assert delay_for(self.cfg, 2) == 2.0

    def test_attempt_4_is_eight(self):
        assert delay_for(self.cfg, 4) == 8.0

    def test_capped_at_max(self):
        cfg = BackoffConfig(strategy=BackoffStrategy.EXPONENTIAL, base_delay=1.0,
                            multiplier=2.0, max_delay=4.0)
        assert delay_for(cfg, 10) == 4.0


# ---------------------------------------------------------------------------
# delay_schedule
# ---------------------------------------------------------------------------

def test_delay_schedule_length():
    cfg = BackoffConfig()
    assert len(delay_schedule(cfg, 5)) == 5


def test_delay_schedule_first_is_zero():
    cfg = BackoffConfig()
    schedule = delay_schedule(cfg, 3)
    assert schedule[0] == 0.0


def test_delay_schedule_empty_for_zero_retries():
    assert delay_schedule(BackoffConfig(), 0) == []


# ---------------------------------------------------------------------------
# parse_backoff_config
# ---------------------------------------------------------------------------

def test_parse_defaults():
    cfg = parse_backoff_config({})
    assert cfg.strategy == BackoffStrategy.EXPONENTIAL
    assert cfg.base_delay == 1.0
    assert cfg.max_delay == 60.0
    assert cfg.jitter is False


def test_parse_custom():
    raw = {"strategy": "linear", "base_delay": 3.0, "max_delay": 30.0, "multiplier": 1.0, "jitter": True}
    cfg = parse_backoff_config(raw)
    assert cfg.strategy == BackoffStrategy.LINEAR
    assert cfg.base_delay == 3.0
    assert cfg.jitter is True


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

def test_invalid_base_delay_raises():
    with pytest.raises(ValueError, match="base_delay"):
        BackoffConfig(base_delay=0)


def test_max_less_than_base_raises():
    with pytest.raises(ValueError, match="max_delay"):
        BackoffConfig(base_delay=10.0, max_delay=5.0)
