"""Tests for pipe_sentinel.jitter and pipe_sentinel.jitter_report."""
from __future__ import annotations

import pytest

from pipe_sentinel.jitter import (
    JitterConfig,
    JitterStrategy,
    parse_jitter_config,
)
from pipe_sentinel.jitter_report import (
    build_jitter_report,
    format_delay_table,
    format_jitter_config,
)


# ---------------------------------------------------------------------------
# JitterConfig.delay_for
# ---------------------------------------------------------------------------

class TestDelayForNone:
    def setup_method(self):
        self.cfg = JitterConfig(strategy=JitterStrategy.NONE, base_delay=1.0,
                                max_delay=60.0, multiplier=2.0, seed=42)

    def test_attempt_0_equals_base(self):
        assert self.cfg.delay_for(0) == pytest.approx(1.0)

    def test_attempt_1_doubles(self):
        assert self.cfg.delay_for(1) == pytest.approx(2.0)

    def test_capped_at_max(self):
        assert self.cfg.delay_for(100) == pytest.approx(60.0)


class TestDelayForFull:
    def setup_method(self):
        self.cfg = JitterConfig(strategy=JitterStrategy.FULL, base_delay=1.0,
                                max_delay=60.0, multiplier=2.0, seed=0)

    def test_delay_between_zero_and_cap(self):
        for attempt in range(6):
            d = self.cfg.delay_for(attempt)
            assert 0.0 <= d <= 60.0


class TestDelayForEqual:
    def setup_method(self):
        self.cfg = JitterConfig(strategy=JitterStrategy.EQUAL, base_delay=2.0,
                                max_delay=32.0, multiplier=2.0, seed=7)

    def test_delay_at_least_half_cap(self):
        for attempt in range(5):
            cap = min(2.0 * (2.0 ** attempt), 32.0)
            d = self.cfg.delay_for(attempt)
            assert d >= cap / 2 - 1e-9
            assert d <= cap + 1e-9


class TestDelayForDecorrelated:
    def setup_method(self):
        self.cfg = JitterConfig(strategy=JitterStrategy.DECORRELATED,
                                base_delay=1.0, max_delay=30.0,
                                multiplier=2.0, seed=3)

    def test_delay_within_max(self):
        for attempt in range(8):
            assert self.cfg.delay_for(attempt) <= 30.0


# ---------------------------------------------------------------------------
# parse_jitter_config
# ---------------------------------------------------------------------------

def test_parse_defaults():
    cfg = parse_jitter_config({})
    assert cfg.strategy == JitterStrategy.FULL
    assert cfg.base_delay == pytest.approx(1.0)
    assert cfg.max_delay == pytest.approx(60.0)
    assert cfg.multiplier == pytest.approx(2.0)


def test_parse_custom_values():
    raw = {"strategy": "none", "base_delay": 0.5, "max_delay": 10.0,
           "multiplier": 3.0, "seed": 99}
    cfg = parse_jitter_config(raw)
    assert cfg.strategy == JitterStrategy.NONE
    assert cfg.base_delay == pytest.approx(0.5)
    assert cfg.max_delay == pytest.approx(10.0)
    assert cfg.multiplier == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# jitter_report
# ---------------------------------------------------------------------------

def test_format_jitter_config_contains_strategy():
    cfg = JitterConfig(strategy=JitterStrategy.EQUAL, seed=0)
    report = format_jitter_config(cfg)
    assert "Equal jitter" in report


def test_format_delay_table_has_correct_rows():
    cfg = JitterConfig(seed=0)
    table = format_delay_table(cfg, attempts=4)
    lines = [l for l in table.splitlines() if l.strip() and not l.startswith("-")]
    # header + 4 data rows
    assert len(lines) == 5


def test_build_jitter_report_is_string():
    cfg = JitterConfig(seed=1)
    report = build_jitter_report(cfg, attempts=3)
    assert isinstance(report, str)
    assert len(report) > 0
