"""Tests for pipe_sentinel.suppression and suppression_report."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipe_sentinel.suppression import SuppressionRule, SuppressionStore
from pipe_sentinel.suppression_report import (
    build_suppression_report,
    format_rule,
)

NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# SuppressionRule
# ---------------------------------------------------------------------------

def test_rule_active_when_no_expiry():
    rule = SuppressionRule("etl_load", "maintenance", expires_at=None)
    assert rule.is_active(NOW) is True


def test_rule_active_before_expiry():
    rule = SuppressionRule("etl_load", "maintenance", expires_at=NOW + timedelta(hours=2))
    assert rule.is_active(NOW) is True


def test_rule_inactive_after_expiry():
    rule = SuppressionRule("etl_load", "maintenance", expires_at=NOW - timedelta(seconds=1))
    assert rule.is_active(NOW) is False


def test_rule_roundtrip_serialisation():
    rule = SuppressionRule("pipe_a", "planned downtime", expires_at=NOW)
    restored = SuppressionRule.from_dict(rule.to_dict())
    assert restored.pipeline_name == rule.pipeline_name
    assert restored.reason == rule.reason
    assert restored.expires_at == rule.expires_at


def test_rule_roundtrip_no_expiry():
    rule = SuppressionRule("pipe_b", "indefinite", expires_at=None)
    restored = SuppressionRule.from_dict(rule.to_dict())
    assert restored.expires_at is None


# ---------------------------------------------------------------------------
# SuppressionStore
# ---------------------------------------------------------------------------

@pytest.fixture
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "suppression.json"


def test_suppress_persists_rule(store_path: Path):
    store = SuppressionStore.load(store_path)
    rule = SuppressionRule("etl_load", "test", expires_at=None)
    store.suppress(rule)
    reloaded = SuppressionStore.load(store_path)
    assert reloaded.is_suppressed("etl_load", NOW)


def test_unsuppress_removes_rule(store_path: Path):
    store = SuppressionStore.load(store_path)
    store.suppress(SuppressionRule("etl_load", "test", expires_at=None))
    removed = store.unsuppress("etl_load")
    assert removed is True
    assert not store.is_suppressed("etl_load", NOW)


def test_unsuppress_missing_returns_false(store_path: Path):
    store = SuppressionStore.load(store_path)
    assert store.unsuppress("nonexistent") is False


def test_is_suppressed_expired_rule(store_path: Path):
    store = SuppressionStore.load(store_path)
    rule = SuppressionRule("etl_load", "old", expires_at=NOW - timedelta(hours=1))
    store.suppress(rule)
    assert not store.is_suppressed("etl_load", NOW)


def test_prune_expired_removes_stale(store_path: Path):
    store = SuppressionStore.load(store_path)
    store.suppress(SuppressionRule("a", "r", expires_at=NOW - timedelta(hours=1)))
    store.suppress(SuppressionRule("b", "r", expires_at=None))
    pruned = store.prune_expired(NOW)
    assert pruned == 1
    assert "a" not in store.rules
    assert "b" in store.rules


# ---------------------------------------------------------------------------
# suppression_report
# ---------------------------------------------------------------------------

def test_format_rule_indefinite():
    rule = SuppressionRule("pipe_x", "planned", expires_at=None)
    line = format_rule(rule, NOW)
    assert "indefinite" in line
    assert "pipe_x" in line
    assert "planned" in line


def test_format_rule_with_expiry():
    rule = SuppressionRule("pipe_y", "deploy", expires_at=NOW + timedelta(hours=3, minutes=15))
    line = format_rule(rule, NOW)
    assert "3h" in line
    assert "pipe_y" in line


def test_build_report_empty():
    report = build_suppression_report([], NOW)
    assert "(none)" in report
    assert "Total: 0" in report


def test_build_report_lists_rules():
    rules = [
        SuppressionRule("alpha", "reason a", expires_at=None),
        SuppressionRule("beta", "reason b", expires_at=NOW + timedelta(minutes=30)),
    ]
    report = build_suppression_report(rules, NOW)
    assert "alpha" in report
    assert "beta" in report
    assert "Total: 2" in report
