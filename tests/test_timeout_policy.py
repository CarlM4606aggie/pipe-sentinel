"""Tests for pipe_sentinel.timeout_policy."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipe_sentinel.timeout_policy import (
    TimeoutPolicy,
    _DEFAULT_TIMEOUT,
    resolve_policy,
    resolve_all,
    build_policy_map,
)


def _make_pipeline(name: str, timeout: int | None = None) -> MagicMock:
    p = MagicMock()
    p.name = name
    p.timeout = timeout
    return p


def _make_config(pipelines, default_timeout=None) -> MagicMock:
    cfg = MagicMock()
    cfg.pipelines = pipelines
    cfg.default_timeout = default_timeout
    return cfg


# ---------------------------------------------------------------------------
# resolve_policy
# ---------------------------------------------------------------------------

class TestResolvePolicy:
    def test_pipeline_level_takes_priority(self):
        p = _make_pipeline("etl", timeout=60)
        result = resolve_policy(p, global_timeout=120)
        assert result.timeout_seconds == 60
        assert result.source == "pipeline"

    def test_global_used_when_no_pipeline_timeout(self):
        p = _make_pipeline("etl", timeout=None)
        result = resolve_policy(p, global_timeout=90)
        assert result.timeout_seconds == 90
        assert result.source == "global"

    def test_default_used_when_no_overrides(self):
        p = _make_pipeline("etl", timeout=None)
        result = resolve_policy(p, global_timeout=None)
        assert result.timeout_seconds == _DEFAULT_TIMEOUT
        assert result.source == "default"

    def test_pipeline_name_preserved(self):
        p = _make_pipeline("my-pipeline", timeout=45)
        result = resolve_policy(p)
        assert result.pipeline_name == "my-pipeline"

    def test_str_representation(self):
        policy = TimeoutPolicy("demo", 30, "pipeline")
        text = str(policy)
        assert "demo" in text
        assert "30s" in text
        assert "pipeline" in text


# ---------------------------------------------------------------------------
# resolve_all
# ---------------------------------------------------------------------------

class TestResolveAll:
    def test_returns_one_policy_per_pipeline(self):
        cfg = _make_config(
            [_make_pipeline("a"), _make_pipeline("b"), _make_pipeline("c")]
        )
        policies = resolve_all(cfg)
        assert len(policies) == 3

    def test_global_timeout_propagates(self):
        cfg = _make_config(
            [_make_pipeline("x", timeout=None), _make_pipeline("y", timeout=None)],
            default_timeout=200,
        )
        policies = resolve_all(cfg)
        assert all(p.timeout_seconds == 200 for p in policies)
        assert all(p.source == "global" for p in policies)

    def test_mixed_sources(self):
        cfg = _make_config(
            [_make_pipeline("has-own", timeout=10), _make_pipeline("inherits", timeout=None)],
            default_timeout=50,
        )
        policies = resolve_all(cfg)
        sources = {p.pipeline_name: p.source for p in policies}
        assert sources["has-own"] == "pipeline"
        assert sources["inherits"] == "global"


# ---------------------------------------------------------------------------
# build_policy_map
# ---------------------------------------------------------------------------

class TestBuildPolicyMap:
    def test_keys_match_pipeline_names(self):
        cfg = _make_config([_make_pipeline("alpha"), _make_pipeline("beta")])
        mapping = build_policy_map(cfg)
        assert set(mapping.keys()) == {"alpha", "beta"}

    def test_values_are_timeout_policies(self):
        cfg = _make_config([_make_pipeline("p1", timeout=15)])
        mapping = build_policy_map(cfg)
        assert isinstance(mapping["p1"], TimeoutPolicy)
        assert mapping["p1"].timeout_seconds == 15
