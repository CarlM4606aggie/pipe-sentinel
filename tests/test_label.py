"""Tests for pipe_sentinel.label."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

import pytest

from pipe_sentinel.label import LabelIndex, LabelSet, build_label_index


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class _FakePipeline:
    name: str
    labels: Dict[str, str] = field(default_factory=dict)


def _make(name: str, **labels: str) -> _FakePipeline:
    return _FakePipeline(name=name, labels=labels)


@pytest.fixture()
def pipelines() -> List[_FakePipeline]:
    return [
        _make("ingest", env="prod", team="data"),
        _make("transform", env="prod", team="ml"),
        _make("export", env="staging", team="data"),
        _make("archive"),
    ]


@pytest.fixture()
def index(pipelines: List[_FakePipeline]) -> LabelIndex:
    return build_label_index(pipelines)


# ---------------------------------------------------------------------------
# LabelSet
# ---------------------------------------------------------------------------

def test_label_set_get_existing():
    ls = LabelSet("p", {"env": "prod"})
    assert ls.get("env") == "prod"


def test_label_set_get_missing_returns_none():
    ls = LabelSet("p", {})
    assert ls.get("env") is None


def test_label_set_matches_exact():
    ls = LabelSet("p", {"env": "prod", "team": "data"})
    assert ls.matches({"env": "prod", "team": "data"}) is True


def test_label_set_matches_subset():
    ls = LabelSet("p", {"env": "prod", "team": "data"})
    assert ls.matches({"env": "prod"}) is True


def test_label_set_does_not_match_wrong_value():
    ls = LabelSet("p", {"env": "prod"})
    assert ls.matches({"env": "staging"}) is False


def test_label_set_len():
    ls = LabelSet("p", {"a": "1", "b": "2"})
    assert len(ls) == 2


# ---------------------------------------------------------------------------
# LabelIndex
# ---------------------------------------------------------------------------

def test_build_label_index_all_pipelines(index: LabelIndex):
    assert set(index.all_pipelines()) == {"ingest", "transform", "export", "archive"}


def test_for_pipeline_returns_label_set(index: LabelIndex):
    ls = index.for_pipeline("ingest")
    assert ls is not None
    assert ls.pipeline == "ingest"


def test_for_pipeline_missing_returns_none(index: LabelIndex):
    assert index.for_pipeline("nonexistent") is None


def test_select_by_single_label(index: LabelIndex):
    result = index.select({"team": "data"})
    assert set(result) == {"ingest", "export"}


def test_select_by_multiple_labels(index: LabelIndex):
    result = index.select({"env": "prod", "team": "data"})
    assert result == ["ingest"]


def test_select_no_match_returns_empty(index: LabelIndex):
    assert index.select({"env": "dev"}) == []


def test_pipeline_with_no_labels_has_empty_label_set(index: LabelIndex):
    ls = index.for_pipeline("archive")
    assert ls is not None
    assert len(ls) == 0
