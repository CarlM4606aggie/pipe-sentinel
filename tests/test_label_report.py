"""Tests for pipe_sentinel.label_report."""
from __future__ import annotations

import pytest

from pipe_sentinel.label import LabelIndex, LabelSet, build_label_index
from pipe_sentinel.label_report import (
    build_label_report,
    format_label_set,
)
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class _FakePipeline:
    name: str
    labels: Dict[str, str] = field(default_factory=dict)


@pytest.fixture()
def index() -> LabelIndex:
    pipelines = [
        _FakePipeline("ingest", {"env": "prod", "team": "data"}),
        _FakePipeline("transform", {"env": "prod", "team": "ml"}),
        _FakePipeline("archive", {}),
    ]
    return build_label_index(pipelines)


# ---------------------------------------------------------------------------
# format_label_set
# ---------------------------------------------------------------------------

def test_format_label_set_with_labels():
    ls = LabelSet("ingest", {"env": "prod", "team": "data"})
    out = format_label_set(ls)
    assert "ingest" in out
    assert "env=prod" in out
    assert "team=data" in out


def test_format_label_set_no_labels():
    ls = LabelSet("archive", {})
    out = format_label_set(ls)
    assert "archive" in out
    assert "no labels" in out


# ---------------------------------------------------------------------------
# build_label_report — full index
# ---------------------------------------------------------------------------

def test_build_label_report_shows_count(index: LabelIndex):
    out = build_label_report(index)
    assert "3" in out


def test_build_label_report_lists_all_pipelines(index: LabelIndex):
    out = build_label_report(index)
    for name in ("ingest", "transform", "archive"):
        assert name in out


# ---------------------------------------------------------------------------
# build_label_report — with selector
# ---------------------------------------------------------------------------

def test_build_label_report_selector_filters(index: LabelIndex):
    out = build_label_report(index, selector={"team": "data"})
    assert "ingest" in out
    assert "transform" not in out


def test_build_label_report_selector_shows_match_count(index: LabelIndex):
    out = build_label_report(index, selector={"env": "prod"})
    assert "2" in out


def test_build_label_report_selector_shows_selector_string(index: LabelIndex):
    out = build_label_report(index, selector={"team": "ml"})
    assert "team=ml" in out


def test_build_label_report_no_match_selector(index: LabelIndex):
    out = build_label_report(index, selector={"env": "dev"})
    assert "0" in out
