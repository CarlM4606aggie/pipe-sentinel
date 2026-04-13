"""Tests for pipe_sentinel.tags."""
from __future__ import annotations

import pytest

from pipe_sentinel.config import PipelineConfig
from pipe_sentinel.tags import (
    TagIndex,
    build_tag_index,
    pipelines_by_tag,
    tags_for_pipeline,
)


def _make(name: str, tags: list[str]) -> PipelineConfig:
    return PipelineConfig(
        name=name,
        command=f"echo {name}",
        schedule="@daily",
        retries=0,
        timeout=30,
        tags=tags,
        recipients=[],
        max_age_minutes=None,
    )


@pytest.fixture()
def pipelines() -> list[PipelineConfig]:
    return [
        _make("ingest", ["etl", "critical"]),
        _make("transform", ["etl"]),
        _make("report", ["critical", "reporting"]),
        _make("cleanup", []),
    ]


# --- build_tag_index ---

def test_build_tag_index_all_tags(pipelines):
    index = build_tag_index(pipelines)
    assert index.all_tags() == {"etl", "critical", "reporting"}


def test_build_tag_index_pipelines_for_etl(pipelines):
    index = build_tag_index(pipelines)
    assert sorted(index.pipelines_for_tag("etl")) == ["ingest", "transform"]


def test_build_tag_index_pipelines_for_critical(pipelines):
    index = build_tag_index(pipelines)
    assert sorted(index.pipelines_for_tag("critical")) == ["ingest", "report"]


def test_build_tag_index_unknown_tag_returns_empty(pipelines):
    index = build_tag_index(pipelines)
    assert index.pipelines_for_tag("nonexistent") == []


def test_build_tag_index_untagged_pipeline_not_in_index(pipelines):
    index = build_tag_index(pipelines)
    for names in index._index.values():
        assert "cleanup" not in names


def test_tag_index_len(pipelines):
    index = build_tag_index(pipelines)
    assert len(index) == 3


# --- pipelines_by_tag ---

def test_pipelines_by_tag_keys(pipelines):
    result = pipelines_by_tag(pipelines)
    assert set(result.keys()) == {"etl", "critical", "reporting"}


def test_pipelines_by_tag_etl_names = pipelines_by_tag(pipelines)
    names = [p.name for p in result["etl"]]
    assert sorted(names) == ["ingest", "transform"]


def test_pipelines_by_tag_empty_when_no_tags():
    result = pipelines_by_tag([_make("solo", [])])
    assert result == {}


# --- tags_for_pipeline ---

def test_tags_for_pipeline_sorted():
    p = _make("x", ["zzz", "aaa", "mmm"])
    assert tags_for_pipeline(p) == ["aaa", "mmm", "zzz"]


def test_tags_for_pipeline_empty():
    p = _make("x", [])
    assert tags_for_pipeline(p) == []
