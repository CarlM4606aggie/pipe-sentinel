"""Tests for pipe_sentinel.filter."""

from __future__ import annotations

import pytest

from pipe_sentinel.config import PipelineConfig
from pipe_sentinel.filter import (
    FilterCriteria,
    apply_filter,
    filter_by_names,
    filter_by_tags,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_pipeline(name: str, tags=None, command="echo ok", retries=0, timeout=30):
    p = PipelineConfig(
        name=name,
        command=command,
        retries=retries,
        timeout=timeout,
    )
    p.tags = tags or []
    return p


@pytest.fixture()
def pipelines():
    return [
        _make_pipeline("ingest", tags=["daily", "critical"]),
        _make_pipeline("transform", tags=["daily"]),
        _make_pipeline("export", tags=["weekly"]),
        _make_pipeline("cleanup", tags=[]),
    ]


# ---------------------------------------------------------------------------
# FilterCriteria.is_empty
# ---------------------------------------------------------------------------


def test_filter_criteria_empty_when_no_fields():
    assert FilterCriteria().is_empty() is True


def test_filter_criteria_not_empty_with_names():
    assert FilterCriteria(names=["ingest"]).is_empty() is False


def test_filter_criteria_not_empty_with_tags():
    assert FilterCriteria(tags=["daily"]).is_empty() is False


# ---------------------------------------------------------------------------
# apply_filter — empty criteria
# ---------------------------------------------------------------------------


def test_apply_filter_empty_criteria_returns_all(pipelines):
    result = apply_filter(pipelines, FilterCriteria())
    assert result == pipelines


# ---------------------------------------------------------------------------
# filter_by_names
# ---------------------------------------------------------------------------


def test_filter_by_names_single_match(pipelines):
    result = filter_by_names(pipelines, ["ingest"])
    assert len(result) == 1
    assert result[0].name == "ingest"


def test_filter_by_names_multiple_matches(pipelines):
    result = filter_by_names(pipelines, ["ingest", "export"])
    names = {p.name for p in result}
    assert names == {"ingest", "export"}


def test_filter_by_names_no_match_returns_empty(pipelines):
    result = filter_by_names(pipelines, ["nonexistent"])
    assert result == []


# ---------------------------------------------------------------------------
# filter_by_tags
# ---------------------------------------------------------------------------


def test_filter_by_tags_single_tag(pipelines):
    result = filter_by_tags(pipelines, ["daily"])
    names = {p.name for p in result}
    assert names == {"ingest", "transform"}


def test_filter_by_tags_multiple_tags_union(pipelines):
    result = filter_by_tags(pipelines, ["critical", "weekly"])
    names = {p.name for p in result}
    assert names == {"ingest", "export"}


def test_filter_by_tags_no_match_returns_empty(pipelines):
    result = filter_by_tags(pipelines, ["unknown-tag"])
    assert result == []


# ---------------------------------------------------------------------------
# apply_filter — combined names + tags (union)
# ---------------------------------------------------------------------------


def test_apply_filter_names_and_tags_union(pipelines):
    criteria = FilterCriteria(names=["cleanup"], tags=["weekly"])
    result = apply_filter(pipelines, criteria)
    names = {p.name for p in result}
    assert names == {"cleanup", "export"}
