"""Tests for pipe_sentinel.cluster and pipe_sentinel.cluster_report."""
from __future__ import annotations

import pytest

from pipe_sentinel.cluster import (
    ClusterReport,
    ClusterResult,
    _fingerprint,
    _normalise,
    cluster_failures,
)
from pipe_sentinel.cluster_report import (
    build_cluster_report,
    format_cluster_result,
)


# ---------------------------------------------------------------------------
# _normalise / _fingerprint
# ---------------------------------------------------------------------------

def test_normalise_strips_numbers():
    assert "<n>" in _normalise("error on line 42")


def test_normalise_strips_timestamps():
    result = _normalise("failed at 2024-01-15T08:30:00")
    assert "<ts>" in result


def test_normalise_strips_hex():
    result = _normalise("segfault at 0xDEADBEEF")
    assert "<addr>" in result


def test_same_fingerprint_for_similar_errors():
    fp1 = _fingerprint("connection refused on port 5432")
    fp2 = _fingerprint("connection refused on port 3306")
    assert fp1 == fp2


def test_different_fingerprint_for_different_errors():
    fp1 = _fingerprint("connection refused")
    fp2 = _fingerprint("disk quota exceeded")
    assert fp1 != fp2


# ---------------------------------------------------------------------------
# cluster_failures
# ---------------------------------------------------------------------------

def test_empty_failures_returns_empty_report():
    report = cluster_failures([])
    assert report.total_clusters == 0
    assert report.clusters == []


def test_single_failure_creates_one_cluster():
    report = cluster_failures([("pipe_a", "connection refused on port 5432")])
    assert report.total_clusters == 1
    assert report.clusters[0].pipelines == ["pipe_a"]


def test_similar_errors_grouped_together():
    failures = [
        ("pipe_a", "connection refused on port 5432"),
        ("pipe_b", "connection refused on port 3306"),
    ]
    report = cluster_failures(failures)
    assert report.total_clusters == 1
    assert set(report.clusters[0].pipelines) == {"pipe_a", "pipe_b"}


def test_different_errors_create_separate_clusters():
    failures = [
        ("pipe_a", "connection refused"),
        ("pipe_b", "disk quota exceeded"),
    ]
    report = cluster_failures(failures)
    assert report.total_clusters == 2


def test_cluster_size_matches_pipeline_count():
    failures = [
        ("pipe_a", "timeout after 30s"),
        ("pipe_b", "timeout after 60s"),
        ("pipe_c", "timeout after 120s"),
    ]
    report = cluster_failures(failures)
    assert report.clusters[0].size == 3


def test_multi_count_and_singleton_count():
    failures = [
        ("pipe_a", "connection refused on port 5432"),
        ("pipe_b", "connection refused on port 3306"),
        ("pipe_c", "unique error xyz"),
    ]
    report = cluster_failures(failures)
    assert report.multi_count == 1
    assert report.singleton_count == 1


# ---------------------------------------------------------------------------
# cluster_report formatting
# ---------------------------------------------------------------------------

def test_format_cluster_result_contains_pipeline_name():
    result = ClusterResult(
        cluster_id="abc12345",
        pipelines=["pipe_a", "pipe_b"],
        sample_error="connection refused",
    )
    text = format_cluster_result(result)
    assert "pipe_a" in text
    assert "pipe_b" in text


def test_format_cluster_result_contains_cluster_id():
    result = ClusterResult(
        cluster_id="abc12345",
        pipelines=["pipe_a"],
        sample_error="disk full",
    )
    text = format_cluster_result(result)
    assert "abc12345" in text


def test_build_cluster_report_shows_header():
    report = cluster_failures([("pipe_a", "connection refused")])
    text = build_cluster_report(report)
    assert "Pipeline Failure Clusters" in text


def test_build_cluster_report_empty_message():
    report = ClusterReport(clusters=[])
    text = build_cluster_report(report)
    assert "No failure clusters" in text


def test_icon_multi_pipeline_is_yellow():
    from pipe_sentinel.cluster_report import _icon
    assert _icon(2) == "🟡"


def test_icon_large_cluster_is_red():
    from pipe_sentinel.cluster_report import _icon
    assert _icon(5) == "🔴"


def test_icon_singleton_is_white():
    from pipe_sentinel.cluster_report import _icon
    assert _icon(1) == "⚪"
