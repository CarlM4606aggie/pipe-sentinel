"""Tests for pipe_sentinel.snapshot and pipe_sentinel.snapshot_report."""
import json
import os
import pytest

from pipe_sentinel.snapshot import (
    PipelineSnapshot,
    SnapshotDiff,
    make_snapshot,
    save_snapshot,
    load_snapshot,
    diff_snapshot,
)
from pipe_sentinel.snapshot_report import (
    format_diff,
    build_snapshot_report,
)


@pytest.fixture()
def snap_path(tmp_path):
    return str(tmp_path / "snapshots.json")


def _snap(name="etl", status="success", cf=0, ts="2024-01-01T00:00:00+00:00"):
    return make_snapshot(name, status, ts, cf)


# --- PipelineSnapshot ---

def test_is_degraded_when_failure():
    s = _snap(status="failure")
    assert s.is_degraded() is True


def test_is_degraded_when_consecutive_failures():
    s = _snap(status="success", cf=2)
    assert s.is_degraded() is True


def test_not_degraded_when_passing():
    s = _snap(status="success", cf=0)
    assert s.is_degraded() is False


# --- save / load ---

def test_save_and_load_roundtrip(snap_path):
    s = _snap(name="pipe_a", status="success")
    save_snapshot(s, snap_path)
    loaded = load_snapshot("pipe_a", snap_path)
    assert loaded is not None
    assert loaded.name == "pipe_a"
    assert loaded.last_status == "success"


def test_load_missing_file_returns_none(snap_path):
    result = load_snapshot("ghost", snap_path)
    assert result is None


def test_load_unknown_pipeline_returns_none(snap_path):
    s = _snap(name="known")
    save_snapshot(s, snap_path)
    assert load_snapshot("unknown", snap_path) is None


def test_save_multiple_pipelines(snap_path):
    save_snapshot(_snap(name="a", status="success"), snap_path)
    save_snapshot(_snap(name="b", status="failure"), snap_path)
    assert load_snapshot("a", snap_path).last_status == "success"
    assert load_snapshot("b", snap_path).last_status == "failure"


# --- SnapshotDiff ---

def test_status_changed_detects_change():
    prev = _snap(status="success")
    cur = _snap(status="failure")
    diff = SnapshotDiff(name="p", previous=prev, current=cur)
    assert diff.status_changed() is True


def test_recovered_true():
    prev = _snap(status="failure")
    cur = _snap(status="success")
    diff = SnapshotDiff(name="p", previous=prev, current=cur)
    assert diff.recovered() is True
    assert diff.newly_failing() is False


def test_newly_failing_true():
    prev = _snap(status="success")
    cur = _snap(status="failure")
    diff = SnapshotDiff(name="p", previous=prev, current=cur)
    assert diff.newly_failing() is True
    assert diff.recovered() is False


def test_no_previous_no_change():
    diff = SnapshotDiff(name="p", previous=None, current=_snap(status="failure"))
    assert diff.status_changed() is False


# --- diff_snapshot ---

def test_diff_snapshot_no_prior(snap_path):
    cur = _snap(name="new_pipe", status="success")
    d = diff_snapshot("new_pipe", cur, snap_path)
    assert d.previous is None


def test_diff_snapshot_with_prior(snap_path):
    old = _snap(name="p", status="success")
    save_snapshot(old, snap_path)
    cur = _snap(name="p", status="failure")
    d = diff_snapshot("p", cur, snap_path)
    assert d.previous is not None
    assert d.newly_failing() is True


# --- snapshot_report ---

def test_build_snapshot_report_empty():
    report = build_snapshot_report([])
    assert "No pipeline snapshots" in report


def test_build_snapshot_report_contains_name():
    cur = _snap(name="my_pipe", status="success")
    diff = SnapshotDiff(name="my_pipe", previous=None, current=cur)
    report = build_snapshot_report([diff])
    assert "my_pipe" in report


def test_build_snapshot_report_shows_newly_failing():
    prev = _snap(status="success")
    cur = _snap(status="failure")
    diff = SnapshotDiff(name="p", previous=prev, current=cur)
    report = build_snapshot_report([diff])
    assert "NEWLY FAILING" in report


def test_build_snapshot_report_shows_recovered():
    prev = _snap(status="failure")
    cur = _snap(status="success")
    diff = SnapshotDiff(name="p", previous=prev, current=cur)
    report = build_snapshot_report([diff])
    assert "RECOVERED" in report
