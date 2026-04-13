"""Tests for pipe_sentinel.exporter."""
from __future__ import annotations

import csv
import io
import json
import os
import tempfile
from datetime import datetime

import pytest

from pipe_sentinel.audit import AuditRecord, init_db, record_run
from pipe_sentinel.exporter import export_csv, export_json, export_records
from pipe_sentinel.runner import RunResult


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "audit.db")
    init_db(path)
    return path


@pytest.fixture()
def populated_db(db_path):
    success = RunResult(
        pipeline_name="etl_alpha",
        success=True,
        returncode=0,
        stdout="ok",
        stderr="",
        duration=1.2,
        attempts=1,
    )
    failure = RunResult(
        pipeline_name="etl_beta",
        success=False,
        returncode=1,
        stdout="",
        stderr="boom",
        duration=0.5,
        attempts=3,
    )
    record_run(db_path, success)
    record_run(db_path, failure)
    return db_path


# ---------------------------------------------------------------------------
# export_csv
# ---------------------------------------------------------------------------

def test_export_csv_empty_returns_empty_string():
    assert export_csv([]) == ""


def test_export_csv_has_header_and_rows(populated_db):
    from pipe_sentinel.audit import fetch_recent
    records = fetch_recent(populated_db, limit=10)
    output = export_csv(records)
    reader = csv.DictReader(io.StringIO(output))
    rows = list(reader)
    assert len(rows) == 2
    assert "pipeline_name" in reader.fieldnames
    assert "success" in reader.fieldnames


def test_export_csv_values_correct(populated_db):
    from pipe_sentinel.audit import fetch_recent
    records = fetch_recent(populated_db, limit=10)
    output = export_csv(records)
    assert "etl_alpha" in output
    assert "etl_beta" in output


# ---------------------------------------------------------------------------
# export_json
# ---------------------------------------------------------------------------

def test_export_json_returns_list(populated_db):
    from pipe_sentinel.audit import fetch_recent
    records = fetch_recent(populated_db, limit=10)
    output = export_json(records)
    parsed = json.loads(output)
    assert isinstance(parsed, list)
    assert len(parsed) == 2


def test_export_json_contains_fields(populated_db):
    from pipe_sentinel.audit import fetch_recent
    records = fetch_recent(populated_db, limit=10)
    parsed = json.loads(export_json(records))
    assert "pipeline_name" in parsed[0]
    assert "success" in parsed[0]


# ---------------------------------------------------------------------------
# export_records (integration)
# ---------------------------------------------------------------------------

def test_export_records_json_format(populated_db):
    output = export_records(populated_db, fmt="json", limit=10)
    parsed = json.loads(output)
    assert len(parsed) == 2


def test_export_records_csv_format(populated_db):
    output = export_records(populated_db, fmt="csv", limit=10)
    assert "pipeline_name" in output


def test_export_records_pipeline_filter(populated_db):
    output = export_records(populated_db, fmt="json", pipeline_name="etl_alpha")
    parsed = json.loads(output)
    assert all(r["pipeline_name"] == "etl_alpha" for r in parsed)
    assert len(parsed) == 1
