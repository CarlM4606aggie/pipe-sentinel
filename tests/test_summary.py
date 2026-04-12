"""Tests for pipe_sentinel.summary."""
import pytest

from pipe_sentinel.runner import RunResult
from pipe_sentinel.scheduler import ScheduleReport
from pipe_sentinel.summary import (
    SummaryStats,
    compute_stats,
    format_summary,
    print_summary,
)


@pytest.fixture
def mixed_results():
    return [
        RunResult(pipeline_name="ingest", success=True, returncode=0,
                  stdout="ok", stderr="", duration=1.5, attempts=1),
        RunResult(pipeline_name="transform", success=False, returncode=1,
                  stdout="", stderr="error", duration=2.3, attempts=3),
        RunResult(pipeline_name="load", success=True, returncode=0,
                  stdout="done", stderr="", duration=0.8, attempts=1),
    ]


@pytest.fixture
def all_passing():
    return [
        RunResult(pipeline_name="a", success=True, returncode=0,
                  stdout="", stderr="", duration=1.0, attempts=1),
        RunResult(pipeline_name="b", success=True, returncode=0,
                  stdout="", stderr="", duration=2.0, attempts=1),
    ]


def test_compute_stats_totals(mixed_results):
    stats = compute_stats(mixed_results)
    assert stats.total == 3
    assert stats.passed == 2
    assert stats.failed == 1


def test_compute_stats_duration(mixed_results):
    stats = compute_stats(mixed_results)
    assert abs(stats.total_duration - 4.6) < 0.001


def test_compute_stats_pass_rate(mixed_results):
    stats = compute_stats(mixed_results)
    assert abs(stats.pass_rate - 66.7) < 0.1


def test_compute_stats_empty():
    stats = compute_stats([])
    assert stats.total == 0
    assert stats.pass_rate == 0.0


def test_format_summary_contains_pipeline_names(mixed_results):
    report = ScheduleReport(results=mixed_results, alerts_sent=0)
    output = format_summary(report)
    assert "ingest" in output
    assert "transform" in output
    assert "load" in output


def test_format_summary_shows_fail_status(mixed_results):
    report = ScheduleReport(results=mixed_results, alerts_sent=0)
    output = format_summary(report)
    assert "FAIL" in output


def test_format_summary_shows_ok_status(all_passing):
    report = ScheduleReport(results=all_passing, alerts_sent=0)
    output = format_summary(report)
    assert "FAIL" not in output
    assert output.count("OK") == 2


def test_format_summary_header(mixed_results):
    report = ScheduleReport(results=mixed_results, alerts_sent=0)
    output = format_summary(report)
    assert "Pipeline Run Summary" in output


def test_print_summary_outputs_to_stdout(mixed_results, capsys):
    report = ScheduleReport(results=mixed_results, alerts_sent=0)
    print_summary(report)
    captured = capsys.readouterr()
    assert "Pipeline Run Summary" in captured.out
    assert "ingest" in captured.out
