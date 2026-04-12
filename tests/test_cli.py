"""Tests for the pipe-sentinel CLI entry point."""

import textwrap
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pipe_sentinel.cli import build_parser, main


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def config_file(tmp_path: Path) -> Path:
    cfg = tmp_path / "sentinel.yml"
    cfg.write_text(
        textwrap.dedent("""\
            smtp:
              host: localhost
              port: 25
              sender: sentinel@example.com
              recipients:
                - ops@example.com

            pipelines:
              - name: echo-ok
                command: echo hello
                timeout: 10
                retries: 0
              - name: echo-fail
                command: "false"
                timeout: 10
                retries: 0
        """)
    )
    return cfg


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

def test_parser_defaults():
    parser = build_parser()
    args = parser.parse_args([])
    assert args.config == Path("sentinel.yml")
    assert args.dry_run is False
    assert args.pipeline is None


def test_parser_custom_config():
    parser = build_parser()
    args = parser.parse_args(["-c", "/tmp/my.yml"])
    assert args.config == Path("/tmp/my.yml")


def test_parser_dry_run_flag():
    parser = build_parser()
    args = parser.parse_args(["--dry-run"])
    assert args.dry_run is True


def test_parser_pipeline_filter():
    parser = build_parser()
    args = parser.parse_args(["--pipeline", "my-pipe"])
    assert args.pipeline == "my-pipe"


# ---------------------------------------------------------------------------
# main() integration tests
# ---------------------------------------------------------------------------

def test_main_missing_config_returns_2(tmp_path: Path):
    rc = main(["-c", str(tmp_path / "nonexistent.yml")])
    assert rc == 2


def test_main_unknown_pipeline_returns_2(config_file: Path):
    rc = main(["-c", str(config_file), "--pipeline", "does-not-exist"])
    assert rc == 2


def test_main_all_pass_returns_0(tmp_path: Path):
    cfg = tmp_path / "ok.yml"
    cfg.write_text(
        textwrap.dedent("""\
            smtp:
              host: localhost
              port: 25
              sender: s@example.com
              recipients: [a@example.com]
            pipelines:
              - name: ok
                command: echo hi
                timeout: 10
                retries: 0
        """)
    )
    with patch("pipe_sentinel.cli.run_all") as mock_run:
        report = MagicMock()
        report.all_passed = True
        report.results = []
        mock_run.return_value = report
        rc = main(["-c", str(cfg)])
    assert rc == 0


def test_main_dry_run_passes_none_smtp(config_file: Path):
    with patch("pipe_sentinel.cli.run_all") as mock_run:
        report = MagicMock()
        report.all_passed = True
        report.results = []
        mock_run.return_value = report
        main(["-c", str(config_file), "--dry-run"])
        _, smtp_arg = mock_run.call_args[0]
        assert smtp_arg is None
