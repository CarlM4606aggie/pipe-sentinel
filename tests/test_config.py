"""Tests for pipe-sentinel config loader."""

import os
import textwrap
import pytest

from pipe_sentinel.config import load_config, PipelineConfig, SentinelConfig


SAMPLE_YAML = textwrap.dedent("""\
    log_dir: /tmp/sentinel_logs
    smtp:
      host: smtp.test.com
      port: 465
      user: user@test.com
      password: secret
      from: noreply@test.com
    pipelines:
      - name: test_pipeline
        command: echo hello
        max_retries: 5
        retry_delay_seconds: 10
        timeout_seconds: 300
        alert_emails:
          - admin@test.com
        enabled: true
      - name: minimal_pipeline
        command: python run.py
""")


@pytest.fixture
def config_file(tmp_path):
    cfg = tmp_path / "sentinel.yml"
    cfg.write_text(SAMPLE_YAML)
    return str(cfg)


def test_load_config_returns_sentinel_config(config_file):
    config = load_config(config_file)
    assert isinstance(config, SentinelConfig)


def test_load_config_smtp_fields(config_file):
    config = load_config(config_file)
    assert config.smtp_host == "smtp.test.com"
    assert config.smtp_port == 465
    assert config.smtp_user == "user@test.com"
    assert config.alert_from == "noreply@test.com"


def test_load_config_pipelines_count(config_file):
    config = load_config(config_file)
    assert len(config.pipelines) == 2


def test_load_config_pipeline_fields(config_file):
    config = load_config(config_file)
    p = config.pipelines[0]
    assert isinstance(p, PipelineConfig)
    assert p.name == "test_pipeline"
    assert p.command == "echo hello"
    assert p.max_retries == 5
    assert p.retry_delay_seconds == 10
    assert p.timeout_seconds == 300
    assert p.alert_emails == ["admin@test.com"]
    assert p.enabled is True


def test_load_config_pipeline_defaults(config_file):
    config = load_config(config_file)
    p = config.pipelines[1]
    assert p.max_retries == 3
    assert p.retry_delay_seconds == 30
    assert p.timeout_seconds is None
    assert p.alert_emails == []
    assert p.enabled is True


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/sentinel.yml")


def test_load_config_empty_file(tmp_path):
    cfg = tmp_path / "sentinel.yml"
    cfg.write_text("")
    with pytest.raises(ValueError, match="empty or invalid"):
        load_config(str(cfg))
