"""Configuration loading and dataclasses for pipe-sentinel."""

import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class PipelineConfig:
    """Configuration for a single monitored pipeline."""

    name: str
    command: str
    schedule: str
    max_retries: int = 3
    retry_delay_seconds: int = 60
    timeout_seconds: int = 3600
    alert_on_failure: bool = True
    description: Optional[str] = None


@dataclass
class SmtpConfig:
    """SMTP settings for email alerts."""

    host: str
    port: int
    username: str
    password: str
    from_address: str
    use_tls: bool = True


@dataclass
class SentinelConfig:
    """Root configuration for pipe-sentinel."""

    alert_recipients: list[str]
    pipelines: list[PipelineConfig]
    smtp: Optional[SmtpConfig] = None
    log_level: str = "INFO"
    state_dir: str = ".pipe_sentinel_state"


def _parse_pipeline(data: dict) -> PipelineConfig:
    return PipelineConfig(
        name=data["name"],
        command=data["command"],
        schedule=data["schedule"],
        max_retries=data.get("max_retries", 3),
        retry_delay_seconds=data.get("retry_delay_seconds", 60),
        timeout_seconds=data.get("timeout_seconds", 3600),
        alert_on_failure=data.get("alert_on_failure", True),
        description=data.get("description"),
    )


def _parse_smtp(data: dict) -> SmtpConfig:
    return SmtpConfig(
        host=data["host"],
        port=int(data["port"]),
        username=data["username"],
        password=data["password"],
        from_address=data["from_address"],
        use_tls=data.get("use_tls", True),
    )


def load_config(path: str | Path) -> SentinelConfig:
    """Load and parse a sentinel YAML configuration file."""
    with open(path, "r") as fh:
        raw = yaml.safe_load(fh)

    smtp = _parse_smtp(raw["smtp"]) if "smtp" in raw else None
    pipelines = [_parse_pipeline(p) for p in raw.get("pipelines", [])]

    return SentinelConfig(
        alert_recipients=raw.get("alert_recipients", []),
        pipelines=pipelines,
        smtp=smtp,
        log_level=raw.get("log_level", "INFO"),
        state_dir=raw.get("state_dir", ".pipe_sentinel_state"),
    )
