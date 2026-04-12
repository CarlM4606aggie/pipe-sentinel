"""Configuration loading and dataclasses for pipe-sentinel."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class PipelineConfig:
    name: str
    command: str
    timeout: int = 60
    retries: int = 0
    retry_delay: float = 5.0
    recipients: list[str] = field(default_factory=list)


@dataclass
class SmtpConfig:
    host: str
    port: int
    sender: str
    username: Optional[str] = None
    password: Optional[str] = None
    use_tls: bool = True


@dataclass
class SentinelConfig:
    pipelines: list[PipelineConfig]
    smtp: Optional[SmtpConfig] = None
    log_level: str = "INFO"


def _parse_pipeline(data: dict) -> PipelineConfig:
    return PipelineConfig(
        name=data["name"],
        command=data["command"],
        timeout=int(data.get("timeout", 60)),
        retries=int(data.get("retries", 0)),
        retry_delay=float(data.get("retry_delay", 5.0)),
        recipients=data.get("recipients", []),
    )


def _parse_smtp(data: dict) -> SmtpConfig:
    return SmtpConfig(
        host=data["host"],
        port=int(data.get("port", 587)),
        sender=data["sender"],
        username=data.get("username") or os.environ.get("SMTP_USERNAME"),
        password=data.get("password") or os.environ.get("SMTP_PASSWORD"),
        use_tls=bool(data.get("use_tls", True)),
    )


def load_config(path: str | Path = "sentinel.yml") -> SentinelConfig:
    """Load and parse the sentinel YAML configuration file."""
    raw = Path(path).read_text(encoding="utf-8")
    data = yaml.safe_load(raw)

    pipelines = [_parse_pipeline(p) for p in data.get("pipelines", [])]
    smtp = _parse_smtp(data["smtp"]) if "smtp" in data else None
    log_level = data.get("log_level", "INFO").upper()

    return SentinelConfig(pipelines=pipelines, smtp=smtp, log_level=log_level)
