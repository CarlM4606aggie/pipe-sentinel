"""Configuration loader for pipe-sentinel."""

import os
from dataclasses import dataclass, field
from typing import List, Optional

import yaml


@dataclass
class PipelineConfig:
    name: str
    command: str
    max_retries: int = 3
    retry_delay_seconds: int = 30
    alert_emails: List[str] = field(default_factory=list)
    timeout_seconds: Optional[int] = None
    enabled: bool = True


@dataclass
class SentinelConfig:
    pipelines: List[PipelineConfig] = field(default_factory=list)
    log_dir: str = "logs"
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    alert_from: Optional[str] = None


def load_config(path: str = "sentinel.yml") -> SentinelConfig:
    """Load and parse the sentinel YAML configuration file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    if raw is None:
        raise ValueError("Config file is empty or invalid YAML.")

    pipelines = [
        PipelineConfig(
            name=p["name"],
            command=p["command"],
            max_retries=p.get("max_retries", 3),
            retry_delay_seconds=p.get("retry_delay_seconds", 30),
            alert_emails=p.get("alert_emails", []),
            timeout_seconds=p.get("timeout_seconds"),
            enabled=p.get("enabled", True),
        )
        for p in raw.get("pipelines", [])
    ]

    smtp = raw.get("smtp", {})
    return SentinelConfig(
        pipelines=pipelines,
        log_dir=raw.get("log_dir", "logs"),
        smtp_host=smtp.get("host"),
        smtp_port=smtp.get("port", 587),
        smtp_user=smtp.get("user"),
        smtp_password=smtp.get("password"),
        alert_from=smtp.get("from"),
    )
