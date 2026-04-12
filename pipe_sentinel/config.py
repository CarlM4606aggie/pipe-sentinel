"""Configuration loading for pipe-sentinel."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class PipelineConfig:
    name: str
    command: str
    retries: int = 0
    timeout: int = 300
    recipients: List[str] = field(default_factory=list)
    max_age_minutes: Optional[int] = None


@dataclass
class SmtpConfig:
    host: str
    port: int
    username: str
    password: str
    from_address: str


@dataclass
class SentinelConfig:
    smtp: SmtpConfig
    pipelines: List[PipelineConfig]
    db_path: str = "audit.db"
    dry_run: bool = False


def _parse_smtp(raw: Dict[str, Any]) -> SmtpConfig:
    return SmtpConfig(
        host=raw["host"],
        port=int(raw["port"]),
        username=raw["username"],
        password=raw["password"],
        from_address=raw["from_address"],
    )


def _parse_pipeline(raw: Dict[str, Any]) -> PipelineConfig:
    return PipelineConfig(
        name=raw["name"],
        command=raw["command"],
        retries=int(raw.get("retries", 0)),
        timeout=int(raw.get("timeout", 300)),
        recipients=raw.get("recipients", []),
        max_age_minutes=raw.get("max_age_minutes", None),
    )


def load_config(path: str) -> SentinelConfig:
    """Load and parse a sentinel YAML configuration file."""
    with open(path, "r") as fh:
        raw = yaml.safe_load(fh)

    smtp = _parse_smtp(raw["smtp"])
    pipelines = [_parse_pipeline(p) for p in raw.get("pipelines", [])]
    db_path = raw.get("db_path", "audit.db")

    return SentinelConfig(smtp=smtp, pipelines=pipelines, db_path=db_path)
