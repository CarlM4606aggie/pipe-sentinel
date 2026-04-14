"""Webhook notification support for pipeline failure alerts."""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from pipe_sentinel.runner import RunResult


@dataclass
class WebhookConfig:
    url: str
    method: str = "POST"
    headers: dict[str, str] = field(default_factory=lambda: {"Content-Type": "application/json"})
    timeout: int = 10


@dataclass
class WebhookResult:
    url: str
    status_code: int | None
    success: bool
    error: str | None = None


def build_payload(result: RunResult) -> dict[str, Any]:
    """Build a JSON-serialisable payload from a RunResult."""
    return {
        "pipeline": result.pipeline_name,
        "status": "failure" if result.returncode != 0 else "success",
        "returncode": result.returncode,
        "duration_seconds": round(result.duration, 3),
        "stdout": result.stdout,
        "stderr": result.stderr,
        "attempts": result.attempts,
    }


def send_webhook(cfg: WebhookConfig, payload: dict[str, Any]) -> WebhookResult:
    """POST *payload* to the configured webhook URL."""
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        cfg.url,
        data=body,
        headers=cfg.headers,
        method=cfg.method,
    )
    try:
        with urllib.request.urlopen(req, timeout=cfg.timeout) as resp:
            return WebhookResult(url=cfg.url, status_code=resp.status, success=True)
    except urllib.error.HTTPError as exc:
        return WebhookResult(url=cfg.url, status_code=exc.code, success=False, error=str(exc))
    except Exception as exc:  # noqa: BLE001
        return WebhookResult(url=cfg.url, status_code=None, success=False, error=str(exc))


def notify_webhook(cfg: WebhookConfig, result: RunResult) -> WebhookResult:
    """Build payload from *result* and send it to the webhook."""
    payload = build_payload(result)
    return send_webhook(cfg, payload)
