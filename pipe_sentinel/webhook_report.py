"""Formatting helpers for webhook delivery results."""
from __future__ import annotations

from pipe_sentinel.webhook import WebhookResult


def _icon(success: bool) -> str:
    return "\u2705" if success else "\u274c"


def format_webhook_result(result: WebhookResult) -> str:
    """Return a single-line summary of a WebhookResult."""
    icon = _icon(result.success)
    code_part = f"  HTTP {result.status_code}" if result.status_code is not None else ""
    error_part = f"  error={result.error}" if result.error else ""
    return f"{icon} {result.url}{code_part}{error_part}"


def build_webhook_report(results: list[WebhookResult]) -> str:
    """Return a multi-line report for a list of WebhookResults."""
    if not results:
        return "No webhooks configured."
    lines = ["Webhook Delivery Report", "=" * 40]
    succeeded = sum(1 for r in results if r.success)
    lines.append(f"Delivered: {succeeded}/{len(results)}")
    lines.append("")
    for r in results:
        lines.append(format_webhook_result(r))
    return "\n".join(lines)


def print_webhook_report(results: list[WebhookResult]) -> None:  # pragma: no cover
    print(build_webhook_report(results))
