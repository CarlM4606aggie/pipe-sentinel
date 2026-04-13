"""Formatting helpers for suppression rule reports."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from pipe_sentinel.suppression import SuppressionRule


def _expires_label(rule: SuppressionRule, now: datetime) -> str:
    if rule.expires_at is None:
        return "indefinite"
    delta = rule.expires_at - now
    total_seconds = int(delta.total_seconds())
    if total_seconds <= 0:
        return "expired"
    hours, remainder = divmod(total_seconds, 3600)
    minutes = remainder // 60
    if hours > 0:
        return f"{hours}h {minutes}m remaining"
    return f"{minutes}m remaining"


def format_rule(rule: SuppressionRule, now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    expires = _expires_label(rule, now)
    return f"  [{expires:>22}]  {rule.pipeline_name}  —  {rule.reason}"


def build_suppression_report(
    rules: List[SuppressionRule],
    now: datetime | None = None,
) -> str:
    now = now or datetime.now(timezone.utc)
    lines: List[str] = ["=== Active Suppression Rules ==="]
    if not rules:
        lines.append("  (none)")
    else:
        for rule in sorted(rules, key=lambda r: r.pipeline_name):
            lines.append(format_rule(rule, now))
    lines.append(f"  Total: {len(rules)} suppressed pipeline(s)")
    return "\n".join(lines)


def print_suppression_report(
    rules: List[SuppressionRule],
    now: datetime | None = None,
) -> None:
    print(build_suppression_report(rules, now))
