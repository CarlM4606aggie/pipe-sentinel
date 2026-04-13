"""Formatting helpers for circuit breaker state reports."""
from __future__ import annotations

import time
from typing import Dict

from pipe_sentinel.circuit_breaker import CircuitState


def _status_icon(state: CircuitState) -> str:
    if state.is_open:
        return "🔴"
    if state.failures > 0:
        return "🟡"
    return "🟢"


def format_state(state: CircuitState, recovery_seconds: int = 300) -> str:
    icon = _status_icon(state)
    parts = [f"{icon} {state.pipeline_name}  failures={state.failures}"]
    if state.is_open:
        elapsed = time.time() - (state.opened_at or 0)
        remaining = max(0, recovery_seconds - elapsed)
        parts.append(f"OPEN  recovery in {remaining:.0f}s")
    else:
        parts.append("CLOSED")
    return "  ".join(parts)


def build_circuit_report(
    states: Dict[str, CircuitState],
    recovery_seconds: int = 300,
) -> str:
    if not states:
        return "No circuit breaker state recorded."
    lines = ["Circuit Breaker Status", "=" * 40]
    open_count = sum(1 for s in states.values() if s.is_open)
    lines.append(f"Pipelines tracked : {len(states)}")
    lines.append(f"Open circuits     : {open_count}")
    lines.append("")
    for name in sorted(states):
        lines.append(format_state(states[name], recovery_seconds))
    return "\n".join(lines)


def print_circuit_report(
    states: Dict[str, CircuitState],
    recovery_seconds: int = 300,
) -> None:
    print(build_circuit_report(states, recovery_seconds))
