"""Format incident reports for CLI output."""
from __future__ import annotations

from typing import List

from pipe_sentinel.incident import Incident


def _icon(incident: Incident) -> str:
    return "🔥" if incident.is_open else "✅"


def format_incident(inc: Incident) -> str:
    icon = _icon(inc)
    dur = (
        f"{inc.duration_seconds:.0f}s"
        if inc.duration_seconds is not None
        else "ongoing"
    )
    lines = [
        f"{icon} {inc.pipeline}",
        f"   Started : {inc.started_at.isoformat()}",
        f"   Failures: {inc.failure_count}",
        f"   Duration: {dur}",
    ]
    if inc.last_error:
        lines.append(f"   Error   : {inc.last_error}")
    return "\n".join(lines)


def build_incident_report(incidents: List[Incident]) -> str:
    if not incidents:
        return "No incidents detected."
    open_inc = [i for i in incidents if i.is_open]
    closed_inc = [i for i in incidents if not i.is_open]
    sections: List[str] = []
    sections.append(f"Incidents: {len(incidents)} total, {len(open_inc)} open")
    sections.append("-" * 40)
    if open_inc:
        sections.append("OPEN")
        for inc in open_inc:
            sections.append(format_incident(inc))
    if closed_inc:
        sections.append("RESOLVED")
        for inc in closed_inc:
            sections.append(format_incident(inc))
    return "\n".join(sections)


def print_incident_report(incidents: List[Incident]) -> None:
    print(build_incident_report(incidents))
