"""Human-readable report of pipeline groupings by tag."""
from __future__ import annotations

from typing import List

from pipe_sentinel.config import PipelineConfig
from pipe_sentinel.tags import build_tag_index, pipelines_by_tag

_HEADER = "Pipeline Tag Report"
_SEP = "-" * 40


def format_tag_report(pipelines: List[PipelineConfig]) -> str:
    """Return a formatted string listing pipelines grouped by tag."""
    by_tag = pipelines_by_tag(pipelines)

    if not by_tag:
        return f"{_HEADER}\n{_SEP}\n(no tags defined)\n"

    lines: List[str] = [_HEADER, _SEP]
    for tag in sorted(by_tag):
        tagged = by_tag[tag]
        lines.append(f"[{tag}]  ({len(tagged)} pipeline(s))")
        for p in sorted(tagged, key=lambda x: x.name):
            lines.append(f"  • {p.name}")
    lines.append(_SEP)
    return "\n".join(lines) + "\n"


def format_untagged(pipelines: List[PipelineConfig]) -> str:
    """Return a formatted string listing pipelines that carry no tags."""
    untagged = [p for p in pipelines if not p.tags]
    if not untagged:
        return "All pipelines have at least one tag.\n"
    lines = ["Untagged pipelines:"]
    for p in sorted(untagged, key=lambda x: x.name):
        lines.append(f"  • {p.name}")
    return "\n".join(lines) + "\n"


def print_tag_report(pipelines: List[PipelineConfig]) -> None:  # pragma: no cover
    """Print the tag report and untagged list to stdout."""
    print(format_tag_report(pipelines))
    print(format_untagged(pipelines))
