"""Formatting helpers for label index output."""
from __future__ import annotations

from typing import Dict, List

from pipe_sentinel.label import LabelIndex, LabelSet


def format_label_set(ls: LabelSet) -> str:
    if not ls.labels:
        return f"  {ls.pipeline}: (no labels)"
    pairs = ", ".join(f"{k}={v}" for k, v in sorted(ls.labels.items()))
    return f"  {ls.pipeline}: {pairs}"


def build_label_report(
    index: LabelIndex,
    selector: Dict[str, str] | None = None,
) -> str:
    lines: List[str] = []
    if selector:
        matched = index.select(selector)
        sel_str = ", ".join(f"{k}={v}" for k, v in sorted(selector.items()))
        lines.append(f"Pipelines matching [{sel_str}]: {len(matched)}")
        for name in sorted(matched):
            ls = index.for_pipeline(name)
            if ls:
                lines.append(format_label_set(ls))
    else:
        pipelines = index.all_pipelines()
        lines.append(f"Label index — {len(pipelines)} pipeline(s)")
        for name in sorted(pipelines):
            ls = index.for_pipeline(name)
            if ls:
                lines.append(format_label_set(ls))
    return "\n".join(lines)


def print_label_report(
    index: LabelIndex,
    selector: Dict[str, str] | None = None,
) -> None:
    print(build_label_report(index, selector))
