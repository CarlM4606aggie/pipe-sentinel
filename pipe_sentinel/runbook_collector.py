"""Collect runbook data from SentinelConfig and wire it to failures."""
from __future__ import annotations

from typing import List

from pipe_sentinel.runbook import RunbookEntry, RunbookIndex, build_runbook_index, runbook_for_failures


def index_from_config(config) -> RunbookIndex:
    """Build a RunbookIndex from SentinelConfig.

    Expects each PipelineConfig to optionally carry `runbook_url` and
    `runbook_notes` attributes (gracefully ignored when absent).
    """
    raw: List[dict] = []
    for pipeline in config.pipelines:
        url = getattr(pipeline, "runbook_url", None)
        notes = getattr(pipeline, "runbook_notes", None)
        if url or notes:
            raw.append({"pipeline": pipeline.name, "url": url, "notes": notes})
    return build_runbook_index(raw)


def entries_for_failures(config, failed_names: List[str]) -> List[RunbookEntry]:
    """Return runbook entries for the given failed pipeline names."""
    index = index_from_config(config)
    return runbook_for_failures(index, failed_names)
