"""Collect roster data from sentinel config."""
from __future__ import annotations
from typing import Any, Dict, List
from pipe_sentinel.roster import Roster, RosterEntry, build_roster, owners_for_failures
from pipe_sentinel.runner import RunResult


def roster_from_config(config: Any) -> Roster:
    """Build a Roster from sentinel config object.

    Expects config.roster to be a list of dicts with keys:
      pipeline, team, owners (optional), slack_channel (optional)
    """
    raw: List[Dict] = getattr(config, "roster", []) or []
    return build_roster(raw)


def failed_pipeline_names(results: List[RunResult]) -> List[str]:
    return [r.pipeline for r in results if not r.success]


def owners_for_results(roster: Roster, results: List[RunResult]) -> Dict[str, List[str]]:
    failed = failed_pipeline_names(results)
    return owners_for_failures(roster, failed)
