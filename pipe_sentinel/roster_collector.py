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
    """Return a list of pipeline names that did not succeed."""
    return [r.pipeline for r in results if not r.success]


def owners_for_results(roster: Roster, results: List[RunResult]) -> Dict[str, List[str]]:
    """Map each failed pipeline to its list of owners.

    Args:
        roster: The roster containing pipeline ownership information.
        results: The list of run results to inspect for failures.

    Returns:
        A dict mapping pipeline name to a list of owner strings for
        every pipeline that failed. Pipelines with no roster entry
        are omitted from the returned dict.
    """
    failed = failed_pipeline_names(results)
    return owners_for_failures(roster, failed)


def unregistered_failures(roster: Roster, results: List[RunResult]) -> List[str]:
    """Return failed pipeline names that have no entry in the roster.

    Useful for surfacing pipelines that are failing but have not yet
    been assigned an owner in the sentinel configuration.

    Args:
        roster: The roster containing pipeline ownership information.
        results: The list of run results to inspect for failures.

    Returns:
        A list of pipeline names that failed and are absent from the roster.
    """
    failed = failed_pipeline_names(results)
    return [name for name in failed if name not in roster]
