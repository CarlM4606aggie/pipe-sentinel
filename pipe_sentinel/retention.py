"""Audit log retention policy: prune records older than a configured age."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from pipe_sentinel.audit import _connect


@dataclass
class RetentionPolicy:
    """Defines how long audit records should be kept."""
    max_age_days: int

    def cutoff(self, now: Optional[datetime] = None) -> datetime:
        """Return the earliest timestamp that should be retained."""
        if now is None:
            now = datetime.now(tz=timezone.utc)
        return now - timedelta(days=self.max_age_days)


@dataclass
class PruneResult:
    """Summary of a retention pruning operation."""
    rows_deleted: int
    cutoff_ts: datetime

    def __str__(self) -> str:
        ts = self.cutoff_ts.strftime("%Y-%m-%d %H:%M:%S")
        return f"Pruned {self.rows_deleted} record(s) older than {ts} UTC"


def prune_records(
    db_path: str,
    policy: RetentionPolicy,
    now: Optional[datetime] = None,
) -> PruneResult:
    """Delete audit records that exceed the retention policy age.

    Args:
        db_path: Path to the SQLite audit database.
        policy:  Retention policy specifying max age in days.
        now:     Reference time (defaults to current UTC time).

    Returns:
        A :class:`PruneResult` describing how many rows were removed.
    """
    cutoff = policy.cutoff(now)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    with _connect(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM audit_log WHERE timestamp < ?",
            (cutoff_str,),
        )
        conn.commit()
        return PruneResult(rows_deleted=cur.rowcount, cutoff_ts=cutoff)


def apply_retention(db_path: str, max_age_days: int) -> PruneResult:
    """Convenience wrapper: create a policy and prune in one call."""
    policy = RetentionPolicy(max_age_days=max_age_days)
    return prune_records(db_path, policy)
