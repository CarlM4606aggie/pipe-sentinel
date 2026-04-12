"""SQLite-backed audit log for pipeline run results."""

import sqlite3
from dataclasses import dataclass
from typing import List, Optional
from pipe_sentinel.runner import RunResult


@dataclass
class AuditRecord:
    id: Optional[int]
    pipeline_name: str
    status: str
    ran_at: str
    duration: Optional[float]
    retries: int
    error: Optional[str]


def _connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection with row factory enabled."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str) -> None:
    """Create the audit_runs table if it does not already exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_runs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_name TEXT NOT NULL,
                status      TEXT NOT NULL,
                ran_at      TEXT NOT NULL,
                duration    REAL,
                retries     INTEGER NOT NULL DEFAULT 0,
                error       TEXT
            )
            """
        )
        conn.commit()


def record_run(db_path: str, result: RunResult) -> None:
    """Persist a RunResult to the audit log."""
    status = "success" if result.success else "failure"
    error_msg = result.stderr.strip() if result.stderr else None
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO audit_runs (pipeline_name, status, ran_at, duration, retries, error)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                result.pipeline_name,
                status,
                result.started_at.isoformat() if result.started_at else "",
                result.duration,
                result.attempts - 1,
                error_msg,
            ),
        )
        conn.commit()


def fetch_recent(db_path: str, limit: int = 50) -> List[AuditRecord]:
    """Return the most recent audit records, newest first."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, pipeline_name, status, ran_at, duration, retries, error
            FROM audit_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [
        AuditRecord(
            id=row["id"],
            pipeline_name=row["pipeline_name"],
            status=row["status"],
            ran_at=row["ran_at"],
            duration=row["duration"],
            retries=row["retries"],
            error=row["error"],
        )
        for row in rows
    ]
