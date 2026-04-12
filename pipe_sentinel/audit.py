"""Audit log module for persisting pipeline run history to a local SQLite database."""

import sqlite3
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from pipe_sentinel.runner import RunResult

DEFAULT_DB_PATH = Path("pipe_sentinel_audit.db")


@dataclass
class AuditRecord:
    pipeline_name: str
    success: bool
    exit_code: Optional[int]
    stdout: str
    stderr: str
    attempts: int
    duration_seconds: float
    recorded_at: str


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Create the audit table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_name TEXT NOT NULL,
                success INTEGER NOT NULL,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                attempts INTEGER NOT NULL,
                duration_seconds REAL NOT NULL,
                recorded_at TEXT NOT NULL
            )
            """
        )


def record_run(result: RunResult, db_path: Path = DEFAULT_DB_PATH) -> None:
    """Persist a RunResult to the audit database."""
    init_db(db_path)
    recorded_at = datetime.utcnow().isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO pipeline_runs
                (pipeline_name, success, exit_code, stdout, stderr, attempts, duration_seconds, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.pipeline_name,
                int(result.success),
                result.exit_code,
                result.stdout,
                result.stderr,
                result.attempts,
                result.duration_seconds,
                recorded_at,
            ),
        )


def fetch_recent(pipeline_name: str, limit: int = 10, db_path: Path = DEFAULT_DB_PATH) -> List[AuditRecord]:
    """Return the most recent audit records for a given pipeline."""
    init_db(db_path)
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT pipeline_name, success, exit_code, stdout, stderr, attempts, duration_seconds, recorded_at
            FROM pipeline_runs
            WHERE pipeline_name = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (pipeline_name, limit),
        ).fetchall()
    return [
        AuditRecord(
            pipeline_name=row["pipeline_name"],
            success=bool(row["success"]),
            exit_code=row["exit_code"],
            stdout=row["stdout"] or "",
            stderr=row["stderr"] or "",
            attempts=row["attempts"],
            duration_seconds=row["duration_seconds"],
            recorded_at=row["recorded_at"],
        )
        for row in rows
    ]
