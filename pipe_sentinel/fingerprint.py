"""Failure fingerprinting: group repeated failures by their error signature."""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass
class FingerprintResult:
    pipeline: str
    fingerprint: str
    sample_stderr: str
    occurrences: int
    is_recurring: bool

    def __str__(self) -> str:
        tag = "RECURRING" if self.is_recurring else "NEW"
        return (
            f"[{tag}] {self.pipeline} | fp={self.fingerprint[:8]} "
            f"| occurrences={self.occurrences}"
        )


@dataclass
class FingerprintReport:
    results: List[FingerprintResult] = field(default_factory=list)

    @property
    def recurring(self) -> List[FingerprintResult]:
        return [r for r in self.results if r.is_recurring]

    @property
    def new_failures(self) -> List[FingerprintResult]:
        return [r for r in self.results if not r.is_recurring]


def _normalise(stderr: str) -> str:
    """Strip volatile parts (line numbers, timestamps, hex addresses)."""
    text = re.sub(r'line \d+', 'line N', stderr)
    text = re.sub(r'0x[0-9a-fA-F]+', '0xADDR', text)
    text = re.sub(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}', 'TIMESTAMP', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def compute_fingerprint(stderr: str) -> str:
    normalised = _normalise(stderr)
    return hashlib.sha256(normalised.encode()).hexdigest()


def detect_fingerprint(
    pipeline: str,
    stderr: str,
    history: Dict[str, int],
    recurrence_threshold: int = 2,
) -> FingerprintResult:
    fp = compute_fingerprint(stderr)
    count = history.get(fp, 0) + 1
    return FingerprintResult(
        pipeline=pipeline,
        fingerprint=fp,
        sample_stderr=stderr[:200],
        occurrences=count,
        is_recurring=count >= recurrence_threshold,
    )


def scan_fingerprints(
    failures: List[Dict],
    history: Optional[Dict[str, int]] = None,
    recurrence_threshold: int = 2,
) -> FingerprintReport:
    """failures: list of dicts with 'pipeline' and 'stderr' keys."""
    hist: Dict[str, int] = history or {}
    results = [
        detect_fingerprint(
            f["pipeline"], f.get("stderr", ""), hist, recurrence_threshold
        )
        for f in failures
    ]
    return FingerprintReport(results=results)
