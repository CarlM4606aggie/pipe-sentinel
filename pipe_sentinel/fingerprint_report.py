"""Formatting helpers for FingerprintReport."""
from __future__ import annotations

from pipe_sentinel.fingerprint import FingerprintResult, FingerprintReport


def _icon(result: FingerprintResult) -> str:
    return "🔁" if result.is_recurring else "🆕"


def format_fingerprint_result(result: FingerprintResult) -> str:
    lines = [
        f"{_icon(result)} {result.pipeline}",
        f"   fingerprint : {result.fingerprint[:16]}...",
        f"   occurrences : {result.occurrences}",
    ]
    if result.sample_stderr:
        snippet = result.sample_stderr[:80].replace("\n", " ")
        lines.append(f"   stderr      : {snippet}")
    return "\n".join(lines)


def build_fingerprint_report(report: FingerprintReport) -> str:
    if not report.results:
        return "No failure fingerprints detected."

    sections: list[str] = []
    sections.append(
        f"Failure Fingerprints  "
        f"[total={len(report.results)}, "
        f"recurring={len(report.recurring)}, "
        f"new={len(report.new_failures)}]"
    )
    sections.append("-" * 60)
    for r in report.results:
        sections.append(format_fingerprint_result(r))
    return "\n".join(sections)


def print_fingerprint_report(report: FingerprintReport) -> None:
    print(build_fingerprint_report(report))
