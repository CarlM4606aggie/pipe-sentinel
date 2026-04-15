"""Formatting helpers for quota state output."""
from __future__ import annotations

from typing import List

from pipe_sentinel.quota import QuotaConfig, QuotaState, QuotaStore


def _bar(used: int, limit: int, width: int = 20) -> str:
    if limit == 0:
        return "[" + "-" * width + "]"
    filled = min(int(used / limit * width), width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def format_quota_state(
    pipeline: str,
    cfg: QuotaConfig,
    store: QuotaStore,
) -> str:
    used = store.runs_in_window(pipeline, cfg.window_seconds)
    exceeded = used >= cfg.max_runs
    bar = _bar(used, cfg.max_runs)
    status = "EXCEEDED" if exceeded else "OK"
    return (
        f"  {pipeline}\n"
        f"    window : {cfg.window_seconds}s\n"
        f"    quota  : {used}/{cfg.max_runs} runs  {bar}  [{status}]\n"
    )


def build_quota_report(
    pipelines: List[str],
    cfg: QuotaConfig,
    store: QuotaStore,
) -> str:
    lines = [f"Quota Report  (max {cfg.max_runs} runs / {cfg.window_seconds}s)\n"]
    for name in pipelines:
        lines.append(format_quota_state(name, cfg, store))
    return "".join(lines)


def print_quota_report(
    pipelines: List[str],
    cfg: QuotaConfig,
    store: QuotaStore,
) -> None:
    print(build_quota_report(pipelines, cfg, store), end="")
