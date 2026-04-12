"""CLI entry point for pipe-sentinel."""

from __future__ import annotations

import argparse
import sys
from typing import Optional, List

from pipe_sentinel.config import load_config
from pipe_sentinel.scheduler import run_all
from pipe_sentinel.health import run_health_checks, print_health_report


DEFAULT_CONFIG = "sentinel.yml"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipe-sentinel",
        description="Monitor and alert on ETL pipeline failures.",
    )
    parser.add_argument(
        "-c", "--config",
        default=DEFAULT_CONFIG,
        help="Path to sentinel YAML config (default: sentinel.yml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print pipelines that would run without executing them.",
    )
    parser.add_argument(
        "--pipeline",
        dest="pipeline_filter",
        metavar="NAME",
        default=None,
        help="Run only the named pipeline.",
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("health", help="Run health checks on configured pipelines.")
    subparsers.add_parser("run", help="Execute all (or filtered) pipelines.")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
    except FileNotFoundError:
        print(f"Error: config file '{args.config}' not found.", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading config: {exc}", file=sys.stderr)
        return 1

    pipelines = config.pipelines
    if args.pipeline_filter:
        pipelines = [p for p in pipelines if p.name == args.pipeline_filter]
        if not pipelines:
            print(f"No pipeline named '{args.pipeline_filter}'.", file=sys.stderr)
            return 1

    if args.command == "health":
        from pipe_sentinel.config import SentinelConfig
        filtered_cfg = SentinelConfig(smtp=config.smtp, pipelines=pipelines)
        results = run_health_checks(filtered_cfg)
        print_health_report(results)
        return 0 if all(r.healthy for r in results) else 2

    if args.dry_run:
        for p in pipelines:
            print(f"[dry-run] {p.name}: {p.command}")
        return 0

    report = run_all(pipelines, config.smtp)
    return 0 if report.all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
