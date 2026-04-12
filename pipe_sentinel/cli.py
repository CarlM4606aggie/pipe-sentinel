"""CLI entry point for pipe-sentinel."""

import sys
import argparse
from pathlib import Path

from pipe_sentinel.config import load_config
from pipe_sentinel.scheduler import run_all


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipe-sentinel",
        description="Monitor and alert on ETL pipeline failures.",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("sentinel.yml"),
        help="Path to the YAML configuration file (default: sentinel.yml).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipelines but skip sending alert notifications.",
    )
    parser.add_argument(
        "--pipeline",
        metavar="NAME",
        default=None,
        help="Run only the pipeline with this name.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config_path: Path = args.config
    if not config_path.exists():
        print(f"[pipe-sentinel] Config file not found: {config_path}", file=sys.stderr)
        return 2

    sentinel_cfg = load_config(config_path)

    pipelines = sentinel_cfg.pipelines
    if args.pipeline:
        pipelines = [p for p in pipelines if p.name == args.pipeline]
        if not pipelines:
            print(
                f"[pipe-sentinel] No pipeline named '{args.pipeline}' found.",
                file=sys.stderr,
            )
            return 2

    smtp_cfg = None if args.dry_run else sentinel_cfg.smtp
    report = run_all(pipelines, smtp_cfg)

    for result in report.results:
        status = "OK" if result.success else "FAIL"
        print(f"  [{status}] {result.pipeline_name}")
        if not result.success and result.stderr:
            print(f"         stderr: {result.stderr.strip()[:120]}")

    if args.dry_run and not report.all_passed:
        print("[pipe-sentinel] Dry-run: alerts suppressed.")

    return 0 if report.all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
