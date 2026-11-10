"""Command line: `aifactory serve`, `aifactory release-check` and benchmark tools."""

from __future__ import annotations

import argparse
import logging
import os


def main(argv: list[str] | None = None) -> None:
    os.umask(0o077)  # before any control-plane storage is opened
    parser = argparse.ArgumentParser(prog="aifactory")
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="Run the web app, API and orchestrator")
    serve.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    serve.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    benchmarks = sub.add_parser("benchmarks", help="Load JSON benchmark suites or import database history")
    commands = benchmarks.add_subparsers(dest="benchmark_command", required=True)
    load = commands.add_parser("load", help="Load a suite.json snapshot into a workspace (identical content is idempotent)")
    load.add_argument("--workspace", required=True, metavar="ID")
    load.add_argument("--path", required=True, metavar="PATH", help="Suite JSON file or directory containing suite.json")
    history = commands.add_parser("import-history", help="Export completed tasks using only stored database evidence")
    history.add_argument("--workspace", required=True, metavar="ID")
    history.add_argument("--repo", required=True, metavar="owner/name")
    history.add_argument("--suite", required=True, metavar="NAME")
    history.add_argument("--output-root", metavar="PATH",
                         help="Export root (default: aifactory/benchmarks in the current source checkout; required outside a checkout)")
    run = commands.add_parser("run", help="Execute isolated benchmark trials without publishing")
    for flag in ("workspace", "suite", "harness", "model"):
        run.add_argument("--" + flag, required=True)
    run.add_argument("--trials", type=int, required=True)
    release = sub.add_parser("release-check", help="Promote the commit staging runs to the release branch when it is ready")
    release.add_argument("--dry-run", action="store_true", help="Check everything but do not move the release branch")
    args = parser.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.command == "benchmarks":
        from .benchmarks import SuiteError, export_history, load_suite

        try:
            if args.benchmark_command == "run":
                import asyncio
                from .pipeline.benchmarking import run_suite, print_summary
                print_summary(asyncio.run(run_suite(args.workspace, args.suite, args.harness, args.model, args.trials)))
            elif args.benchmark_command == "load":
                suite = load_suite(args.workspace, args.path)
                print(f"Loaded {suite.name} revision {suite.revision}: {len(suite.tasks)} task(s), {suite.content_hash}")
            else:
                destination = export_history(args.workspace, args.repo, args.suite, args.output_root)
                print(f"Exported {destination}")
        except (SuiteError, OSError) as exc:
            parser.error(str(exc))
    if args.command == "release-check":
        import json
        import sys

        from .release import ReleaseError, release_check

        try:
            result = release_check(dry_run=args.dry_run)
        except ReleaseError as exc:
            parser.error(str(exc))
        print(json.dumps(result.as_dict()))
        sys.exit(1 if result.failed else 0)
    if args.command == "serve":
        import uvicorn

        os.umask(0o077)  # control-plane files stay private; job sandboxes set their own umask

        from .app import create_app

        uvicorn.run(create_app(), host=args.host, port=args.port, proxy_headers=True, forwarded_allow_ips="*",
                    log_level=os.environ.get("LOG_LEVEL", "info").lower(), timeout_keep_alive=30)


if __name__ == "__main__":
    main()
