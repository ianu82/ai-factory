"""Command line: `aifactory serve`."""

from __future__ import annotations

import argparse
import logging
import os


def main(argv: list[str] | None = None) -> None:
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
    args = parser.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.command == "benchmarks":
        from .benchmarks import SuiteError, export_history, load_suite

        try:
            if args.benchmark_command == "load":
                suite = load_suite(args.workspace, args.path)
                print(f"Loaded {suite.name} revision {suite.revision}: {len(suite.tasks)} task(s), {suite.content_hash}")
            else:
                destination = export_history(args.workspace, args.repo, args.suite, args.output_root)
                print(f"Exported {destination}")
        except (SuiteError, OSError) as exc:
            parser.error(str(exc))
    if args.command == "serve":
        import uvicorn

        os.umask(0o077)  # control-plane files stay private; job sandboxes set their own umask

        from .app import create_app

        uvicorn.run(create_app(), host=args.host, port=args.port, proxy_headers=True, forwarded_allow_ips="*",
                    log_level=os.environ.get("LOG_LEVEL", "info").lower(), timeout_keep_alive=30)


if __name__ == "__main__":
    main()
