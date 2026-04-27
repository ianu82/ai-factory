from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .linear_workflow import LinearWorkflowError, LinearWorkflowSync
from .reliability import (
    OperationReaper,
    RecoveryManager,
    ReliabilityError,
    operation_stale_seconds_from_env,
)


def register_reliability_commands(subparsers: argparse._SubParsersAction) -> None:
    reaper_parser = subparsers.add_parser(
        "factory-reap-stale-operations",
        help="Mark stale operation heartbeats or expired run leases as stuck.",
    )
    reaper_parser.add_argument(
        "--store-dir",
        type=Path,
        default=Path(".factory-automation"),
        help="Directory where automation state and run bundles are persisted.",
    )
    reaper_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root for optional Linear sync.",
    )
    reaper_parser.add_argument(
        "--stale-seconds",
        type=float,
        default=None,
        help="Override the operation heartbeat stale threshold.",
    )

    retry_parser = subparsers.add_parser(
        "factory-retry",
        help="Clear a stuck run for another scheduler attempt.",
    )
    retry_parser.add_argument("--work-item-id", required=True)
    retry_parser.add_argument("--reason", default=None)
    retry_parser.add_argument(
        "--store-dir",
        type=Path,
        default=Path(".factory-automation"),
        help="Directory where automation state and run bundles are persisted.",
    )
    retry_parser.add_argument("--repo-root", type=Path, default=None)

    unblock_parser = subparsers.add_parser(
        "factory-unblock",
        help="Clear operator-blocked recovery state without forcing an immediate retry.",
    )
    unblock_parser.add_argument("--work-item-id", required=True)
    unblock_parser.add_argument("--reason", default=None)
    unblock_parser.add_argument(
        "--store-dir",
        type=Path,
        default=Path(".factory-automation"),
        help="Directory where automation state and run bundles are persisted.",
    )
    unblock_parser.add_argument("--repo-root", type=Path, default=None)

    dead_letter_parser = subparsers.add_parser(
        "factory-dead-letter",
        help="Move a run out of scheduler rotation with an operator reason.",
    )
    dead_letter_parser.add_argument("--work-item-id", required=True)
    dead_letter_parser.add_argument("--reason", required=True)
    dead_letter_parser.add_argument(
        "--store-dir",
        type=Path,
        default=Path(".factory-automation"),
        help="Directory where automation state and run bundles are persisted.",
    )
    dead_letter_parser.add_argument("--repo-root", type=Path, default=None)


def handle_reliability_command(args: argparse.Namespace) -> int | None:
    if args.command == "factory-reap-stale-operations":
        if args.stale_seconds is not None and args.stale_seconds <= 0:
            print(
                "Factory stale-operation reaper failed: --stale-seconds must be > 0",
                file=sys.stderr,
            )
            return 1
        try:
            linear_sync = LinearWorkflowSync.maybe_create(
                args.store_dir,
                repo_root_override=args.repo_root,
            )
            result = OperationReaper(
                args.store_dir,
                stale_seconds=args.stale_seconds or operation_stale_seconds_from_env(),
                linear_sync=linear_sync,
            ).run()
        except (ReliabilityError, LinearWorkflowError) as exc:
            print(f"Factory stale-operation reaper failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command in {"factory-retry", "factory-unblock", "factory-dead-letter"}:
        try:
            linear_sync = LinearWorkflowSync.maybe_create(
                args.store_dir,
                repo_root_override=args.repo_root,
            )
            recovery = RecoveryManager(args.store_dir, linear_sync=linear_sync)
            if args.command == "factory-retry":
                result = recovery.retry(args.work_item_id, reason=args.reason)
            elif args.command == "factory-unblock":
                result = recovery.unblock(args.work_item_id, reason=args.reason)
            else:
                result = recovery.dead_letter(args.work_item_id, reason=args.reason)
        except (ReliabilityError, LinearWorkflowError) as exc:
            print(f"Factory recovery command failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2))
        return 0

    return None
