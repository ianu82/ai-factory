"""Product delivery entry points, independent of legacy staged CLI handlers."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from .linear_trigger import LinearTriggerConfig
from .product_delivery import ProductDelivery
from .product_harness import resolve_harness
from .product_linear_client import LinearProductClient
from .product_linear_intake import LinearProductRoute
from .product_linear_worker import LinearProductWorker
from .product_profile import ProductProfile
from .product_promotion import GitHubPromotion
from .product_replay import run_suite
from .product_task import ProductTask
from .product_outcomes import outcome_metrics
from .product_improvements import propose_improvements
from .run_journal import RunJournal


def _bounded_int(minimum, maximum):
    def parse(value):
        try:
            number = int(value)
        except ValueError:
            raise argparse.ArgumentTypeError("Expected an integer") from None
        if not minimum <= number <= maximum:
            raise argparse.ArgumentTypeError(f"Must be between {minimum} and {maximum}")
        return number

    return parse


def register_product_commands(subparsers) -> None:
    for name in ["product-validate", "product-deliver", "product-replay"]:
        parser = subparsers.add_parser(name)
        parser.add_argument("--profile", type=Path, required=True)
        parser.add_argument("--target-root", type=Path, required=True)
        if name != "product-validate":
            parser.add_argument("--store-dir", type=Path, default=Path(".factory-automation"))
            parser.add_argument("--env-file", type=Path, help="Explicit controller environment file outside target source")
        if name == "product-deliver":
            parser.add_argument("--task", type=Path, required=True)
            parser.add_argument("--publish", action="store_true", help="Publish a PR; the trusted profile controls automatic merge")
        if name == "product-replay":
            parser.add_argument("--suite", type=Path, required=True)
            parser.add_argument("--repetitions", type=int, default=1)
    linear = subparsers.add_parser("product-linear-worker")
    linear.add_argument("--profile", type=Path, required=True)
    linear.add_argument("--route", type=Path, required=True)
    linear.add_argument("--target-root", type=Path, required=True)
    linear.add_argument("--store-dir", type=Path, required=True)
    linear.add_argument("--env-file", type=Path, help="Explicit controller environment file outside target source")
    linear.add_argument("--once", action="store_true")
    linear.add_argument("--poll-seconds", type=_bounded_int(5, 3600), default=60)
    linear.add_argument("--max-issues", type=_bounded_int(1, 100), default=1)
    linear.add_argument("--runner", choices=["docker", "railway"], default="docker")
    inspect = subparsers.add_parser("product-inspect")
    inspect.add_argument("--store-dir", type=Path, required=True)
    inspect.add_argument("--run-id", required=True)
    inspect.add_argument("--events", action="store_true")
    metrics = subparsers.add_parser("product-metrics")
    metrics.add_argument("--store-dir", type=Path, required=True)
    improvements = subparsers.add_parser("product-improvements")
    improvements.add_argument("--store-dir", type=Path, required=True)
    improvements.add_argument("--min-failures", type=int, default=3)
    improvements.add_argument("--limit", type=int, default=3)
    improvements.add_argument("--cooldown-hours", type=float, default=24)


def handle_product_command(args, *, parse_env) -> int | None:
    if not args.command.startswith("product-"):
        return None
    try:
        if args.command == "product-improvements":
            result = {"proposals": propose_improvements(args.store_dir, min_failures=args.min_failures,
                limit=args.limit, cooldown_hours=args.cooldown_hours)}
        elif args.command == "product-metrics":
            result = outcome_metrics(args.store_dir)
        elif args.command == "product-inspect":
            journal = RunJournal(args.store_dir, args.run_id, read_only=True)
            result = journal.summary()
            if args.events:
                result["timeline"] = journal.events()
        else:
            profile = ProductProfile.load(args.profile, target_root=args.target_root)
            profile.validate_repository(args.target_root)
            if args.command == "product-linear-worker":
                route = LinearProductRoute.load(args.route, target_root=args.target_root)
                if profile.document.get("auto_merge") is not True:
                    raise ValueError("Automatic Linear delivery requires auto_merge")
                if (not profile.document.get("required_status_checks")
                        or any(not name.strip() for name in profile.document["required_status_checks"])):
                    raise ValueError("Automatic Linear delivery requires named GitHub CI checks")
                target, store = args.target_root.resolve(), args.store_dir.resolve()
                if store.is_relative_to(target) or target.is_relative_to(store):
                    raise ValueError("State store must be outside candidate target")
            env_file = getattr(args, "env_file", None)
            if env_file:
                if env_file.resolve().is_relative_to(args.target_root.resolve()):
                    raise ValueError("Controller environment file must be outside candidate source")
                for name, value in parse_env(env_file).items():
                    if (
                        (args.command == "product-linear-worker" and name == "LINEAR_API_KEY")
                        or name.startswith("AI_FACTORY_OPENAI_") or name in {
                            "OPENAI_API_KEY", "OPENAI_ORGANIZATION", "OPENAI_PROJECT", "GH_TOKEN", "GITHUB_TOKEN",
                        }
                    ):
                        os.environ.setdefault(name, value)
            if args.command == "product-linear-worker":
                api_key = os.environ.get("LINEAR_API_KEY", "").strip()
                if not api_key:
                    raise ValueError("LINEAR_API_KEY is required")
                config = LinearTriggerConfig(
                    target_team_id=route.document["team_id"],
                    target_state_id=route.document["queued_state_id"],
                    api_key=api_key,
                )
                client = LinearProductClient(config)
                options = {}
                if args.runner == "railway":
                    from .railway_runner import RailwayDelivery
                    options["delivery_factory"] = RailwayDelivery
                worker = LinearProductWorker(profile, args.target_root, args.store_dir, route, client, **options)
                while True:
                    results = worker.run_cycle(max_issues=args.max_issues)
                    print(json.dumps({"runner": args.runner, "results": results}), flush=True)
                    if args.once:
                        blocked = any(item.get("status") in {"blocked", "pending", "conflict"} for item in results)
                        return 1 if blocked else 0
                    time.sleep(args.poll_seconds)
            if args.command == "product-validate":
                document = profile.document
                result = {"status": "valid", "profile_sha": profile.digest, "repository": document["repository"],
                          "builder_model": document["builder_model"], "reviewer_model": document["reviewer_model"],
                          "builder_harness": resolve_harness(document, "builder"),
                          "reviewer_harness": resolve_harness(document, "verifier")}
            elif args.command == "product-replay":
                result = run_suite(profile, args.target_root, args.store_dir, args.suite, repetitions=args.repetitions)
            else:
                task = ProductTask.load(args.task, target_root=args.target_root)
                delivery = ProductDelivery(profile, args.target_root, args.store_dir)
                def publish(candidate, journal):
                    return GitHubPromotion(profile, task, args.target_root, args.store_dir,
                        smoke=delivery.smoke, deadline=delivery.deadline)(candidate, journal)
                result = delivery.run(task, promote=publish if args.publish else None)
        print(json.dumps(result, indent=2))
        return 1 if result.get("total") is not None and result["correct"] != result["total"] else 0
    except KeyboardInterrupt:
        if args.command == "product-linear-worker":
            return 0
        raise
    except Exception as exc:
        if args.command == "product-linear-worker":
            print("Product Linear workflow failed; inspect controller configuration and evidence", file=sys.stderr)
            return 1
        if not isinstance(exc, (ValueError, OSError, RuntimeError)):
            raise
        print(f"Product workflow failed: {exc}", file=sys.stderr)
        return 1
