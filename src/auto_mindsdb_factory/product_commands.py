"""Product delivery entry points, independent of legacy staged CLI handlers."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from .product_delivery import ProductDelivery
from .product_profile import ProductProfile
from .product_promotion import GitHubPromotion
from .product_replay import run_suite
from .product_task import ProductTask
from .product_outcomes import outcome_metrics
from .run_journal import RunJournal


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
    inspect = subparsers.add_parser("product-inspect")
    inspect.add_argument("--store-dir", type=Path, required=True)
    inspect.add_argument("--run-id", required=True)
    inspect.add_argument("--events", action="store_true")
    metrics = subparsers.add_parser("product-metrics")
    metrics.add_argument("--store-dir", type=Path, required=True)


def handle_product_command(args, *, parse_env) -> int | None:
    if not args.command.startswith("product-"):
        return None
    try:
        if args.command == "product-metrics":
            result = outcome_metrics(args.store_dir)
        elif args.command == "product-inspect":
            journal = RunJournal(args.store_dir, args.run_id)
            result = journal.summary()
            if args.events:
                result["timeline"] = journal.events()
        else:
            profile = ProductProfile.load(args.profile, target_root=args.target_root)
            profile.validate_repository(args.target_root)
            env_file = getattr(args, "env_file", None)
            if env_file:
                if env_file.resolve().is_relative_to(args.target_root.resolve()):
                    raise ValueError("Controller environment file must be outside candidate source")
                for name, value in parse_env(env_file).items():
                    if name.startswith("AI_FACTORY_OPENAI_") or name in {
                        "OPENAI_API_KEY", "OPENAI_ORGANIZATION", "OPENAI_PROJECT", "GH_TOKEN", "GITHUB_TOKEN",
                    }:
                        os.environ.setdefault(name, value)
            if args.command == "product-validate":
                result = {"status": "valid", "profile_sha": profile.digest, "repository": profile.document["repository"]}
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
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"Product workflow failed: {exc}", file=sys.stderr)
        return 1
