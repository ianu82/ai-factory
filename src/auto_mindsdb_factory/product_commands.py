"""Product delivery entry points, independent of legacy staged CLI handlers."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
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
from .product_cancellation import request_cancellation
from .product_inference import InferencePolicy, require_inference_execution


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
    inference = subparsers.add_parser("product-inference-check")
    for name in ("profile", "target-root"):
        inference.add_argument("--" + name, type=Path, required=True)
    inference.add_argument("--auth-workspace-id")
    inference.add_argument("--env-file", type=Path)
    inference.add_argument("--codex-qualification", action="store_true")
    handoff = subparsers.add_parser("product-github-handoff")
    for name in ("request", "receipts-dir", "target-root"):
        handoff.add_argument("--" + name, type=Path, required=True)
    for name in ("repository", "run-id", "base-sha", "candidate-sha"):
        handoff.add_argument("--" + name, required=True)
    handoff.add_argument("--base-branch", default="main")
    handoff.add_argument("--bundle", type=Path)
    handoff.add_argument("--bundle-sha256")
    cancel = subparsers.add_parser("product-cancel")
    cancel.add_argument("--store-dir", type=Path, required=True)
    cancel.add_argument("--run-id", required=True)
    for name in ["product-validate", "product-deliver", "product-replay"]:
        parser = subparsers.add_parser(name)
        parser.add_argument("--profile", type=Path, required=True)
        parser.add_argument("--target-root", type=Path, required=True)
        parser.add_argument("--codex-qualification", action="store_true",
                            help="Explicit manual certification lane; requires a matching trusted profile")
        if name != "product-validate":
            parser.add_argument("--store-dir", type=Path, default=Path(".factory-automation"))
            parser.add_argument("--env-file", type=Path, help="Explicit controller environment file outside target source")
            parser.add_argument("--runner", choices=["docker", "railway"], default="docker")
        if name == "product-deliver":
            parser.add_argument("--task", type=Path, required=True)
            parser.add_argument("--publish", action="store_true", help="Publish a PR; the trusted profile controls automatic merge")
            parser.add_argument("--base-sha", help="Full preloaded commit SHA; remote promotion still requires exact-base CAS")
            parser.add_argument("--github-transport", choices=["controller", "operator-handoff"], default="controller")
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
        if args.command == "product-github-handoff":
            from .github_handoff import PublicationScope
            from .github_handoff_channel import read_document
            from .github_handoff_executor import execute_request
            scope = PublicationScope(args.repository, args.run_id, args.base_sha, args.candidate_sha, args.base_branch)
            request = read_document(args.request)
            scope.validate_request(request)
            args.receipts_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            result = execute_request(scope, request, args.target_root,
                args.receipts_dir / (request["id"] + ".response.json"),
                bundle=args.bundle, bundle_sha256=args.bundle_sha256)
        elif args.command == "product-inference-check":
            from .product_inference_auth import check_credential, require_auth_workspace, require_controller_import_path
            require_controller_import_path(args.target_root)
            profile = ProductProfile.load(args.profile, target_root=args.target_root,
                codex_qualification=args.codex_qualification)
            profile.validate_repository(args.target_root)
            policy = InferencePolicy.from_document(profile.document.get("inference"))
            require_auth_workspace(policy, args.auth_workspace_id)
            key = os.environ.get("AI_FACTORY_MINDSHUB_API_KEY", "")
            if args.env_file:
                if args.env_file.resolve().is_relative_to(args.target_root.resolve()):
                    raise ValueError("Controller environment file must be outside candidate source")
                try:
                    supplied = parse_env(args.env_file)
                except Exception:
                    raise ValueError("Could not read MindsHub controller credential configuration") from None
                key = key or supplied.get("AI_FACTORY_MINDSHUB_API_KEY", "")
            result = asyncio.run(check_credential(policy, key, args.auth_workspace_id))
        elif args.command == "product-cancel":
            result = request_cancellation(args.store_dir, args.run_id)
        elif args.command == "product-improvements":
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
            profile = ProductProfile.load(args.profile, target_root=args.target_root,
                codex_qualification=getattr(args, "codex_qualification", False))
            profile.validate_repository(args.target_root)
            if args.command != "product-validate":
                require_inference_execution(profile.document)
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
                if any(resolve_harness(document, role) == "anton" for role in ("builder", "verifier")):
                    from .anton_session import validate_anton_installation
                    validate_anton_installation()
                if any(resolve_harness(document, role) == "codex" for role in ("builder", "verifier")):
                    from .codex_session import validate_codex_installation
                    validate_codex_installation()
                result = {"status": "valid", "profile_sha": profile.digest, "repository": document["repository"],
                          "builder_model": document["builder_model"], "reviewer_model": document["reviewer_model"],
                          "builder_harness": resolve_harness(document, "builder"),
                          "reviewer_harness": resolve_harness(document, "verifier")}
                if "execution_lane" in document:
                    result["execution_lane"] = document["execution_lane"]
                if "inference" in document:
                    result["inference"] = InferencePolicy.from_document(document["inference"]).inspect()
            elif args.command == "product-replay":
                delivery_factory = ProductDelivery
                if args.runner == "railway":
                    from .railway_runner import RailwayDelivery
                    delivery_factory = RailwayDelivery
                result = run_suite(profile, args.target_root, args.store_dir, args.suite,
                    repetitions=args.repetitions, delivery_factory=delivery_factory)
            else:
                if args.base_sha is not None and not re.fullmatch(r"[0-9a-f]{40}", args.base_sha):
                    raise ValueError("Preloaded base requires a full commit SHA")
                if args.github_transport == "operator-handoff" and not args.publish:
                    raise ValueError("Operator GitHub handoff requires --publish")
                task = ProductTask.load(args.task, target_root=args.target_root)
                delivery_factory = ProductDelivery
                if args.runner == "railway":
                    from .railway_runner import RailwayDelivery
                    delivery_factory = RailwayDelivery
                delivery = delivery_factory(profile, args.target_root, args.store_dir)
                delivery.publication_transport = "operator-handoff-v1" if args.github_transport == "operator-handoff" else "controller-github-v1"
                def publish(candidate, journal):
                    delivery.check_cancelled()
                    transport = None
                    if args.github_transport == "operator-handoff":
                        from .github_handoff_channel import GitHubHandoff
                        transport = GitHubHandoff(profile, task, args.target_root,
                            deadline=delivery.deadline, check_cancelled=delivery.check_cancelled)
                    return GitHubPromotion(profile, task, args.target_root, args.store_dir,
                        smoke=delivery.smoke, deadline=delivery.deadline,
                        before_merge=delivery.check_cancelled, transport=transport)(candidate, journal)
                result = delivery.run(task, promote=publish if args.publish else None, base_sha=args.base_sha)
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
