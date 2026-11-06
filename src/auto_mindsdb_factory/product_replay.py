"""Frozen, side-effect-free delivery/verification comparisons with retained scores."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from uuid import uuid4

from jsonschema import validate, ValidationError

from .automation import FactoryRunStore
from .intake import repo_root
from .product_delivery import ProductDelivery, VerificationFailed
from .product_agent import RoleSnapshotError
from .product_profile import ProductProfile
from .product_planning import planning_manifest
from .product_execution import execution_manifest
from .product_roles import snapshot_roles
from .product_task import ProductTask, TASK_SCHEMA
from .product_workspace import git
from .reliability import OperationTracker, write_json_atomic
from .run_journal import RunJournal


SHA = {"type": "string", "pattern": "^[0-9a-f]{40}$"}
SUITE_SCHEMA = {
    "type": "object", "additionalProperties": False, "required": ["version", "cases"],
    "properties": {"version": {"const": 1}, "cases": {"type": "array", "minItems": 1, "items": {
        "type": "object", "additionalProperties": False,
        "required": ["name", "mode", "base_sha", "task", "expected"],
        "properties": {"name": {"type": "string", "minLength": 1}, "mode": {"enum": ["build", "verify"]},
            "base_sha": SHA, "candidate_sha": SHA, "task": TASK_SCHEMA, "expected": {"enum": ["verified", "rejected"]}},
        "allOf": [{"if": {"properties": {"mode": {"const": "verify"}}}, "then": {"required": ["candidate_sha"]}}],
    }}},
}


def load_suite(path: Path, target: Path) -> dict:
    if path.resolve().is_relative_to(target.resolve()):
        raise ValueError("Replay expectations must live outside candidate source")
    suite = json.loads(path.read_text(encoding="utf-8"))
    try:
        validate(suite, SUITE_SCHEMA)
    except ValidationError as exc:
        raise ValueError("Invalid replay suite; full commit SHAs are mandatory") from exc
    if len({case["name"] for case in suite["cases"]}) != len(suite["cases"]):
        raise ValueError("Replay case names must be unique")
    for case in suite["cases"]:
        ProductTask.from_document(case["task"])
    return suite


def run_suite(profile: ProductProfile, target: Path, store: Path, suite_path: Path,
              *, repetitions: int = 1, delivery_factory=ProductDelivery) -> dict:
    if not 1 <= repetitions <= 10:
        raise ValueError("Replay repetitions must be between 1 and 10")
    suite = load_suite(suite_path, target)
    suite_sha = hashlib.sha256(json.dumps(suite, sort_keys=True).encode()).hexdigest()
    results = []
    session = uuid4().hex[:12]
    for repetition in range(1, repetitions + 1):
        for case in suite["cases"]:
            # Neither expected outcomes nor case names enter the agent's context.
            run_id = "replay-" + uuid4().hex
            task = ProductTask.from_document({**case["task"], "id": run_id})
            metadata = {"suite_sha": suite_sha, "case": case["name"], "repetition": repetition, "mode": case["mode"],
                        "original_task_sha": ProductTask.from_document(case["task"]).digest}
            delivery = delivery_factory(profile, target, store)
            journal = RunJournal(store, run_id)
            actual, reason = "unverified", None
            try:
                if case["mode"] == "build":
                    # No publication callback: replay can never push, open PRs or merge.
                    result = delivery.run(task, base_sha=case["base_sha"], replay=metadata)
                    actual = result["status"]
                else:
                    delivery.deadline = time.monotonic() + profile.document["max_run_seconds"]
                    locks = FactoryRunStore(store, repo_root_override=repo_root())
                    with locks.run_lease(run_id, ttl_seconds=60, renew_interval_seconds=15):
                        journal.create({"purpose": "replay", "replay": metadata, "verification_only": True,
                            "runtime_sha": git(repo_root(), "rev-parse", "HEAD"), "task_sha": task.digest,
                            "profile": profile.document,
                            "profile_sha": profile.digest, "task": task.document, "base_sha": case["base_sha"],
                            "candidate_sha": case["candidate_sha"], "role_snapshots": snapshot_roles(),
                            **planning_manifest(profile), **execution_manifest(profile.document)})
                        with OperationTracker(store, run_id, "delivery", "replay-verification"):
                            try:
                                journal.append("base_resolved", commit_sha=case["base_sha"])
                                verification = delivery.verify_fixture(task, case["candidate_sha"], case["base_sha"], journal)
                                journal.append("candidate_verified", **verification)
                                actual = "verified"
                            except VerificationFailed as exc:
                                actual, reason = "rejected", str(exc)
                            journal.append("run_finished", status=actual)
            except Exception as exc:
                reason = str(exc) if isinstance(exc, RoleSnapshotError) else type(exc).__name__
                journal.append("run_finished", status="unverified", reason=reason)
            score = {**metadata, "run_id": run_id, "expected": case["expected"], "actual": actual,
                     "correct": actual == case["expected"], "reason": reason, "usage": journal.summary()}
            journal.append("replay_scored", scorer_version=1, **score)
            results.append(score)
    summary = {"version": 1, "suite_sha": suite_sha, "profile_sha": profile.digest,
               "correct": sum(result["correct"] for result in results), "total": len(results), "results": results}
    output = store / f"replay-{session}.json"
    write_json_atomic(output, summary)
    return {**summary, "report_path": str(output)}
