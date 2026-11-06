"""Controller-owned planning phase and retained guidance admission."""

import json
import tempfile
import time
from pathlib import Path

from .connectors import OpenAIResponsesAgentConnector
from .docker_runner import RunnerError
from .product_agent import HarnessStopped
from .product_plan import ProductPlan, freeze_plan, read_plan
from .product_planner import ProductPlanner, PLANNER_VERSION, REPORT_RESERVE_SECONDS
from .product_roles import select_role
from .product_workspace import source_files, executable_paths
from .product_execution import execution_config


class PlanningStopped(HarnessStopped):
    pass


def planning_manifest(profile):
    settings = profile.document
    return {"planner_role_snapshot": select_role("planner", PLANNER_VERSION), "planning_limits": {
        "harness": "native", "model": settings["builder_model"],
        "runner_preparation_budget": "shared_run_deadline",
        "max_turns": min(8, settings["max_agent_turns"]),
        "max_tokens": min(100000, settings["max_total_tokens"]), "max_seconds": 120,
        "last_request_report_only": True, "report_reserve_seconds": REPORT_RESERVE_SECONDS}}


def retained_plan(delivery, task, base, journal):
    """Called before each execution; a self-consistent replacement is still rejected."""
    try:
        manifest = json.loads((journal.root / "manifest.json").read_text())
        expected = planning_manifest(delivery.profile)
        if any(manifest.get(key) != value for key, value in expected.items()):
            raise ValueError("untrusted planning configuration")
        plan = read_plan(journal, task=task, base_sha=base, profile_sha=delivery.profile.digest)
        events = [e["data"] for e in journal.events() if e["kind"] == "plan_frozen"]
        if (events != [{"digest": plan.digest, "status": plan.document["content"]["status"]}]
                or plan.document["planner_role"] != expected["planner_role_snapshot"]
                or plan.document["content"]["status"] != "ready"):
            raise ValueError("planning is not ready or its acknowledgement changed")
        return {"digest": plan.digest, "guidance": plan.document["content"]}
    except Exception:
        raise PlanningStopped("A trusted ready plan is required before execution") from None


def plan_delivery(delivery, task, base, journal, *, planner_factory=None):
    """One bounded investigation, outside candidate source and before implementation."""
    try:
        manifest = json.loads((journal.root / "manifest.json").read_text())
        if any(manifest.get(k) != v for k, v in planning_manifest(delivery.profile).items()):
            raise ValueError("untrusted planning configuration")
        usage = journal.summary()
        if not usage["usage_complete"] or usage["total_tokens"] >= delivery.settings["max_total_tokens"]:
            raise PlanningStopped("Planning usage is unknown or its token budget exhausted")
        if time.monotonic() >= delivery.deadline:
            raise PlanningStopped("Run time budget exhausted before planning")
        if delivery.settings["max_agent_turns"] < 2:
            raise PlanningStopped("Planning needs an investigation request and a report request")
        if (journal.root / "plan.json").exists() or any(e["kind"] == "plan_frozen" for e in journal.events()):
            raise PlanningStopped("Planning cannot replace an existing run plan")
        config = execution_config(delivery.settings, "planner", journal) if planner_factory is None else None
        # Allocate the first worker under the already-running global deadline.
        # Source setup, investigation and inference remain within the planning phase.
        delivery.prepare_runner(journal)
        deadline = min(delivery.deadline, time.monotonic() + 120)
        if time.monotonic() >= deadline:
            raise PlanningStopped("Run time budget exhausted during runner preparation")
        journal.append("planning_started", max_seconds=120,
                       remaining_run_seconds=round(delivery.deadline - time.monotonic(), 3))
        if planner_factory is None:
            planner = ProductPlanner(OpenAIResponsesAgentConnector(config), journal, task=task,
                base_sha=base, profile=delivery.profile, deadline=deadline, cancelled=delivery.cancelled)
        else:
            planner = planner_factory(task, base, journal, deadline)
        with tempfile.TemporaryDirectory(prefix="ai-factory-plan-") as temporary:
            workspace = Path(temporary) / "source"
            original = delivery._prepare(base, workspace, journal, deadline=deadline)
            modes = executable_paths(workspace, original)
            investigated = False
            with delivery.sandbox_factory(workspace, delivery.settings["image"], writable=False,
                    platform=delivery.settings["platform"]) as sandbox:
                def command(argv):
                    nonlocal investigated
                    result = delivery._command(sandbox, argv, deadline=deadline)
                    investigated = True
                    return result
                content = planner.run("planner", {**task.public_context, "base_sha": base,
                    "required_checks": delivery.settings["checks"],
                    "allowed_paths": delivery.settings["allowed_paths"]}, command)
            if (not investigated or time.monotonic() >= deadline
                    or source_files(workspace) != original or executable_paths(workspace, original) != modes):
                raise PlanningStopped("Planning did not complete unchanged read-only investigation")
        plan = ProductPlan.create(content, task=task, base_sha=base, profile_sha=delivery.profile.digest,
                                  planner_role=select_role("planner", PLANNER_VERSION))
        freeze_plan(journal, plan, task=task, base_sha=base, profile_sha=delivery.profile.digest)
        journal.append("plan_frozen", digest=plan.digest, status=content["status"])
        return retained_plan(delivery, task, base, journal)
    except (PlanningStopped, RunnerError):
        raise
    except Exception:
        raise PlanningStopped("Planning failed; implementation is not authorized") from None
