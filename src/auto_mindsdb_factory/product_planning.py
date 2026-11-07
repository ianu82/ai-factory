"""Controller-owned planning phase and retained guidance admission."""

import json
import tempfile
import time
from pathlib import Path

from .docker_runner import RunnerError
from .product_agent import AgentSessionError, HarnessStopped
from .product_plan import PlanError, ProductPlan, freeze_plan, read_plan
from .product_planner import ProductPlanner, PLANNER_VERSION, REPORT_RESERVE_SECONDS
from .product_roles import select_role
from .product_workspace import source_files, executable_paths
from .product_cancellation import enter_cancellable
from .run_journal import JournalError


class PlanningStopped(HarnessStopped):
    pass


FINALIZATION_RESERVE_SECONDS = 20
_FAILURE_PHASES = {"admission", "worker_preparation", "planning_initialization", "source_preparation",
                   "investigation_or_sandbox", "sandbox_finalization", "source_validation", "plan_freeze", "ready_check"}


def _record_failure(journal, phase, error):
    # Do not format exception text, provider data or mutable runner attributes.
    categories = ((PlanningStopped, "planning_stopped"), (HarnessStopped, "harness_stopped"),
                  (AgentSessionError, "agent_error"), (RunnerError, "runner_error"),
                  (PlanError, "plan_error"), (JournalError, "evidence_error"),
                  (TimeoutError, "timeout"), (OSError, "io_error"), (ValueError, "invalid_data"))
    category = next((label for kind, label in categories if isinstance(error, kind)), "unknown")
    try:
        journal.append("planning_failed", role="planner", category=category,
            phase=phase if type(phase) is str and phase in _FAILURE_PHASES else "unknown")
    except Exception:
        raise PlanningStopped("Planning failure evidence could not be retained") from None


def planning_manifest(profile):
    settings = profile.document
    return {"planner_role_snapshot": select_role("planner", PLANNER_VERSION), "planning_limits": {
        "harness": "native", "model": settings["builder_model"],
        "runner_preparation_budget": "shared_run_deadline",
        "max_turns": min(8, settings["max_agent_turns"]),
        "max_tokens": min(100000, settings["max_total_tokens"]), "max_seconds": 120,
        "last_request_report_only": True, "report_reserve_seconds": REPORT_RESERVE_SECONDS,
        "finalization_reserve_seconds": FINALIZATION_RESERVE_SECONDS}}


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
    phase = "admission"
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
        config = delivery._execution_config("planner", journal) if planner_factory is None else None
        # Allocate the first worker under the already-running global deadline.
        # Source setup, investigation and inference remain within the planning phase.
        phase = "worker_preparation"
        delivery.prepare_runner(journal)
        phase = "planning_initialization"
        deadline = min(delivery.deadline, time.monotonic() + 120)
        if time.monotonic() >= deadline:
            raise PlanningStopped("Run time budget exhausted during runner preparation")
        # Stop investigation/inference early enough to close the job and validate
        # its returned source, without extending either the phase or run deadline.
        work_deadline = deadline - FINALIZATION_RESERVE_SECONDS
        if time.monotonic() >= work_deadline:
            raise PlanningStopped("Insufficient planning time for safe finalization")
        journal.append("planning_started", max_seconds=120,
                       finalization_reserve_seconds=FINALIZATION_RESERVE_SECONDS,
                       remaining_run_seconds=round(delivery.deadline - time.monotonic(), 3))
        if planner_factory is None:
            planner = ProductPlanner(delivery._connector(config, deadline=work_deadline), journal, task=task,
                base_sha=base, profile=delivery.profile, deadline=work_deadline, cancelled=delivery.cancelled)
        else:
            planner = planner_factory(task, base, journal, work_deadline)
        with tempfile.TemporaryDirectory(prefix="ai-factory-plan-") as temporary:
            workspace = Path(temporary) / "source"
            phase = "source_preparation"
            original = delivery._prepare(base, workspace, journal, deadline=work_deadline, planning=True)
            modes = executable_paths(workspace, original)
            investigated = False
            phase = "investigation_or_sandbox"
            manager = delivery.sandbox_factory(workspace, delivery.settings["image"], writable=False,
                    platform=delivery.settings["platform"])
            def finalization_failed():
                nonlocal phase
                phase = "sandbox_finalization"
            with enter_cancellable(manager, cancelled=delivery.cancelled, deadline=work_deadline,
                    finalization_deadline=deadline, on_finalization_failure=finalization_failed) as sandbox:
                def command(argv):
                    nonlocal investigated
                    result = delivery._command(sandbox, argv, deadline=work_deadline)
                    investigated = True
                    return result
                content = planner.run("planner", {**task.public_context, "base_sha": base,
                    "source_environment": {"source_access": "read_only", "git_metadata": "absent",
                                           "base_binding": "controller_export"},
                    "required_checks": delivery.settings["checks"],
                    "allowed_paths": delivery.settings["allowed_paths"]}, command)
            phase = "source_validation"
            checks = {"investigated": investigated, "source_unchanged": source_files(workspace) == original,
                      "modes_unchanged": executable_paths(workspace, original) == modes,
                      "within_deadline": time.monotonic() < deadline}
            journal.append("planning_validation", **checks)
            if not all(checks.values()):
                raise PlanningStopped("Planning did not complete unchanged read-only investigation")
        if time.monotonic() >= deadline:
            raise PlanningStopped("Planning finalization exceeded its phase deadline")
        phase = "plan_freeze"
        plan = ProductPlan.create(content, task=task, base_sha=base, profile_sha=delivery.profile.digest,
                                  planner_role=select_role("planner", PLANNER_VERSION))
        freeze_plan(journal, plan, task=task, base_sha=base, profile_sha=delivery.profile.digest)
        journal.append("plan_frozen", digest=plan.digest, status=content["status"])
        phase = "ready_check"
        return retained_plan(delivery, task, base, journal)
    except (PlanningStopped, RunnerError) as error:
        _record_failure(journal, phase, error)
        raise
    except Exception as error:
        _record_failure(journal, phase, error)
        raise PlanningStopped("Planning failed; implementation is not authorized") from None
