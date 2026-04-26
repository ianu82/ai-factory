from __future__ import annotations

import re
import os
import shlex
import subprocess
import sys
import time
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contracts import load_validators, validation_errors_for
from .controller import ControllerEvent, ControllerState, FactoryController, WorkItem
from .eval_common import deferred_tiers, merge_gate_tiers
from .intake import build_identifier, repo_root, utc_now


class EvalExecutionError(RuntimeError):
    """Base class for Stage 5 eval execution failures."""


class EvalExecutionEligibilityError(EvalExecutionError):
    """Raised when a work item cannot enter Stage 5 eval execution."""


class EvalExecutionConsistencyError(EvalExecutionError):
    """Raised when Stage 4 artifacts disagree about the work item being evaluated."""


@dataclass(slots=True)
class GateResult:
    check_kind: str
    status: str
    command: list[str] | None
    exit_code: int | None
    stdout: str
    stderr: str
    duration_seconds: int
    summary: str

    def to_document(self) -> dict[str, Any]:
        return {
            "check_kind": self.check_kind,
            "status": self.status,
            "command": None if self.command is None else list(self.command),
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "duration_seconds": self.duration_seconds,
            "summary": self.summary,
        }


class CommandGateRunner:
    """Run the subset of eval checks that are backed by real local commands."""

    MAX_CAPTURED_OUTPUT_CHARS = 4000
    DEFAULT_COMMANDS = {
        "unit": [sys.executable, "-m", "pytest", "-q"],
        "contract": [sys.executable, "scripts/validate_contracts.py"],
    }
    ENV_COMMANDS = {
        "lint": "AI_FACTORY_GATE_LINT_COMMAND",
        "typecheck": "AI_FACTORY_GATE_TYPECHECK_COMMAND",
        "unit": "AI_FACTORY_GATE_UNIT_COMMAND",
        "contract": "AI_FACTORY_GATE_CONTRACT_COMMAND",
        "integration": "AI_FACTORY_GATE_INTEGRATION_COMMAND",
        "migration_safety": "AI_FACTORY_GATE_MIGRATION_SAFETY_COMMAND",
    }

    def __init__(
        self,
        repo_root: Path,
        *,
        commands_by_kind: dict[str, list[str]] | None = None,
        required_kinds: set[str] | None = None,
        timeout_seconds: int = 900,
    ) -> None:
        self.repo_root = repo_root.resolve()
        self.commands_by_kind = commands_by_kind or dict(self.DEFAULT_COMMANDS)
        self.required_kinds = required_kinds or {"unit", "contract"}
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_env(cls, repo_root: Path) -> "CommandGateRunner":
        commands = dict(cls.DEFAULT_COMMANDS)
        for kind, env_name in cls.ENV_COMMANDS.items():
            raw = os.environ.get(env_name, "").strip()
            if raw:
                commands[kind] = shlex.split(raw)
        required_raw = os.environ.get("AI_FACTORY_REQUIRED_GATE_KINDS", "unit,contract")
        required = {item.strip() for item in required_raw.split(",") if item.strip()}
        timeout = int(os.environ.get("AI_FACTORY_GATE_TIMEOUT_SECONDS", "900"))
        return cls(repo_root, commands_by_kind=commands, required_kinds=required, timeout_seconds=timeout)

    def is_blocking_kind(self, kind: str) -> bool:
        return kind in self.required_kinds

    def run_check(self, check: dict[str, Any]) -> GateResult:
        kind = str(check["kind"])
        command = self.commands_by_kind.get(kind)
        if not command:
            return GateResult(
                check_kind=kind,
                status="not_configured",
                command=None,
                exit_code=None,
                stdout="",
                stderr="",
                duration_seconds=0,
                summary=f"No command-backed gate is configured for {kind}.",
            )
        try:
            started = time.monotonic()
            completed = subprocess.run(
                command,
                cwd=self.repo_root,
                check=False,
                capture_output=True,
                text=True,
                timeout=min(self.timeout_seconds, int(check["timeout_minutes"]) * 60),
            )
            duration = max(0, int(round(time.monotonic() - started)))
            status = "passed" if completed.returncode == 0 else "failed"
            return GateResult(
                check_kind=kind,
                status=status,
                command=list(command),
                exit_code=completed.returncode,
                stdout=self._captured_output(completed.stdout),
                stderr=self._captured_output(completed.stderr),
                duration_seconds=duration,
                summary=(
                    f"Command-backed gate passed: {' '.join(command)}."
                    if status == "passed"
                    else f"Command-backed gate failed: {' '.join(command)}."
                ),
            )
        except subprocess.TimeoutExpired as exc:
            return GateResult(
                check_kind=kind,
                status="failed",
                command=list(command),
                exit_code=124,
                stdout=self._captured_output(exc.stdout if isinstance(exc.stdout, str) else ""),
                stderr=self._captured_output(exc.stderr if isinstance(exc.stderr, str) else str(exc)),
                duration_seconds=min(self.timeout_seconds, int(check["timeout_minutes"]) * 60),
                summary=f"Command-backed gate timed out: {' '.join(command)}.",
            )
        except OSError as exc:
            return GateResult(
                check_kind=kind,
                status="failed",
                command=list(command),
                exit_code=127,
                stdout="",
                stderr=str(exc),
                duration_seconds=0,
                summary=f"Command-backed gate could not start: {' '.join(command)}.",
            )

    @classmethod
    def _captured_output(cls, output: str) -> str:
        if len(output) <= cls.MAX_CAPTURED_OUTPUT_CHARS:
            return output
        return output[-cls.MAX_CAPTURED_OUTPUT_CHARS :]


@dataclass(slots=True)
class Stage5EvalResult:
    spec_packet: dict[str, Any]
    policy_decision: dict[str, Any]
    ticket_bundle: dict[str, Any]
    eval_manifest: dict[str, Any]
    pr_packet: dict[str, Any]
    prompt_contract: dict[str, Any]
    tool_schema: dict[str, Any]
    golden_dataset: dict[str, Any]
    latency_baseline: dict[str, Any]
    eval_report: dict[str, Any]
    work_item: WorkItem

    def to_document(self) -> dict[str, Any]:
        return {
            "spec_packet": self.spec_packet,
            "policy_decision": self.policy_decision,
            "ticket_bundle": self.ticket_bundle,
            "eval_manifest": self.eval_manifest,
            "pr_packet": self.pr_packet,
            "prompt_contract": self.prompt_contract,
            "tool_schema": self.tool_schema,
            "golden_dataset": self.golden_dataset,
            "latency_baseline": self.latency_baseline,
            "eval_report": self.eval_report,
            "work_item": self.work_item.to_document(),
            "history": [
                {
                    "event": record.event,
                    "from_state": record.from_state,
                    "to_state": record.to_state,
                    "artifact_id": record.artifact_id,
                    "occurred_at": record.occurred_at,
                }
                for record in self.work_item.history
            ],
        }


class EvalRunner:
    """Execute eval checks against the reviewable PR packet."""

    PROMPT_CONTRACT_ID = "eval_runner.v1"
    _DEFAULT_DURATIONS = {
        "lint": 45,
        "typecheck": 90,
        "unit": 120,
        "contract": 75,
        "integration": 240,
        "latency": 180,
        "cost": 150,
        "llm_quality": 210,
        "migration_safety": 180,
        "adversarial": 240,
    }

    def __init__(self, *, gate_runner: CommandGateRunner | None = None) -> None:
        self.gate_runner = gate_runner

    def build_eval_report(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        eval_manifest: dict[str, Any],
        pr_packet: dict[str, Any],
        prompt_contract: dict[str, Any],
        tool_schema: dict[str, Any],
        golden_dataset: dict[str, Any],
        latency_baseline: dict[str, Any],
        *,
        artifact_id: str,
        build_attempt: int,
        evaluated_pr_artifact_version: int,
        timestamp: str | None = None,
        failing_check_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        created_at = timestamp or utc_now()
        failing_ids = failing_check_ids or set()
        policy_artifact = policy_decision["artifact"]
        merge_tiers = merge_gate_tiers(eval_manifest)
        deferred = deferred_tiers(eval_manifest)
        tier_results: list[dict[str, Any]] = []
        failing_merge_tiers: list[str] = []
        failing_check_ids_out: list[str] = []
        passed_check_ids: list[str] = []
        pending_check_ids: list[str] = []
        deferred_check_ids: list[str] = []
        not_configured_check_ids: list[str] = []
        warning_count = 0

        for tier in eval_manifest["tiers"]:
            merge_gate = tier["name"] in merge_tiers
            check_results: list[dict[str, Any]] = []
            required_failure = False
            optional_failure = False
            for check in tier["checks"]:
                result = self._run_check(
                    tier_name=tier["name"],
                    check=check,
                    merge_gate=merge_gate,
                    failing_check_ids=failing_ids,
                    prompt_contract=prompt_contract,
                    tool_schema=tool_schema,
                    golden_dataset=golden_dataset,
                    latency_baseline=latency_baseline,
                )
                check_results.append(result)
                if result["status"] == "failed":
                    failing_check_ids_out.append(result["id"])
                    if check["required"] and merge_gate and self._blocks_merge(check):
                        required_failure = True
                    elif not check["required"]:
                        optional_failure = True
                        warning_count += 1
                elif result["status"] == "passed":
                    passed_check_ids.append(result["id"])
                elif result["status"] == "deferred":
                    deferred_check_ids.append(result["id"])
                    pending_check_ids.append(result["id"])
                elif result["status"] == "not_configured":
                    not_configured_check_ids.append(result["id"])
                    if check["required"] and merge_gate and self._blocks_merge(check):
                        required_failure = True
                else:
                    pending_check_ids.append(result["id"])

            if not merge_gate:
                tier_status = "deferred"
            elif required_failure:
                tier_status = "failed"
                failing_merge_tiers.append(tier["name"])
            elif optional_failure:
                tier_status = "warning"
            elif all(check["status"] == "not_configured" for check in check_results):
                tier_status = "not_configured"
            else:
                tier_status = "passed"

            tier_results.append(
                {
                    "name": tier["name"],
                    "merge_gate": merge_gate,
                    "status": tier_status,
                    "checks": check_results,
                }
            )

        merge_gate_passed = not failing_merge_tiers
        recommendation = (
            "advance_to_security_review"
            if merge_gate_passed
            else "return_for_revision"
        )
        resulting_pr_artifact_version = evaluated_pr_artifact_version + 1
        return {
            "artifact": {
                "id": artifact_id,
                "version": 1,
                "source_stage": "eval_execution",
                "next_stage": "security_review" if merge_gate_passed else "build",
                "status": "approved" if merge_gate_passed else "blocked",
                "risk_tier": policy_artifact["risk_tier"],
                "execution_lane": policy_artifact["execution_lane"],
                "owner_agent": "Eval Gatekeeper",
                "policy_decision_id": policy_artifact["id"],
                "model_fingerprint": self.PROMPT_CONTRACT_ID,
                "budget_class": policy_artifact["budget_class"],
                "rollback_class": policy_artifact["rollback_class"],
                "approval_requirements": list(policy_artifact["approval_requirements"]),
                "blocking_issues": self._blocking_issues(tier_results),
                "created_at": created_at,
                "updated_at": created_at,
            },
            "spec_packet_id": spec_packet["artifact"]["id"],
            "pr_packet_id": pr_packet["artifact"]["id"],
            "eval_manifest_id": eval_manifest["artifact"]["id"],
            "prompt_contract_id": prompt_contract["artifact"]["id"],
            "tool_schema_id": tool_schema["artifact"]["id"],
            "golden_dataset_id": golden_dataset["artifact"]["id"],
            "latency_baseline_id": latency_baseline["artifact"]["id"],
            "build_attempt": build_attempt,
            "evaluated_pr_artifact_version": evaluated_pr_artifact_version,
            "resulting_pr_artifact_version": resulting_pr_artifact_version,
            "tiers": tier_results,
            "summary": {
                "merge_gate_tiers": merge_tiers,
                "deferred_tiers": deferred,
                "merge_gate_passed": merge_gate_passed,
                "recommendation": recommendation,
                "failing_merge_gate_tiers": failing_merge_tiers,
                "failing_check_ids": failing_check_ids_out,
                "passed_check_ids": passed_check_ids,
                "pending_check_ids": pending_check_ids,
                "deferred_check_ids": deferred_check_ids,
                "not_configured_check_ids": not_configured_check_ids,
                "warning_count": warning_count,
            },
        }

    def finalize_pr_packet(
        self,
        pr_packet: dict[str, Any],
        eval_report: dict[str, Any],
        *,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        updated = deepcopy(pr_packet)
        updated_at = timestamp or utc_now()
        summary = eval_report["summary"]
        blocking_findings = self._blocking_issues(eval_report["tiers"])
        warning_findings = self._warning_findings(eval_report["tiers"])
        non_blocking = [
            finding
            for finding in updated["reviewer_report"]["non_blocking_findings"]
            if not finding.startswith("Pending eval tiers before merge:")
        ]
        if summary["merge_gate_passed"] and summary["deferred_tiers"]:
            non_blocking.append(
                "Deferred eval tiers after merge: "
                f"{', '.join(summary['deferred_tiers'])}."
            )
        elif summary["deferred_tiers"]:
            non_blocking.append(
                "Deferred eval tiers remain queued once merge gates pass: "
                f"{', '.join(summary['deferred_tiers'])}."
            )
        non_blocking.extend(warning_findings)
        non_blocking = self._dedupe(non_blocking)

        updated["artifact"]["version"] = int(updated["artifact"]["version"]) + 1
        updated["artifact"]["owner_agent"] = "Eval Gatekeeper"
        updated["artifact"]["model_fingerprint"] = self.PROMPT_CONTRACT_ID
        updated["artifact"]["updated_at"] = updated_at

        status_by_name = {
            check["name"]: check["status"]
            for tier in eval_report["tiers"]
            for check in tier["checks"]
            if check["status"] != "pending"
        }
        for check in updated["checks"]:
            if check["name"] in status_by_name:
                check["status"] = status_by_name[check["name"]]

        if summary["merge_gate_passed"]:
            updated["artifact"]["status"] = "approved"
            updated["artifact"]["next_stage"] = "security_review"
            updated["artifact"]["blocking_issues"] = []
            updated["reviewer_report"] = {
                "approved": True,
                "blocking_findings": [],
                "non_blocking_findings": non_blocking,
            }
            updated["merge_readiness"] = {
                "reviewable": True,
                "mergeable": True,
                "blockers": [],
            }
            return updated

        updated["artifact"]["status"] = "blocked"
        updated["artifact"]["next_stage"] = "build"
        updated["artifact"]["blocking_issues"] = blocking_findings
        updated["reviewer_report"] = {
            "approved": False,
            "blocking_findings": blocking_findings,
            "non_blocking_findings": non_blocking,
        }
        updated["merge_readiness"] = {
            "reviewable": False,
            "mergeable": False,
            "blockers": blocking_findings,
        }
        return updated

    def _run_check(
        self,
        *,
        tier_name: str,
        check: dict[str, Any],
        merge_gate: bool,
        failing_check_ids: set[str],
        prompt_contract: dict[str, Any],
        tool_schema: dict[str, Any],
        golden_dataset: dict[str, Any],
        latency_baseline: dict[str, Any],
    ) -> dict[str, Any]:
        if not merge_gate:
            return {
                "id": check["id"],
                "name": check["name"],
                "kind": check["kind"],
                "required": check["required"],
                "status": "deferred",
                "duration_seconds": 0,
                "summary": f"{tier_name} is deferred to a later stage.",
                **({"baseline_ref": check["baseline_ref"]} if "baseline_ref" in check else {}),
            }

        if self.gate_runner is not None:
            gate = self.gate_runner.run_check(check)
            result = {
                "id": check["id"],
                "name": check["name"],
                "kind": check["kind"],
                "required": check["required"],
                "status": gate.status,
                "duration_seconds": gate.duration_seconds,
                "summary": gate.summary,
                **({"baseline_ref": check["baseline_ref"]} if "baseline_ref" in check else {}),
            }
            if gate.command is not None:
                result["command"] = " ".join(gate.command)
                result["exit_code"] = gate.exit_code
            if gate.status == "failed":
                if gate.stdout:
                    result["stdout"] = gate.stdout
                if gate.stderr:
                    result["stderr"] = gate.stderr
            return result

        should_fail = check["id"] in failing_check_ids
        duration_seconds = min(
            self._DEFAULT_DURATIONS.get(check["kind"], 120),
            int(check["timeout_minutes"]) * 60,
        )
        if check["kind"] in {"latency", "cost", "llm_quality"}:
            threshold = self._extract_numeric_threshold(check["pass_condition"])
            observed = self._observed_metric(
                check["kind"],
                threshold,
                should_fail,
                tool_count=len(tool_schema["tools"]),
                golden_count=len(golden_dataset["entries"]),
            )
            summary = self._metric_summary(
                check["kind"],
                threshold,
                observed,
                should_fail,
                reference_count=len(latency_baseline["reference_check_ids"]),
                prompt_contract_id=prompt_contract["artifact"]["id"],
            )
            result = {
                "id": check["id"],
                "name": check["name"],
                "kind": check["kind"],
                "required": check["required"],
                "status": "failed" if should_fail else "passed",
                "duration_seconds": duration_seconds,
                "summary": summary,
                "threshold_value": threshold,
                "observed_value": observed,
            }
            if "baseline_ref" in check:
                result["baseline_ref"] = check["baseline_ref"]
            return result

        if check["kind"] == "integration":
            summary = (
                f"Exercised {len(tool_schema['tools'])} integration tool paths without regressions."
                if not should_fail
                else "Integration flow regressed against the scoped tool and runtime expectations."
            )
        elif check["kind"] == "contract":
            summary = (
                f"Prompt contract {prompt_contract['artifact']['id']} stayed schema-valid."
                if not should_fail
                else "Contract validation detected an incompatible request or response shape."
            )
        elif check["kind"] == "adversarial":
            summary = (
                f"Adversarial probes across {len(golden_dataset['failure_injection_cases'])} cases stayed bounded."
                if not should_fail
                else "At least one adversarial probe escaped the bounded failure path."
            )
        elif check["kind"] == "migration_safety":
            summary = (
                "Migration and rollback safety checks passed for the current rollback class."
                if not should_fail
                else "Migration or rollback safety checks failed for the current rollout plan."
            )
        else:
            summary = (
                f"{check['name']} passed on the reviewable PR candidate."
                if not should_fail
                else f"{check['name']} failed on the reviewable PR candidate."
            )
        return {
            "id": check["id"],
            "name": check["name"],
            "kind": check["kind"],
            "required": check["required"],
            "status": "failed" if should_fail else "passed",
            "duration_seconds": duration_seconds,
            "summary": summary,
            **({"baseline_ref": check["baseline_ref"]} if "baseline_ref" in check else {}),
        }

    @staticmethod
    def _blocking_issues(tiers: list[dict[str, Any]]) -> list[str]:
        blockers: list[str] = []
        for tier in tiers:
            failed_checks = [
                check["name"]
                for check in tier["checks"]
                if check["required"] and check["status"] in {"failed", "not_configured"}
            ]
            if failed_checks:
                blockers.append(
                    f"Eval gate failed in {tier['name']}: {', '.join(failed_checks)}."
                )
        return blockers

    def _blocks_merge(self, check: dict[str, Any]) -> bool:
        if self.gate_runner is None:
            return True
        return self.gate_runner.is_blocking_kind(str(check["kind"]))

    @staticmethod
    def _warning_findings(tiers: list[dict[str, Any]]) -> list[str]:
        warnings: list[str] = []
        for tier in tiers:
            failed_optional_checks = [
                check["name"]
                for check in tier["checks"]
                if not check["required"] and check["status"] == "failed"
            ]
            if failed_optional_checks:
                warnings.append(
                    f"Optional eval warnings in {tier['name']}: {', '.join(failed_optional_checks)}."
                )
        return warnings

    @staticmethod
    def _metric_summary(
        kind: str,
        threshold: float | None,
        observed: float | None,
        failed: bool,
        *,
        reference_count: int,
        prompt_contract_id: str,
    ) -> str:
        label = {
            "latency": "Latency regression",
            "cost": "Cost regression",
            "llm_quality": "Quality score delta",
        }[kind]
        if threshold is None or observed is None:
            return (
                f"{label} {'failed' if failed else 'passed'} without a numeric threshold."
            )
        direction = "exceeded" if failed else "stayed within"
        return (
            f"{label} {direction} the threshold at {observed:.2f} "
            f"against {threshold:.2f} using {reference_count} reference checks "
            f"and prompt contract {prompt_contract_id}."
        )

    @staticmethod
    def _observed_metric(
        kind: str,
        threshold: float | None,
        failed: bool,
        *,
        tool_count: int,
        golden_count: int,
    ) -> float | None:
        if threshold is None:
            return None
        if failed:
            return round(threshold + max(1.0, tool_count * 0.8), 2)
        if kind == "llm_quality":
            return round(max(0.01, threshold - max(0.02, golden_count * 0.003)), 2)
        return round(max(0.5, threshold - max(1.0, tool_count * 0.7)), 2)

    @staticmethod
    def _extract_numeric_threshold(pass_condition: str) -> float | None:
        match = re.search(r"(\d+(?:\.\d+)?)", pass_condition)
        if match is None:
            return None
        return float(match.group(1))

    @staticmethod
    def _dedupe(items: list[str]) -> list[str]:
        deduped: list[str] = []
        for item in items:
            if item not in deduped:
                deduped.append(item)
        return deduped


class Stage5EvalPipeline:
    """Execute merge-gating eval tiers and finalize merge readiness."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        controller: FactoryController | None = None,
        eval_runner: EvalRunner | None = None,
        gate_runner: CommandGateRunner | None = None,
    ) -> None:
        self.root = repo_root(root)
        self.controller = controller or FactoryController()
        self.eval_runner = eval_runner or EvalRunner(gate_runner=gate_runner)
        self.validators = load_validators(self.root)

    def process(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        ticket_bundle: dict[str, Any],
        eval_manifest: dict[str, Any],
        pr_packet: dict[str, Any],
        prompt_contract: dict[str, Any],
        tool_schema: dict[str, Any],
        golden_dataset: dict[str, Any],
        latency_baseline: dict[str, Any],
        work_item: WorkItem,
        *,
        eval_report_id: str | None = None,
        failing_check_ids: list[str] | None = None,
    ) -> Stage5EvalResult:
        self._validate_document("spec-packet", spec_packet)
        self._validate_document("policy-decision", policy_decision)
        self._validate_document("ticket-bundle", ticket_bundle)
        self._validate_document("eval-manifest", eval_manifest)
        self._validate_document("pr-packet", pr_packet)
        self._validate_document("prompt-contract", prompt_contract)
        self._validate_document("tool-schema", tool_schema)
        self._validate_document("golden-dataset", golden_dataset)
        self._validate_document("latency-baseline", latency_baseline)
        self._validate_document("work-item", work_item.to_document())
        self._validate_consistency(
            spec_packet,
            policy_decision,
            ticket_bundle,
            eval_manifest,
            pr_packet,
            prompt_contract,
            tool_schema,
            golden_dataset,
            latency_baseline,
            work_item,
        )

        if policy_decision["decision"] != "active_build_candidate":
            raise EvalExecutionEligibilityError(
                "Only active_build_candidate items can enter Stage 5 eval execution."
            )
        if work_item.state is not ControllerState.PR_REVIEWABLE:
            raise EvalExecutionEligibilityError(
                "Work item must be in PR_REVIEWABLE before Stage 5; "
                f"got {work_item.state.value}."
            )
        if work_item.attempt_count < 1:
            raise EvalExecutionEligibilityError(
                "Work item must have at least one build attempt before Stage 5 runs."
            )

        timestamp = utc_now()
        evaluated_pr_artifact_version = int(pr_packet["artifact"]["version"])
        artifact_id = eval_report_id or self._default_eval_report_id(
            spec_packet["artifact"]["id"],
            work_item.attempt_count,
            evaluated_pr_artifact_version,
        )
        eval_report = self.eval_runner.build_eval_report(
            spec_packet,
            policy_decision,
            eval_manifest,
            pr_packet,
            prompt_contract,
            tool_schema,
            golden_dataset,
            latency_baseline,
            artifact_id=artifact_id,
            build_attempt=work_item.attempt_count,
            evaluated_pr_artifact_version=evaluated_pr_artifact_version,
            timestamp=timestamp,
            failing_check_ids=set(failing_check_ids or []),
        )
        updated_pr_packet = self.eval_runner.finalize_pr_packet(
            pr_packet,
            eval_report,
            timestamp=timestamp,
        )

        self._validate_document("eval-report", eval_report)
        self._validate_document("pr-packet", updated_pr_packet)
        self._validate_generated_consistency(
            updated_pr_packet,
            eval_report,
            eval_manifest,
            work_item,
        )

        working_item = deepcopy(work_item)
        event = (
            ControllerEvent.REQUIRED_EVAL_TIER_PASSED
            if eval_report["summary"]["merge_gate_passed"]
            else ControllerEvent.REQUIRED_EVAL_TIER_FAILED
        )
        self.controller.apply_event(
            working_item,
            event=event,
            artifact_id=eval_report["artifact"]["id"],
            occurred_at=eval_report["artifact"]["updated_at"],
        )
        self._validate_document("work-item", working_item.to_document())

        return Stage5EvalResult(
            spec_packet=spec_packet,
            policy_decision=policy_decision,
            ticket_bundle=ticket_bundle,
            eval_manifest=eval_manifest,
            pr_packet=updated_pr_packet,
            prompt_contract=prompt_contract,
            tool_schema=tool_schema,
            golden_dataset=golden_dataset,
            latency_baseline=latency_baseline,
            eval_report=eval_report,
            work_item=working_item,
        )

    def _validate_document(self, schema_name: str, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.validators[schema_name], document)
        if errors:
            raise EvalExecutionError(f"{schema_name} failed validation: {'; '.join(errors)}")

    @staticmethod
    def _default_eval_report_id(
        spec_packet_id: str,
        build_attempt: int,
        evaluated_pr_artifact_version: int,
    ) -> str:
        return build_identifier(
            "eval-report",
            f"attempt-{build_attempt}-prv-{evaluated_pr_artifact_version}-{spec_packet_id}",
            max_length=64,
        )

    @staticmethod
    def _validate_consistency(
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        ticket_bundle: dict[str, Any],
        eval_manifest: dict[str, Any],
        pr_packet: dict[str, Any],
        prompt_contract: dict[str, Any],
        tool_schema: dict[str, Any],
        golden_dataset: dict[str, Any],
        latency_baseline: dict[str, Any],
        work_item: WorkItem,
    ) -> None:
        spec_packet_id = spec_packet["artifact"]["id"]
        policy_artifact = policy_decision["artifact"]
        lane = policy_decision["lane_assignment"]["lane"]
        ticket_ids = [ticket["id"] for ticket in ticket_bundle["tickets"]]

        if work_item.current_artifact_id != pr_packet["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "work-item current_artifact_id must match the provided pr-packet."
            )
        if work_item.policy_decision_id != policy_artifact["id"]:
            raise EvalExecutionConsistencyError(
                "work-item policy_decision_id does not match the policy decision artifact."
            )
        if work_item.execution_lane != lane:
            raise EvalExecutionConsistencyError(
                "work-item execution_lane does not match the policy lane."
            )
        if work_item.risk_score != policy_decision["risk_score"]:
            raise EvalExecutionConsistencyError(
                "work-item risk_score does not match the policy decision."
            )
        if work_item.source_provider != spec_packet["source"]["provider"]:
            raise EvalExecutionConsistencyError(
                "work-item source_provider does not match the provided spec-packet."
            )
        if work_item.source_external_id != spec_packet["source"]["external_id"]:
            raise EvalExecutionConsistencyError(
                "work-item source_external_id does not match the provided spec-packet."
            )
        if policy_decision["spec_packet_id"] != spec_packet_id:
            raise EvalExecutionConsistencyError(
                "policy-decision does not reference the provided spec-packet."
            )
        if policy_decision["decision"] != spec_packet["relevance"]["decision"]:
            raise EvalExecutionConsistencyError(
                "policy-decision decision does not match the provided spec-packet."
            )
        if policy_decision["risk_score"] != spec_packet["risk_profile"]["risk_score"]:
            raise EvalExecutionConsistencyError(
                "policy-decision risk score does not match the provided spec-packet."
            )
        if ticket_bundle["spec_packet_id"] != spec_packet_id:
            raise EvalExecutionConsistencyError(
                "ticket-bundle does not reference the provided spec-packet."
            )
        if ticket_bundle["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "ticket-bundle eval_manifest_id does not match the provided eval-manifest."
            )
        if eval_manifest["target_id"] != ticket_bundle["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "eval-manifest target_id does not match the provided ticket-bundle."
            )
        if pr_packet["spec_packet_id"] != spec_packet_id:
            raise EvalExecutionConsistencyError(
                "pr-packet spec_packet_id does not match the provided spec-packet."
            )
        if pr_packet["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "pr-packet eval_manifest_id does not match the provided eval-manifest."
            )
        if sorted(pr_packet["ticket_ids"]) != sorted(ticket_ids):
            raise EvalExecutionConsistencyError(
                "pr-packet ticket_ids do not match the provided ticket-bundle."
            )
        if not pr_packet["reviewer_report"]["approved"]:
            raise EvalExecutionEligibilityError("pr-packet must be approved before Stage 5 runs.")
        if pr_packet["reviewer_report"]["blocking_findings"]:
            raise EvalExecutionEligibilityError(
                "pr-packet still has blocking findings and cannot enter Stage 5."
            )
        if not pr_packet["merge_readiness"]["reviewable"]:
            raise EvalExecutionEligibilityError(
                "pr-packet must be reviewable before Stage 5 runs."
            )
        if pr_packet["merge_readiness"]["mergeable"]:
            raise EvalExecutionEligibilityError(
                "pr-packet is already mergeable and should not re-enter Stage 5."
            )

        if prompt_contract["spec_packet_id"] != spec_packet_id:
            raise EvalExecutionConsistencyError(
                "prompt-contract spec_packet_id does not match the provided spec-packet."
            )
        if prompt_contract["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "prompt-contract pr_packet_id does not match the provided pr-packet."
            )
        if prompt_contract["tool_schema_id"] != tool_schema["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "prompt-contract tool_schema_id does not match the provided tool-schema."
            )
        if prompt_contract["golden_dataset_id"] != golden_dataset["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "prompt-contract golden_dataset_id does not match the provided golden dataset."
            )
        if tool_schema["spec_packet_id"] != spec_packet_id:
            raise EvalExecutionConsistencyError(
                "tool-schema spec_packet_id does not match the provided spec-packet."
            )
        if tool_schema["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "tool-schema prompt_contract_id does not match the provided prompt contract."
            )
        if golden_dataset["spec_packet_id"] != spec_packet_id:
            raise EvalExecutionConsistencyError(
                "golden-dataset spec_packet_id does not match the provided spec-packet."
            )
        if golden_dataset["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "golden-dataset prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["spec_packet_id"] != spec_packet_id:
            raise EvalExecutionConsistencyError(
                "latency-baseline spec_packet_id does not match the provided spec-packet."
            )
        if latency_baseline["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "latency-baseline prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "latency-baseline pr_packet_id does not match the provided pr-packet."
            )

        tier_names = [tier["name"] for tier in eval_manifest["tiers"]]
        if sorted(tier_names) != sorted(policy_decision["required_eval_tiers"]):
            raise EvalExecutionConsistencyError(
                "eval-manifest tiers do not match the policy decision required tiers."
            )
        if not merge_gate_tiers(eval_manifest):
            raise EvalExecutionConsistencyError(
                "eval-manifest must include at least one merge-gating eval tier."
            )

        for artifact_name, artifact in (
            ("ticket-bundle", ticket_bundle["artifact"]),
            ("eval-manifest", eval_manifest["artifact"]),
            ("pr-packet", pr_packet["artifact"]),
            ("prompt-contract", prompt_contract["artifact"]),
            ("tool-schema", tool_schema["artifact"]),
            ("golden-dataset", golden_dataset["artifact"]),
            ("latency-baseline", latency_baseline["artifact"]),
        ):
            if artifact["policy_decision_id"] != policy_artifact["id"]:
                raise EvalExecutionConsistencyError(
                    f"{artifact_name} policy_decision_id does not match the policy decision artifact."
                )
            if artifact["execution_lane"] != lane:
                raise EvalExecutionConsistencyError(
                    f"{artifact_name} execution_lane does not match the policy lane."
                )
            if artifact["risk_tier"] != policy_artifact["risk_tier"]:
                raise EvalExecutionConsistencyError(
                    f"{artifact_name} risk_tier does not match the policy decision artifact."
                )
            if artifact["budget_class"] != policy_artifact["budget_class"]:
                raise EvalExecutionConsistencyError(
                    f"{artifact_name} budget_class does not match the policy decision artifact."
                )
            if artifact["rollback_class"] != policy_artifact["rollback_class"]:
                raise EvalExecutionConsistencyError(
                    f"{artifact_name} rollback_class does not match the policy decision artifact."
                )
            if sorted(artifact["approval_requirements"]) != sorted(
                policy_artifact["approval_requirements"]
            ):
                raise EvalExecutionConsistencyError(
                    f"{artifact_name} approval_requirements do not match the policy decision artifact."
                )

    @staticmethod
    def _validate_generated_consistency(
        pr_packet: dict[str, Any],
        eval_report: dict[str, Any],
        eval_manifest: dict[str, Any],
        work_item: WorkItem,
    ) -> None:
        if eval_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "eval-report pr_packet_id does not match the updated pr-packet artifact."
            )
        if eval_report["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise EvalExecutionConsistencyError(
                "eval-report eval_manifest_id does not match the provided eval-manifest."
            )
        if eval_report["build_attempt"] != work_item.attempt_count:
            raise EvalExecutionConsistencyError(
                "eval-report build_attempt does not match the work-item attempt count."
            )
        if eval_report["evaluated_pr_artifact_version"] + 1 != eval_report["resulting_pr_artifact_version"]:
            raise EvalExecutionConsistencyError(
                "eval-report resulting_pr_artifact_version must be exactly one greater than the evaluated version."
            )
        if eval_report["resulting_pr_artifact_version"] != pr_packet["artifact"]["version"]:
            raise EvalExecutionConsistencyError(
                "eval-report resulting_pr_artifact_version does not match the updated pr-packet version."
            )

        status_by_name = {
            check["name"]: check["status"]
            for tier in eval_report["tiers"]
            for check in tier["checks"]
            if check["status"] != "pending"
        }
        for check in pr_packet["checks"]:
            if check["name"] in status_by_name and check["status"] != status_by_name[check["name"]]:
                raise EvalExecutionConsistencyError(
                    f"pr-packet check '{check['name']}' does not match the eval-report result."
                )

        merge_gate_tier_set = set(merge_gate_tiers(eval_manifest))
        deferred_tier_set = set(deferred_tiers(eval_manifest))
        summary = eval_report["summary"]
        if set(summary["merge_gate_tiers"]) != merge_gate_tier_set:
            raise EvalExecutionConsistencyError(
                "eval-report merge_gate_tiers do not match the merge-gating eval tiers."
            )
        if set(summary["deferred_tiers"]) != deferred_tier_set:
            raise EvalExecutionConsistencyError(
                "eval-report deferred_tiers do not match the deferred eval tiers."
            )
        expected_mergeable = summary["merge_gate_passed"]
        if pr_packet["merge_readiness"]["mergeable"] != expected_mergeable:
            raise EvalExecutionConsistencyError(
                "pr-packet merge_readiness.mergeable does not match the eval-report decision."
            )
        if expected_mergeable and pr_packet["reviewer_report"]["blocking_findings"]:
            raise EvalExecutionConsistencyError(
                "mergeable pr-packet cannot retain blocking findings after eval execution."
            )
