from __future__ import annotations

import hashlib
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contracts import load_validators, validation_errors_for
from .controller import ControllerEvent, ControllerState, FactoryController, WorkItem
from .intake import build_identifier, repo_root, utc_now


class MergeError(RuntimeError):
    """Base class for merge-orchestration failures."""


class MergeEligibilityError(MergeError):
    """Raised when a work item cannot enter the merge stage."""


class MergeConsistencyError(MergeError):
    """Raised when pre-merge artifacts disagree about the candidate."""


@dataclass(slots=True)
class StageMergeResult:
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
    security_review: dict[str, Any]
    merge_decision: dict[str, Any]
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
            "security_review": self.security_review,
            "merge_decision": self.merge_decision,
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


class MergeConductor:
    """Apply lane-aware merge policy after security approval."""

    PROMPT_CONTRACT_ID = "merge_conductor.v1"
    OWNER_AGENT = "Merge Conductor"

    def build_merge_decision(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        pr_packet: dict[str, Any],
        security_review: dict[str, Any],
        *,
        artifact_id: str,
        build_attempt: int,
        approved_merge_reviewers: list[str] | None = None,
        blocking_findings: list[str] | None = None,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        created_at = timestamp or utc_now()
        policy_artifact = policy_decision["artifact"]
        approvals_granted = self._approvals_granted(
            policy_decision,
            approved_merge_reviewers or [],
        )
        pending_approvals = self._pending_merge_approvals(policy_decision, approvals_granted)
        merge_checks = self._merge_checks(
            policy_decision,
            pr_packet,
            security_review,
            pending_approvals=pending_approvals,
            explicit_blocking_findings=blocking_findings or [],
        )
        blocking_summary = self._blocking_findings(merge_checks)
        watch_summary = self._watch_findings(merge_checks)
        passed_check_ids = [check["id"] for check in merge_checks if check["status"] == "passed"]
        failed_check_ids = [check["id"] for check in merge_checks if check["status"] == "failed"]
        needs_human_review_check_ids = [
            check["id"]
            for check in merge_checks
            if check["status"] == "needs_human_review"
        ]
        decision = self._merge_decision(
            policy_decision,
            approvals_granted=approvals_granted,
            pending_approvals=pending_approvals,
            blocking_summary=blocking_summary,
            approved_merge_reviewers=approved_merge_reviewers or [],
        )
        evaluated_pr_artifact_version = int(pr_packet["artifact"]["version"])
        resulting_pr_artifact_version = evaluated_pr_artifact_version + 1
        merge_plan = self._merge_plan(policy_decision, pr_packet, approvals_granted, pending_approvals)

        return {
            "artifact": {
                "id": artifact_id,
                "version": 1,
                "source_stage": "merge",
                "next_stage": self._next_stage(decision["status"]),
                "status": self._artifact_status(decision["status"]),
                "risk_tier": policy_artifact["risk_tier"],
                "execution_lane": policy_artifact["execution_lane"],
                "owner_agent": self.OWNER_AGENT,
                "policy_decision_id": policy_artifact["id"],
                "model_fingerprint": self.PROMPT_CONTRACT_ID,
                "budget_class": policy_artifact["budget_class"],
                "rollback_class": policy_artifact["rollback_class"],
                "approval_requirements": list(policy_artifact["approval_requirements"]),
                "blocking_issues": list(blocking_summary),
                "created_at": created_at,
                "updated_at": created_at,
            },
            "spec_packet_id": spec_packet["artifact"]["id"],
            "pr_packet_id": pr_packet["artifact"]["id"],
            "security_review_id": security_review["artifact"]["id"],
            "build_attempt": build_attempt,
            "evaluated_pr_artifact_version": evaluated_pr_artifact_version,
            "resulting_pr_artifact_version": resulting_pr_artifact_version,
            "merge_plan": merge_plan,
            "merge_decision": decision,
            "summary": {
                "blocking_findings": blocking_summary,
                "watch_findings": watch_summary,
                "passed_check_ids": passed_check_ids,
                "failed_check_ids": failed_check_ids,
                "needs_human_review_check_ids": needs_human_review_check_ids,
            },
        }

    def finalize_pr_packet(
        self,
        pr_packet: dict[str, Any],
        merge_decision: dict[str, Any],
        *,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        updated = deepcopy(pr_packet)
        updated_at = timestamp or utc_now()
        decision = merge_decision["merge_decision"]
        summary = merge_decision["summary"]
        blocking_findings = summary["blocking_findings"]
        non_blocking = [
            finding
            for finding in updated["reviewer_report"]["non_blocking_findings"]
            if not finding.startswith("Merge conductor ")
        ]
        non_blocking.extend(summary["watch_findings"])

        updated["artifact"]["version"] = int(updated["artifact"]["version"]) + 1
        updated["artifact"]["owner_agent"] = self.OWNER_AGENT
        updated["artifact"]["model_fingerprint"] = self.PROMPT_CONTRACT_ID
        updated["artifact"]["updated_at"] = updated_at
        updated["merge_execution"] = {
            "target_branch": merge_decision["merge_plan"]["target_branch"],
            "method": merge_decision["merge_plan"]["method"],
            "status": self._merge_execution_status(decision["status"]),
            "merge_commit_sha": decision["merge_commit_sha"],
            "merged_at": decision["merged_at"],
            "merged_by": decision["merged_by"],
        }

        if decision["status"] == "merged":
            updated["artifact"]["status"] = "approved"
            updated["artifact"]["next_stage"] = "release_staging"
            updated["artifact"]["blocking_issues"] = []
            non_blocking.append(
                "Merge conductor merged the PR automatically."
                if decision["mode"] == "auto"
                else "Merge conductor merged the PR after explicit merge approval."
            )
            updated["reviewer_report"] = {
                "approved": True,
                "blocking_findings": [],
                "non_blocking_findings": self._dedupe(non_blocking),
            }
            updated["merge_readiness"] = {
                "reviewable": True,
                "mergeable": True,
                "blockers": [],
            }
            return updated

        if decision["status"] == "pending_human":
            updated["artifact"]["status"] = "ready"
            updated["artifact"]["next_stage"] = "human_merge_signoff"
            updated["artifact"]["blocking_issues"] = []
            non_blocking.append(
                "Merge conductor is waiting on explicit human merge approval."
            )
            updated["reviewer_report"] = {
                "approved": True,
                "blocking_findings": [],
                "non_blocking_findings": self._dedupe(non_blocking),
            }
            updated["merge_readiness"] = {
                "reviewable": True,
                "mergeable": False,
                "blockers": ["Merge approval is still pending."],
            }
            return updated

        updated["artifact"]["status"] = "blocked"
        updated["artifact"]["next_stage"] = "build"
        updated["artifact"]["blocking_issues"] = blocking_findings
        updated["reviewer_report"] = {
            "approved": False,
            "blocking_findings": blocking_findings,
            "non_blocking_findings": self._dedupe(non_blocking),
        }
        updated["merge_readiness"] = {
            "reviewable": False,
            "mergeable": False,
            "blockers": blocking_findings,
        }
        return updated

    def _merge_checks(
        self,
        policy_decision: dict[str, Any],
        pr_packet: dict[str, Any],
        security_review: dict[str, Any],
        *,
        pending_approvals: list[str],
        explicit_blocking_findings: list[str],
    ) -> list[dict[str, Any]]:
        pr_blockers = list(pr_packet["merge_readiness"]["blockers"])
        merge_execution = pr_packet.get("merge_execution", {})
        merge_readiness_passed = pr_packet["merge_readiness"]["mergeable"] or (
            merge_execution.get("status") == "pending_human"
            and pr_packet["merge_readiness"]["reviewable"]
        )
        return [
            {
                "id": "security_signoff",
                "status": (
                    "passed"
                    if security_review["signoff"]["status"] == "approved"
                    else "failed"
                ),
                "finding": "Security review must be approved before merge can continue.",
            },
            {
                "id": "reviewer_approval",
                "status": (
                    "passed"
                    if pr_packet["reviewer_report"]["approved"]
                    and not pr_packet["reviewer_report"]["blocking_findings"]
                    else "failed"
                ),
                "finding": "Reviewer approval must remain intact before merge.",
            },
            {
                "id": "merge_readiness",
                "status": "passed" if merge_readiness_passed else "failed",
                "finding": (
                    pr_blockers[0]
                    if pr_blockers
                    else "PR merge-readiness blockers must be cleared before merge."
                ),
            },
            {
                "id": "merge_approval",
                "status": "needs_human_review" if pending_approvals else "passed",
                "finding": "Policy requires explicit human merge approval before the PR can merge.",
            },
            {
                "id": "explicit_merge_blocking_findings",
                "status": "failed" if explicit_blocking_findings else "passed",
                "finding": explicit_blocking_findings[0]
                if explicit_blocking_findings
                else "No explicit merge blocking findings were provided.",
            },
            {
                "id": "merge_not_already_executed",
                "status": (
                    "failed"
                    if pr_packet.get("merge_execution", {}).get("status") == "merged"
                    else "passed"
                ),
                "finding": "PR is already marked as merged and should not re-enter the merge stage.",
            },
            {
                "id": "merge_policy_mode",
                "status": "passed",
                "finding": (
                    "Lane merge policy is "
                    f"{policy_decision['autonomy_policy']['merge']}."
                ),
            },
        ]

    @staticmethod
    def _blocking_findings(merge_checks: list[dict[str, Any]]) -> list[str]:
        findings: list[str] = []
        for check in merge_checks:
            if check["status"] == "failed":
                findings.append(check["finding"])
        return MergeConductor._dedupe(findings)

    @staticmethod
    def _watch_findings(merge_checks: list[dict[str, Any]]) -> list[str]:
        findings: list[str] = []
        for check in merge_checks:
            if check["status"] == "needs_human_review":
                findings.append(check["finding"])
        return MergeConductor._dedupe(findings)

    def _merge_decision(
        self,
        policy_decision: dict[str, Any],
        *,
        approvals_granted: list[str],
        pending_approvals: list[str],
        blocking_summary: list[str],
        approved_merge_reviewers: list[str],
    ) -> dict[str, Any]:
        if blocking_summary:
            return {
                "status": "blocked",
                "mode": "blocked",
                "approvals_granted": approvals_granted,
                "pending_approvals": pending_approvals,
                "rationale": "Merge orchestration found blocking issues that must be fixed before merge.",
                "merged_by": None,
                "merged_at": None,
                "merge_commit_sha": None,
            }

        if pending_approvals:
            return {
                "status": "pending_human",
                "mode": "human_required",
                "approvals_granted": approvals_granted,
                "pending_approvals": pending_approvals,
                "rationale": "Lane policy requires explicit human merge approval before the PR can merge.",
                "merged_by": None,
                "merged_at": None,
                "merge_commit_sha": None,
            }

        merged_at = utc_now()
        return {
            "status": "merged",
            "mode": "human_approved" if approvals_granted else "auto",
            "approvals_granted": approvals_granted,
            "pending_approvals": [],
            "rationale": (
                "Merge checks passed and explicit human merge approval was granted."
                if approvals_granted
                else "Merge checks passed and lane policy allows autonomous merge."
            ),
            "merged_by": approved_merge_reviewers[0] if approved_merge_reviewers else self.OWNER_AGENT,
            "merged_at": merged_at,
            "merge_commit_sha": self._merge_commit_sha(policy_decision["artifact"]["id"], merged_at),
        }

    def _merge_plan(
        self,
        policy_decision: dict[str, Any],
        pr_packet: dict[str, Any],
        approvals_granted: list[str],
        pending_approvals: list[str],
    ) -> dict[str, Any]:
        merge_execution = pr_packet.get("merge_execution", {})
        return {
            "target_branch": merge_execution.get("target_branch") or "main",
            "method": merge_execution.get("method") or self._default_merge_method(policy_decision),
            "autonomy_mode": policy_decision["autonomy_policy"]["merge"],
            "required_approvals": ["merge"] if self._merge_required(policy_decision) else [],
            "approvals_granted": approvals_granted,
            "pending_approvals": pending_approvals,
        }

    @staticmethod
    def _artifact_status(decision_status: str) -> str:
        if decision_status == "merged":
            return "approved"
        if decision_status == "pending_human":
            return "ready"
        return "blocked"

    @staticmethod
    def _next_stage(decision_status: str) -> str:
        if decision_status == "merged":
            return "release_staging"
        if decision_status == "pending_human":
            return "human_merge_signoff"
        return "build"

    @staticmethod
    def _merge_execution_status(decision_status: str) -> str:
        return {
            "merged": "merged",
            "pending_human": "pending_human",
            "blocked": "blocked",
        }[decision_status]

    @staticmethod
    def _merge_commit_sha(seed: str, merged_at: str) -> str:
        return hashlib.sha1(f"{seed}:{merged_at}".encode("utf-8")).hexdigest()[:12]

    @staticmethod
    def _default_merge_method(policy_decision: dict[str, Any]) -> str:
        return (
            "merge_commit"
            if policy_decision["lane_assignment"]["lane"] == "restricted"
            else "squash"
        )

    @staticmethod
    def _merge_required(policy_decision: dict[str, Any]) -> bool:
        return "merge" in policy_decision["required_approvals"]

    @classmethod
    def _approvals_granted(
        cls,
        policy_decision: dict[str, Any],
        approved_merge_reviewers: list[str],
    ) -> list[str]:
        if not cls._merge_required(policy_decision):
            return []
        return ["merge"] if approved_merge_reviewers else []

    @classmethod
    def _pending_merge_approvals(
        cls,
        policy_decision: dict[str, Any],
        approvals_granted: list[str],
    ) -> list[str]:
        required = {"merge"} if cls._merge_required(policy_decision) else set()
        return sorted(required - set(approvals_granted))

    @staticmethod
    def _dedupe(items: list[str]) -> list[str]:
        deduped: list[str] = []
        for item in items:
            if item not in deduped:
                deduped.append(item)
        return deduped


class StageMergePipeline:
    """Run merge orchestration for a security-approved PR candidate."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        controller: FactoryController | None = None,
        merge_conductor: MergeConductor | None = None,
    ) -> None:
        self.root = repo_root(root)
        self.controller = controller or FactoryController()
        self.merge_conductor = merge_conductor or MergeConductor()
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
        eval_report: dict[str, Any],
        security_review: dict[str, Any],
        work_item: WorkItem,
        *,
        merge_decision_id: str | None = None,
        approved_merge_reviewers: list[str] | None = None,
        blocking_findings: list[str] | None = None,
    ) -> StageMergeResult:
        self._validate_document("spec-packet", spec_packet)
        self._validate_document("policy-decision", policy_decision)
        self._validate_document("ticket-bundle", ticket_bundle)
        self._validate_document("eval-manifest", eval_manifest)
        self._validate_document("pr-packet", pr_packet)
        self._validate_document("prompt-contract", prompt_contract)
        self._validate_document("tool-schema", tool_schema)
        self._validate_document("golden-dataset", golden_dataset)
        self._validate_document("latency-baseline", latency_baseline)
        self._validate_document("eval-report", eval_report)
        self._validate_document("security-review", security_review)
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
            eval_report,
            security_review,
            work_item,
        )

        timestamp = utc_now()
        evaluated_pr_artifact_version = int(pr_packet["artifact"]["version"])
        artifact_id = merge_decision_id or self._default_merge_decision_id(
            spec_packet["artifact"]["id"],
            work_item.attempt_count,
            evaluated_pr_artifact_version,
        )
        merge_decision = self.merge_conductor.build_merge_decision(
            spec_packet,
            policy_decision,
            pr_packet,
            security_review,
            artifact_id=artifact_id,
            build_attempt=work_item.attempt_count,
            approved_merge_reviewers=approved_merge_reviewers,
            blocking_findings=blocking_findings,
            timestamp=timestamp,
        )
        updated_pr_packet = self.merge_conductor.finalize_pr_packet(
            pr_packet,
            merge_decision,
            timestamp=timestamp,
        )

        self._validate_document("merge-decision", merge_decision)
        self._validate_document("pr-packet", updated_pr_packet)
        self._validate_generated_consistency(
            updated_pr_packet,
            security_review,
            merge_decision,
            work_item,
        )

        working_item = deepcopy(work_item)
        if work_item.state is ControllerState.SECURITY_APPROVED:
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.MERGE_STARTED,
                artifact_id=merge_decision["artifact"]["id"],
                occurred_at=merge_decision["artifact"]["created_at"],
            )
        decision_status = merge_decision["merge_decision"]["status"]
        if decision_status == "blocked":
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.MERGE_BLOCKED,
                artifact_id=merge_decision["artifact"]["id"],
                occurred_at=merge_decision["artifact"]["updated_at"],
            )
        elif decision_status == "merged":
            if working_item.state is not ControllerState.MERGE_REVIEWING:
                self.controller.apply_event(
                    working_item,
                    event=ControllerEvent.MERGE_STARTED,
                    artifact_id=merge_decision["artifact"]["id"],
                    occurred_at=merge_decision["artifact"]["created_at"],
                )
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.PR_MERGED,
                artifact_id=merge_decision["artifact"]["id"],
                occurred_at=merge_decision["artifact"]["updated_at"],
            )
        elif decision_status == "pending_human":
            working_item.current_artifact_id = merge_decision["artifact"]["id"]
            working_item.updated_at = merge_decision["artifact"]["updated_at"]
        self._validate_document("work-item", working_item.to_document())

        return StageMergeResult(
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
            security_review=security_review,
            merge_decision=merge_decision,
            work_item=working_item,
        )

    def _validate_document(self, schema_name: str, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.validators[schema_name], document)
        if errors:
            raise MergeError(f"{schema_name} failed validation: {'; '.join(errors)}")

    @staticmethod
    def _default_merge_decision_id(
        spec_packet_id: str,
        build_attempt: int,
        evaluated_pr_artifact_version: int,
    ) -> str:
        return build_identifier(
            "merge-decision",
            f"attempt-{build_attempt}-prv-{evaluated_pr_artifact_version}-{spec_packet_id}",
            max_length=72,
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
        eval_report: dict[str, Any],
        security_review: dict[str, Any],
        work_item: WorkItem,
    ) -> None:
        spec_packet_id = spec_packet["artifact"]["id"]
        policy_artifact = policy_decision["artifact"]
        lane = policy_decision["lane_assignment"]["lane"]

        if policy_decision["decision"] != "active_build_candidate":
            raise MergeEligibilityError(
                "Only active_build_candidate items can enter the merge stage."
            )
        if work_item.state not in {
            ControllerState.SECURITY_APPROVED,
            ControllerState.MERGE_REVIEWING,
        }:
            raise MergeEligibilityError(
                "Work item must be in SECURITY_APPROVED or MERGE_REVIEWING before merge; "
                f"got {work_item.state.value}."
            )
        if work_item.attempt_count < 1:
            raise MergeEligibilityError(
                "Work item must have at least one build attempt before merge runs."
            )
        if security_review["signoff"]["status"] != "approved":
            raise MergeEligibilityError(
                "Merge only runs after security review is approved."
            )
        if work_item.state is ControllerState.SECURITY_APPROVED and work_item.current_artifact_id != security_review["artifact"]["id"]:
            raise MergeConsistencyError(
                "work-item current_artifact_id must match the security-review when merge starts."
            )
        if work_item.state is ControllerState.MERGE_REVIEWING and not work_item.current_artifact_id:
            raise MergeConsistencyError(
                "work-item in MERGE_REVIEWING must retain the previous merge artifact id."
            )
        if work_item.policy_decision_id != policy_artifact["id"]:
            raise MergeConsistencyError(
                "work-item policy_decision_id does not match the policy decision artifact."
            )
        if work_item.execution_lane != lane:
            raise MergeConsistencyError(
                "work-item execution_lane does not match the policy lane."
            )
        if work_item.risk_score != policy_decision["risk_score"]:
            raise MergeConsistencyError(
                "work-item risk_score does not match the policy decision."
            )
        if work_item.source_provider != spec_packet["source"]["provider"]:
            raise MergeConsistencyError(
                "work-item source_provider does not match the provided spec-packet."
            )
        if work_item.source_external_id != spec_packet["source"]["external_id"]:
            raise MergeConsistencyError(
                "work-item source_external_id does not match the provided spec-packet."
            )
        if policy_decision["spec_packet_id"] != spec_packet_id:
            raise MergeConsistencyError(
                "policy-decision does not reference the provided spec-packet."
            )
        if ticket_bundle["spec_packet_id"] != spec_packet_id:
            raise MergeConsistencyError(
                "ticket-bundle does not reference the provided spec-packet."
            )
        if ticket_bundle["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise MergeConsistencyError(
                "ticket-bundle eval_manifest_id does not match the provided eval-manifest."
            )
        if eval_manifest["target_id"] != ticket_bundle["artifact"]["id"]:
            raise MergeConsistencyError(
                "eval-manifest target_id does not match the provided ticket-bundle."
            )
        if pr_packet["spec_packet_id"] != spec_packet_id:
            raise MergeConsistencyError(
                "pr-packet spec_packet_id does not match the provided spec-packet."
            )
        if pr_packet["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise MergeConsistencyError(
                "pr-packet eval_manifest_id does not match the provided eval-manifest."
            )
        if prompt_contract["spec_packet_id"] != spec_packet_id:
            raise MergeConsistencyError(
                "prompt-contract spec_packet_id does not match the provided spec-packet."
            )
        if prompt_contract["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise MergeConsistencyError(
                "prompt-contract pr_packet_id does not match the provided pr-packet."
            )
        if tool_schema["spec_packet_id"] != spec_packet_id:
            raise MergeConsistencyError(
                "tool-schema spec_packet_id does not match the provided spec-packet."
            )
        if tool_schema["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise MergeConsistencyError(
                "tool-schema prompt_contract_id does not match the provided prompt contract."
            )
        if golden_dataset["spec_packet_id"] != spec_packet_id:
            raise MergeConsistencyError(
                "golden-dataset spec_packet_id does not match the provided spec-packet."
            )
        if golden_dataset["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise MergeConsistencyError(
                "golden-dataset prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["spec_packet_id"] != spec_packet_id:
            raise MergeConsistencyError(
                "latency-baseline spec_packet_id does not match the provided spec-packet."
            )
        if latency_baseline["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise MergeConsistencyError(
                "latency-baseline prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise MergeConsistencyError(
                "latency-baseline pr_packet_id does not match the provided pr-packet."
            )
        if eval_report["spec_packet_id"] != spec_packet_id:
            raise MergeConsistencyError(
                "eval-report spec_packet_id does not match the provided spec-packet."
            )
        if eval_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise MergeConsistencyError(
                "eval-report pr_packet_id does not match the provided pr-packet."
            )
        if security_review["spec_packet_id"] != spec_packet_id:
            raise MergeConsistencyError(
                "security-review spec_packet_id does not match the provided spec-packet."
            )
        if security_review["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise MergeConsistencyError(
                "security-review pr_packet_id does not match the provided pr-packet."
            )
        if security_review["eval_report_id"] != eval_report["artifact"]["id"]:
            raise MergeConsistencyError(
                "security-review eval_report_id does not match the provided eval-report."
            )
        if work_item.attempt_count != security_review["build_attempt"]:
            raise MergeConsistencyError(
                "work-item attempt_count does not match the security-review build attempt."
            )
        if int(pr_packet["artifact"]["version"]) < security_review["resulting_pr_artifact_version"]:
            raise MergeConsistencyError(
                "pr-packet version cannot be earlier than the security-review resulting version."
            )
        if work_item.state is ControllerState.SECURITY_APPROVED:
            if int(pr_packet["artifact"]["version"]) != security_review["resulting_pr_artifact_version"]:
                raise MergeConsistencyError(
                    "pr-packet version must match the security-review resulting version when merge starts."
                )
            if not pr_packet["merge_readiness"]["mergeable"]:
                raise MergeEligibilityError(
                    "pr-packet must be mergeable before the merge stage starts."
                )
        if work_item.state is ControllerState.MERGE_REVIEWING and not pr_packet["reviewer_report"]["approved"]:
            raise MergeEligibilityError(
                "pr-packet must remain approved while waiting on merge approval."
            )

        for artifact_name, artifact in (
            ("ticket-bundle", ticket_bundle["artifact"]),
            ("eval-manifest", eval_manifest["artifact"]),
            ("pr-packet", pr_packet["artifact"]),
            ("prompt-contract", prompt_contract["artifact"]),
            ("tool-schema", tool_schema["artifact"]),
            ("golden-dataset", golden_dataset["artifact"]),
            ("latency-baseline", latency_baseline["artifact"]),
            ("eval-report", eval_report["artifact"]),
            ("security-review", security_review["artifact"]),
        ):
            if artifact["policy_decision_id"] != policy_artifact["id"]:
                raise MergeConsistencyError(
                    f"{artifact_name} policy_decision_id does not match the policy decision artifact."
                )
            if artifact["execution_lane"] != lane:
                raise MergeConsistencyError(
                    f"{artifact_name} execution_lane does not match the policy lane."
                )
            if artifact["risk_tier"] != policy_artifact["risk_tier"]:
                raise MergeConsistencyError(
                    f"{artifact_name} risk_tier does not match the policy decision artifact."
                )
            if artifact["budget_class"] != policy_artifact["budget_class"]:
                raise MergeConsistencyError(
                    f"{artifact_name} budget_class does not match the policy decision artifact."
                )
            if artifact["rollback_class"] != policy_artifact["rollback_class"]:
                raise MergeConsistencyError(
                    f"{artifact_name} rollback_class does not match the policy decision artifact."
                )
            if sorted(artifact["approval_requirements"]) != sorted(
                policy_artifact["approval_requirements"]
            ):
                raise MergeConsistencyError(
                    f"{artifact_name} approval_requirements do not match the policy decision artifact."
                )

    @staticmethod
    def _validate_generated_consistency(
        pr_packet: dict[str, Any],
        security_review: dict[str, Any],
        merge_decision: dict[str, Any],
        work_item: WorkItem,
    ) -> None:
        if merge_decision["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise MergeConsistencyError(
                "merge-decision pr_packet_id does not match the updated pr-packet artifact."
            )
        if merge_decision["security_review_id"] != security_review["artifact"]["id"]:
            raise MergeConsistencyError(
                "merge-decision security_review_id does not match the provided security-review."
            )
        if merge_decision["build_attempt"] != work_item.attempt_count:
            raise MergeConsistencyError(
                "merge-decision build_attempt does not match the work-item attempt count."
            )
        if merge_decision["evaluated_pr_artifact_version"] + 1 != merge_decision["resulting_pr_artifact_version"]:
            raise MergeConsistencyError(
                "merge-decision resulting_pr_artifact_version must be exactly one greater than the evaluated version."
            )
        if merge_decision["resulting_pr_artifact_version"] != pr_packet["artifact"]["version"]:
            raise MergeConsistencyError(
                "merge-decision resulting_pr_artifact_version does not match the updated pr-packet version."
            )

        decision_status = merge_decision["merge_decision"]["status"]
        merge_execution = pr_packet.get("merge_execution")
        if not isinstance(merge_execution, dict):
            raise MergeConsistencyError(
                "updated pr-packet must include merge_execution metadata after the merge stage."
            )
        if decision_status == "merged":
            if merge_execution["status"] != "merged":
                raise MergeConsistencyError(
                    "merged merge-decision must leave merge_execution.status as merged."
                )
            if not pr_packet["merge_readiness"]["mergeable"]:
                raise MergeConsistencyError(
                    "merged PR must remain mergeable for downstream release stages."
                )
        elif decision_status == "pending_human":
            if merge_execution["status"] != "pending_human":
                raise MergeConsistencyError(
                    "pending human merge-decision must leave merge_execution.status as pending_human."
                )
            if pr_packet["merge_readiness"]["mergeable"]:
                raise MergeConsistencyError(
                    "pending human merge-decision cannot leave the pr-packet mergeable."
                )
            if not pr_packet["merge_readiness"]["reviewable"]:
                raise MergeConsistencyError(
                    "pending human merge-decision must keep the pr-packet reviewable."
                )
        else:
            if merge_execution["status"] != "blocked":
                raise MergeConsistencyError(
                    "blocked merge-decision must leave merge_execution.status as blocked."
                )
            if pr_packet["merge_readiness"]["mergeable"]:
                raise MergeConsistencyError(
                    "blocked merge-decision cannot leave the pr-packet mergeable."
                )
            if pr_packet["reviewer_report"]["approved"]:
                raise MergeConsistencyError(
                    "blocked merge-decision cannot leave the reviewer_report approved."
                )
