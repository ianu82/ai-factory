from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .connectors import AgentConnector, AgentTask, FactoryConnectorError
from .contracts import load_validators, validation_errors_for
from .controller import ControllerEvent, ControllerState, FactoryController, WorkItem
from .eval_common import pending_merge_gate_tiers
from .intake import build_identifier, normalize_whitespace, repo_root, slugify, utc_now


class BuildReviewError(RuntimeError):
    """Base class for Stage 3 build/review failures."""


class BuildReviewEligibilityError(BuildReviewError):
    """Raised when a work item cannot enter the Stage 3 build/review flow."""


class BuildReviewConsistencyError(BuildReviewError):
    """Raised when Stage 2 artifacts disagree about the work item being built."""


@dataclass(slots=True)
class Stage3BuildReviewResult:
    spec_packet: dict[str, Any]
    policy_decision: dict[str, Any]
    ticket_bundle: dict[str, Any]
    eval_manifest: dict[str, Any]
    pr_packet: dict[str, Any]
    work_item: WorkItem

    def to_document(self) -> dict[str, Any]:
        return {
            "spec_packet": self.spec_packet,
            "policy_decision": self.policy_decision,
            "ticket_bundle": self.ticket_bundle,
            "eval_manifest": self.eval_manifest,
            "pr_packet": self.pr_packet,
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


class BudgetGuardian:
    """Enforce Stage 3 build-loop limits before the builder starts."""

    def assert_build_allowed(
        self,
        work_item: WorkItem,
        policy_decision: dict[str, Any],
    ) -> None:
        max_cycles = int(policy_decision["budget_policy"]["max_pr_review_cycles"])
        if work_item.attempt_count >= max_cycles:
            raise BuildReviewEligibilityError(
                "Build loop retry budget is exhausted for this work item."
            )


class Builder:
    """Draft a PR packet from the Stage 2 ticket and eval plan."""

    PROMPT_CONTRACT_ID = "builder.v1"
    DEFAULT_TARGET_BRANCH = "main"
    _DEFAULT_MERGE_METHOD_BY_LANE = {
        "fast": "squash",
        "guarded": "squash",
        "restricted": "merge_commit",
    }
    PATH_HINTS = {
        "api_contract": [
            "integrations/anthropic/contracts.py",
            "tests/contracts/test_anthropic_contracts.py",
        ],
        "tool_runtime": [
            "integrations/anthropic/tool_runtime.py",
            "tests/integration/test_anthropic_tool_runtime.py",
        ],
        "model_routing": [
            "integrations/anthropic/routing.py",
            "tests/integration/test_anthropic_routing.py",
        ],
        "sdk": [
            "integrations/anthropic/sdk_adapter.py",
            "tests/unit/test_anthropic_sdk_adapter.py",
        ],
        "control_plane": [
            "control_plane/anthropic_flags.py",
            "tests/unit/test_anthropic_flags.py",
        ],
        "billing": [
            "billing/anthropic_metering.py",
            "tests/unit/test_anthropic_metering.py",
        ],
        "auth": [
            "auth/anthropic_permissions.py",
            "tests/unit/test_anthropic_permissions.py",
        ],
        "anthropic_integration": [
            "integrations/anthropic/client.py",
            "tests/integration/test_anthropic_client.py",
        ],
    }
    FACTORY_PATH_HINTS = {
        "api_contract": [
            "src/auto_mindsdb_factory/vertical_slice.py",
            "tests/test_vertical_slice.py",
        ],
        "tool_runtime": [
            "src/auto_mindsdb_factory/connectors.py",
            "tests/test_connectors.py",
        ],
        "model_routing": [
            "src/auto_mindsdb_factory/vertical_slice.py",
            "tests/test_vertical_slice.py",
        ],
        "sdk": [
            "src/auto_mindsdb_factory/connectors.py",
            "tests/test_connectors.py",
        ],
        "control_plane": [
            "src/auto_mindsdb_factory/__main__.py",
            "tests/test_cli.py",
        ],
        "billing": [
            "src/auto_mindsdb_factory/release_staging.py",
            "tests/test_release_staging.py",
        ],
        "auth": [
            "src/auto_mindsdb_factory/security_review.py",
            "tests/test_security_review.py",
        ],
        "anthropic_integration": [
            "src/auto_mindsdb_factory/intake.py",
            "tests/test_intake.py",
        ],
    }

    def __init__(
        self,
        *,
        agent_connector: AgentConnector | None = None,
    ) -> None:
        self.agent_connector = agent_connector

    def build_pr_packet(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        ticket_bundle: dict[str, Any],
        eval_manifest: dict[str, Any],
        *,
        artifact_id: str,
        repository: str,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        created_at = timestamp or utc_now()
        policy_artifact = policy_decision["artifact"]
        pending_tiers = self._pending_eval_tiers(eval_manifest)
        draft, model_fingerprint = self._agent_pr_draft(
            spec_packet,
            ticket_bundle,
            eval_manifest,
            repository,
        )

        return {
            "artifact": {
                "id": artifact_id,
                "version": 1,
                "source_stage": "build",
                "next_stage": "review",
                "status": "draft",
                "risk_tier": policy_artifact["risk_tier"],
                "execution_lane": policy_artifact["execution_lane"],
                "owner_agent": "Builder",
                "policy_decision_id": policy_artifact["id"],
                "model_fingerprint": model_fingerprint or self.PROMPT_CONTRACT_ID,
                "budget_class": policy_artifact["budget_class"],
                "rollback_class": policy_artifact["rollback_class"],
                "approval_requirements": list(policy_artifact["approval_requirements"]),
                "created_at": created_at,
                "updated_at": created_at,
            },
            "spec_packet_id": spec_packet["artifact"]["id"],
            "ticket_ids": [ticket["id"] for ticket in ticket_bundle["tickets"]],
            "eval_manifest_id": eval_manifest["artifact"]["id"],
            "branch_name": self._branch_name(spec_packet),
            "pull_request": {
                "repository": repository,
                "number": None,
                "url": None,
                "title": self._pull_request_title(spec_packet),
            },
            "summary": {
                "what_changed": (
                    draft["what_changed"] if draft is not None else self._what_changed(ticket_bundle)
                ),
                "key_risks": (
                    draft["key_risks"] if draft is not None else self._key_risks(ticket_bundle)
                ),
                "migrations": self._migrations(ticket_bundle),
                "rollback_notes": self._rollback_notes(ticket_bundle, policy_artifact["rollback_class"]),
            },
            "changed_paths": self._merge_changed_paths(
                spec_packet,
                draft["changed_paths"] if draft is not None else None,
            ),
            "checks": self._checks(eval_manifest),
            "reviewer_report": {
                "approved": False,
                "blocking_findings": [],
                "non_blocking_findings": [],
            },
            "merge_execution": {
                "target_branch": self.DEFAULT_TARGET_BRANCH,
                "method": self._DEFAULT_MERGE_METHOD_BY_LANE[
                    policy_decision["lane_assignment"]["lane"]
                ],
                "status": "not_started",
                "merge_commit_sha": None,
                "merged_at": None,
                "merged_by": None,
            },
            "merge_readiness": {
                "reviewable": False,
                "mergeable": False,
                "blockers": self._draft_blockers(pending_tiers),
            },
        }

    @staticmethod
    def _branch_name(spec_packet: dict[str, Any]) -> str:
        title_slug = slugify(spec_packet["source"]["title"])[:48].strip("-")
        if not title_slug:
            title_slug = "anthropic-change"
        return f"factory/{title_slug}"

    @staticmethod
    def _pull_request_title(spec_packet: dict[str, Any]) -> str:
        title = normalize_whitespace(spec_packet["source"]["title"]).rstrip(".")
        return f"Implement {title}"

    @staticmethod
    def _what_changed(ticket_bundle: dict[str, Any]) -> list[str]:
        changes: list[str] = []
        for ticket in ticket_bundle["tickets"]:
            changes.append(
                f"{ticket['title']} with scope anchored on {ticket['scope'][0].rstrip('.')}"
            )
        return changes

    @staticmethod
    def _key_risks(ticket_bundle: dict[str, Any]) -> list[str]:
        risks: list[str] = []
        for ticket in ticket_bundle["tickets"]:
            risks.extend(ticket["known_edge_cases"][:1])
        return Builder._dedupe(risks)

    @staticmethod
    def _migrations(ticket_bundle: dict[str, Any]) -> list[str]:
        migrations: list[str] = []
        for ticket in ticket_bundle["tickets"]:
            if ticket["kind"] == "data":
                migrations.append("Migration safety review is required before merge.")
        return migrations

    @staticmethod
    def _rollback_notes(ticket_bundle: dict[str, Any], rollback_class: str) -> str:
        if ticket_bundle["tickets"]:
            return ticket_bundle["tickets"][0]["rollback_strategy"]
        if rollback_class == "manual_recovery_required":
            return "Manual recovery is required before resuming writes or customer traffic."
        return "Disable the feature flag or revert the deploy to restore the previous behavior."

    @classmethod
    def _changed_paths(cls, spec_packet: dict[str, Any]) -> list[str]:
        paths: list[str] = []
        for surface in spec_packet["summary"]["affected_surfaces"]:
            paths.extend(cls._path_hints_for(spec_packet, surface))
        return cls._dedupe(paths)[:8] or [
            "integrations/anthropic/feature.py",
            "tests/integration/test_anthropic_feature.py",
        ]

    @classmethod
    def _merge_changed_paths(
        cls,
        spec_packet: dict[str, Any],
        agent_paths: list[str] | None,
    ) -> list[str]:
        deterministic_paths = cls._changed_paths(spec_packet)
        if not agent_paths:
            return deterministic_paths
        suggested = [
            normalize_whitespace(path)
            for path in agent_paths
            if isinstance(path, str)
            and path.strip()
            and not path.startswith("/")
            and ".." not in path.split("/")
        ]
        return cls._dedupe(suggested + deterministic_paths)[:8]

    @classmethod
    def _path_hints_for(cls, spec_packet: dict[str, Any], surface: str) -> list[str]:
        source = spec_packet.get("source", {})
        if source.get("kind") == "manual_intake" and source.get("provider") in {
            "github",
            "internal",
            "manual",
        }:
            return cls.FACTORY_PATH_HINTS.get(surface, cls.PATH_HINTS.get(surface, []))
        return cls.PATH_HINTS.get(surface, [])

    @staticmethod
    def _checks(eval_manifest: dict[str, Any]) -> list[dict[str, Any]]:
        checks: list[dict[str, Any]] = []
        for tier in eval_manifest["tiers"]:
            for check in tier["checks"]:
                if not check["required"]:
                    continue
                checks.append(
                    {
                        "name": check["name"],
                        "status": "passed" if tier["name"] == "pr_smoke" else "pending",
                    }
                )
        return checks

    @staticmethod
    def _pending_eval_tiers(eval_manifest: dict[str, Any]) -> list[str]:
        return pending_merge_gate_tiers(
            eval_manifest,
            completed_tiers={"pr_smoke"},
        )

    @classmethod
    def _draft_blockers(cls, pending_tiers: list[str]) -> list[str]:
        blockers = ["Reviewer sign-off is still pending."]
        if pending_tiers:
            blockers.append(
                f"Pending eval tiers before merge: {', '.join(pending_tiers)}."
            )
        return blockers

    @staticmethod
    def _dedupe(items: list[str]) -> list[str]:
        deduped: list[str] = []
        for item in items:
            if item not in deduped:
                deduped.append(item)
        return deduped

    def _agent_pr_draft(
        self,
        spec_packet: dict[str, Any],
        ticket_bundle: dict[str, Any],
        eval_manifest: dict[str, Any],
        repository: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        if self.agent_connector is None:
            return None, None
        task = AgentTask(
            name="stage3_pr_draft",
            instructions=(
                "You are the Stage 3 builder agent. "
                "Draft a concise PR summary from the supplied tickets and spec. "
                "Return concrete 'what changed' bullets, the most important key risks, "
                "and plausible repo-relative changed paths. "
                "Do not invent unrelated work or refer to files outside the repository."
            ),
            input_document={
                "repository": repository,
                "source_title": spec_packet["source"]["title"],
                "problem": spec_packet["summary"]["problem"],
                "proposed_capability": spec_packet["summary"]["proposed_capability"],
                "affected_surfaces": spec_packet["summary"]["affected_surfaces"],
                "open_questions": [question["question"] for question in spec_packet["open_questions"]],
                "tickets": [
                    {
                        "id": ticket["id"],
                        "title": ticket["title"],
                        "kind": ticket["kind"],
                        "summary": ticket["summary"],
                        "scope": ticket["scope"],
                        "known_edge_cases": ticket["known_edge_cases"],
                    }
                    for ticket in ticket_bundle["tickets"]
                ],
                "required_eval_tiers": [tier["name"] for tier in eval_manifest["tiers"]],
                "deterministic_path_hints": self._changed_paths(spec_packet),
            },
            output_schema=self._builder_draft_schema(),
        )
        try:
            result = self.agent_connector.run_task(task)
        except FactoryConnectorError as exc:
            raise BuildReviewError(f"Agent-assisted PR drafting failed: {exc}") from exc
        draft = result.output_document
        return {
            "what_changed": self._dedupe(
                [normalize_whitespace(item) for item in draft["what_changed"]]
            )[:4],
            "key_risks": self._dedupe(
                [normalize_whitespace(item) for item in draft["key_risks"]]
            )[:4],
            "changed_paths": self._dedupe(
                [normalize_whitespace(item) for item in draft["changed_paths"]]
            )[:8],
        }, result.model_fingerprint

    @staticmethod
    def _builder_draft_schema() -> dict[str, Any]:
        return {
            "type": "object",
            "additionalProperties": False,
            "required": ["what_changed", "key_risks", "changed_paths"],
            "properties": {
                "what_changed": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 4,
                    "items": {"type": "string", "minLength": 1},
                },
                "key_risks": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 4,
                    "items": {"type": "string", "minLength": 1},
                },
                "changed_paths": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 8,
                    "items": {"type": "string", "minLength": 1},
                },
            },
        }


class Reviewer:
    """Review the builder draft and decide if the PR is reviewable."""

    PROMPT_CONTRACT_ID = "reviewer.v1"

    def __init__(
        self,
        *,
        agent_connector: AgentConnector | None = None,
    ) -> None:
        self.agent_connector = agent_connector

    def review_pr_packet(
        self,
        pr_packet: dict[str, Any],
        spec_packet: dict[str, Any],
        eval_manifest: dict[str, Any],
        *,
        blocking_findings: list[str] | None = None,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        reviewed_packet = deepcopy(pr_packet)
        updated_at = timestamp or utc_now()
        pending_tiers = Builder._pending_eval_tiers(eval_manifest)
        agent_review, model_fingerprint = self._agent_review(
            pr_packet,
            spec_packet,
            eval_manifest,
        )
        findings = self._dedupe(
            list(blocking_findings or [])
            + (agent_review["blocking_findings"] if agent_review is not None else [])
        )
        non_blocking = self._dedupe(
            (agent_review["non_blocking_findings"] if agent_review is not None else [])
            + self._non_blocking_findings(spec_packet, pending_tiers)
        )
        approved = not findings

        reviewed_packet["artifact"]["owner_agent"] = "Reviewer"
        reviewed_packet["artifact"]["model_fingerprint"] = (
            model_fingerprint or self.PROMPT_CONTRACT_ID
        )
        reviewed_packet["artifact"]["updated_at"] = updated_at
        reviewed_packet["artifact"]["status"] = "approved" if approved else "blocked"
        reviewed_packet["artifact"]["next_stage"] = "eval" if approved else "build"
        reviewed_packet["artifact"]["blocking_issues"] = findings
        reviewed_packet["reviewer_report"] = {
            "approved": approved,
            "blocking_findings": findings,
            "non_blocking_findings": non_blocking,
        }
        blockers = list(findings)
        if pending_tiers:
            blockers.append(
                f"Pending eval tiers before merge: {', '.join(pending_tiers)}."
            )
        reviewed_packet["merge_readiness"] = {
            "reviewable": approved,
            "mergeable": approved and not pending_tiers,
            "blockers": blockers,
        }
        return reviewed_packet

    @staticmethod
    def _non_blocking_findings(
        spec_packet: dict[str, Any],
        pending_tiers: list[str],
    ) -> list[str]:
        findings: list[str] = []
        if pending_tiers:
            findings.append(
                f"Pending eval tiers before merge: {', '.join(pending_tiers)}."
            )
        for question in spec_packet["open_questions"]:
            findings.append(f"Carry open question: {question['question']}")
        return findings[:3]

    def _agent_review(
        self,
        pr_packet: dict[str, Any],
        spec_packet: dict[str, Any],
        eval_manifest: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str | None]:
        if self.agent_connector is None:
            return None, None
        task = AgentTask(
            name="stage3_pr_review",
            instructions=(
                "You are the Stage 3 reviewer agent. "
                "Identify only material blocking findings or worthwhile non-blocking findings "
                "for this PR draft. Focus on correctness, architecture fit, and edge cases. "
                "Do not repeat the known pending eval tiers or open questions, because the factory adds those separately."
            ),
            input_document={
                "pull_request_title": pr_packet["pull_request"]["title"],
                "summary": pr_packet["summary"],
                "changed_paths": pr_packet["changed_paths"],
                "checks": pr_packet["checks"],
                "problem": spec_packet["summary"]["problem"],
                "acceptance_criteria": [
                    criterion["description"] for criterion in spec_packet["acceptance_criteria"]
                ],
                "open_questions": [question["question"] for question in spec_packet["open_questions"]],
                "pending_eval_tiers": Builder._pending_eval_tiers(eval_manifest),
            },
            output_schema=self._review_schema(),
        )
        try:
            result = self.agent_connector.run_task(task)
        except FactoryConnectorError as exc:
            raise BuildReviewError(f"Agent-assisted PR review failed: {exc}") from exc
        review = result.output_document
        return {
            "blocking_findings": self._dedupe(
                [normalize_whitespace(item) for item in review["blocking_findings"]]
            )[:3],
            "non_blocking_findings": self._dedupe(
                [normalize_whitespace(item) for item in review["non_blocking_findings"]]
            )[:3],
        }, result.model_fingerprint

    @staticmethod
    def _review_schema() -> dict[str, Any]:
        return {
            "type": "object",
            "additionalProperties": False,
            "required": ["blocking_findings", "non_blocking_findings"],
            "properties": {
                "blocking_findings": {
                    "type": "array",
                    "maxItems": 3,
                    "items": {"type": "string", "minLength": 1},
                },
                "non_blocking_findings": {
                    "type": "array",
                    "maxItems": 3,
                    "items": {"type": "string", "minLength": 1},
                },
            },
        }

    @staticmethod
    def _dedupe(items: list[str]) -> list[str]:
        deduped: list[str] = []
        for item in items:
            if item not in deduped:
                deduped.append(item)
        return deduped


class Stage3BuildReviewPipeline:
    """Run Budget Guardian -> Builder -> Reviewer to produce a reviewable PR packet."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        controller: FactoryController | None = None,
        budget_guardian: BudgetGuardian | None = None,
        builder: Builder | None = None,
        reviewer: Reviewer | None = None,
        agent_connector: AgentConnector | None = None,
    ) -> None:
        self.root = repo_root(root)
        self.controller = controller or FactoryController()
        self.budget_guardian = budget_guardian or BudgetGuardian()
        self.builder = builder or Builder(agent_connector=agent_connector)
        self.reviewer = reviewer or Reviewer(agent_connector=agent_connector)
        self.validators = load_validators(self.root)

    def process(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        ticket_bundle: dict[str, Any],
        eval_manifest: dict[str, Any],
        work_item: WorkItem,
        *,
        pr_packet_id: str | None = None,
        repository: str = "mindsdb/platform",
        blocking_findings: list[str] | None = None,
    ) -> Stage3BuildReviewResult:
        self._validate_document("spec-packet", spec_packet)
        self._validate_document("policy-decision", policy_decision)
        self._validate_document("ticket-bundle", ticket_bundle)
        self._validate_document("eval-manifest", eval_manifest)
        self._validate_document("work-item", work_item.to_document())
        self._validate_consistency(
            spec_packet,
            policy_decision,
            ticket_bundle,
            eval_manifest,
            work_item,
        )

        if policy_decision["decision"] != "active_build_candidate":
            raise BuildReviewEligibilityError(
                "Only active_build_candidate items can advance to build/review."
            )
        if work_item.state not in {ControllerState.TICKETED, ControllerState.PR_REVISION}:
            raise BuildReviewEligibilityError(
                "Work item must be in TICKETED or PR_REVISION before build/review; "
                f"got {work_item.state.value}."
            )

        self.budget_guardian.assert_build_allowed(work_item, policy_decision)

        timestamp = utc_now()
        packet_artifact_id = pr_packet_id or build_identifier(
            "pr",
            spec_packet["artifact"]["id"],
            max_length=64,
        )

        working_item = deepcopy(work_item)
        if working_item.state is ControllerState.TICKETED:
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.BUILD_SLOT_RESERVED,
                occurred_at=timestamp,
            )
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.BUILDER_STARTED,
                occurred_at=timestamp,
            )
        else:
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.BUILDER_RETRY_STARTED,
                occurred_at=timestamp,
            )

        builder_timestamp = working_item.updated_at
        pr_packet = self.builder.build_pr_packet(
            spec_packet,
            policy_decision,
            ticket_bundle,
            eval_manifest,
            artifact_id=packet_artifact_id,
            repository=repository,
            timestamp=builder_timestamp,
        )
        self.controller.apply_event(
            working_item,
            event=ControllerEvent.PR_CREATED,
            artifact_id=pr_packet["artifact"]["id"],
            occurred_at=pr_packet["artifact"]["updated_at"],
        )

        reviewer_timestamp = utc_now()
        self.controller.apply_event(
            working_item,
            event=ControllerEvent.REVIEWER_STARTED,
            occurred_at=reviewer_timestamp,
        )
        pr_packet = self.reviewer.review_pr_packet(
            pr_packet,
            spec_packet,
            eval_manifest,
            blocking_findings=blocking_findings,
            timestamp=reviewer_timestamp,
        )
        self._validate_document("pr-packet", pr_packet)

        if pr_packet["reviewer_report"]["blocking_findings"]:
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.BLOCKING_FINDINGS_PRESENT,
                artifact_id=pr_packet["artifact"]["id"],
                occurred_at=pr_packet["artifact"]["updated_at"],
            )
        else:
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.REVIEWABLE_TRUE,
                artifact_id=pr_packet["artifact"]["id"],
                occurred_at=pr_packet["artifact"]["updated_at"],
            )

        self._validate_document("work-item", working_item.to_document())
        return Stage3BuildReviewResult(
            spec_packet=spec_packet,
            policy_decision=policy_decision,
            ticket_bundle=ticket_bundle,
            eval_manifest=eval_manifest,
            pr_packet=pr_packet,
            work_item=working_item,
        )

    def _validate_document(self, schema_name: str, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.validators[schema_name], document)
        if errors:
            raise BuildReviewError(f"{schema_name} failed validation: {'; '.join(errors)}")

    @staticmethod
    def _validate_consistency(
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        ticket_bundle: dict[str, Any],
        eval_manifest: dict[str, Any],
        work_item: WorkItem,
    ) -> None:
        spec_packet_id = spec_packet["artifact"]["id"]
        policy_artifact = policy_decision["artifact"]
        ticket_artifact = ticket_bundle["artifact"]
        eval_artifact = eval_manifest["artifact"]
        lane = policy_decision["lane_assignment"]["lane"]
        ticket_ids = [ticket["id"] for ticket in ticket_bundle["tickets"]]

        if policy_decision["spec_packet_id"] != spec_packet_id:
            raise BuildReviewConsistencyError(
                "policy-decision does not reference the provided spec-packet."
            )
        if policy_decision["decision"] != spec_packet["relevance"]["decision"]:
            raise BuildReviewConsistencyError(
                "policy-decision decision does not match the provided spec-packet."
            )
        if policy_decision["risk_score"] != spec_packet["risk_profile"]["risk_score"]:
            raise BuildReviewConsistencyError(
                "policy-decision risk score does not match the provided spec-packet."
            )
        if ticket_bundle["spec_packet_id"] != spec_packet_id:
            raise BuildReviewConsistencyError(
                "ticket-bundle does not reference the provided spec-packet."
            )
        if ticket_bundle["eval_manifest_id"] != eval_artifact["id"]:
            raise BuildReviewConsistencyError(
                "ticket-bundle eval_manifest_id does not match the provided eval-manifest."
            )
        if eval_manifest["target_type"] != "ticket_bundle":
            raise BuildReviewConsistencyError("eval-manifest target_type must be ticket_bundle.")
        if eval_manifest["target_id"] != ticket_artifact["id"]:
            raise BuildReviewConsistencyError(
                "eval-manifest target_id does not match the provided ticket-bundle."
            )
        tier_names = [tier["name"] for tier in eval_manifest["tiers"]]
        if "pr_smoke" not in tier_names:
            raise BuildReviewConsistencyError("eval-manifest must include the pr_smoke tier.")
        if sorted(tier_names) != sorted(policy_decision["required_eval_tiers"]):
            raise BuildReviewConsistencyError(
                "eval-manifest tiers do not match the policy decision required tiers."
            )
        if len(ticket_ids) != len(set(ticket_ids)):
            raise BuildReviewConsistencyError("ticket-bundle contains duplicate ticket ids.")
        if work_item.source_provider != spec_packet["source"]["provider"]:
            raise BuildReviewConsistencyError(
                "work-item source_provider does not match the provided spec-packet."
            )
        if work_item.source_external_id != spec_packet["source"]["external_id"]:
            raise BuildReviewConsistencyError(
                "work-item source_external_id does not match the provided spec-packet."
            )
        if work_item.risk_score != policy_decision["risk_score"]:
            raise BuildReviewConsistencyError(
                "work-item risk_score does not match the policy decision."
            )
        if work_item.policy_decision_id != policy_artifact["id"]:
            raise BuildReviewConsistencyError(
                "work-item policy_decision_id does not match the policy decision artifact."
            )
        if work_item.execution_lane != lane:
            raise BuildReviewConsistencyError(
                "work-item execution_lane does not match the policy lane."
            )
        if work_item.state is ControllerState.TICKETED and work_item.current_artifact_id != ticket_artifact["id"]:
            raise BuildReviewConsistencyError(
                "work-item current_artifact_id must match the provided ticket-bundle artifact."
            )
        if work_item.state is ControllerState.PR_REVISION and not work_item.current_artifact_id:
            raise BuildReviewConsistencyError(
                "work-item in PR_REVISION must retain the previous PR artifact id."
            )
        if ticket_artifact["policy_decision_id"] != policy_artifact["id"]:
            raise BuildReviewConsistencyError(
                "ticket-bundle policy_decision_id does not match the policy decision artifact."
            )
        if eval_artifact["policy_decision_id"] != policy_artifact["id"]:
            raise BuildReviewConsistencyError(
                "eval-manifest policy_decision_id does not match the policy decision artifact."
            )
        if ticket_artifact["execution_lane"] != lane or eval_artifact["execution_lane"] != lane:
            raise BuildReviewConsistencyError(
                "ticket-bundle/eval-manifest execution_lane does not match the policy lane."
            )
        for artifact_name, artifact in (
            ("ticket-bundle", ticket_artifact),
            ("eval-manifest", eval_artifact),
        ):
            if artifact["risk_tier"] != policy_artifact["risk_tier"]:
                raise BuildReviewConsistencyError(
                    f"{artifact_name} risk_tier does not match the policy decision artifact."
                )
            if artifact["budget_class"] != policy_artifact["budget_class"]:
                raise BuildReviewConsistencyError(
                    f"{artifact_name} budget_class does not match the policy decision artifact."
                )
            if artifact["rollback_class"] != policy_artifact["rollback_class"]:
                raise BuildReviewConsistencyError(
                    f"{artifact_name} rollback_class does not match the policy decision artifact."
                )
            if sorted(artifact["approval_requirements"]) != sorted(
                policy_artifact["approval_requirements"]
            ):
                raise BuildReviewConsistencyError(
                    f"{artifact_name} approval_requirements do not match the policy decision artifact."
                )
        for ticket in ticket_bundle["tickets"]:
            if ticket["execution_lane"] != lane:
                raise BuildReviewConsistencyError(
                    f"ticket {ticket['id']} execution_lane does not match the policy lane."
                )
            if sorted(ticket["required_eval_tiers"]) != sorted(policy_decision["required_eval_tiers"]):
                raise BuildReviewConsistencyError(
                    f"ticket {ticket['id']} required_eval_tiers do not match the policy decision."
                )
