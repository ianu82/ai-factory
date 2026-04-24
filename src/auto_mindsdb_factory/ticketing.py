from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .connectors import AgentConnector, AgentTask, FactoryConnectorError
from .contracts import load_validators, validation_errors_for
from .controller import ControllerEvent, ControllerState, FactoryController, WorkItem
from .intake import build_identifier, normalize_whitespace, repo_root, utc_now


class TicketingError(RuntimeError):
    """Base class for Stage 2 planning failures."""


class TicketingEligibilityError(TicketingError):
    """Raised when a work item is not eligible to enter ticketing."""


class TicketingConsistencyError(TicketingError):
    """Raised when Stage 1 artifacts do not describe the same work item."""


@dataclass(slots=True)
class Stage2TicketingResult:
    spec_packet: dict[str, Any]
    policy_decision: dict[str, Any]
    ticket_bundle: dict[str, Any]
    eval_manifest: dict[str, Any]
    work_item: WorkItem

    def to_document(self) -> dict[str, Any]:
        return {
            "spec_packet": self.spec_packet,
            "policy_decision": self.policy_decision,
            "ticket_bundle": self.ticket_bundle,
            "eval_manifest": self.eval_manifest,
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


@dataclass(frozen=True, slots=True)
class TicketSlice:
    slug: str
    title_suffix: str
    kind: str
    surfaces: tuple[str, ...]
    scope_items: tuple[str, ...]
    definition_of_done: tuple[str, ...]


class TicketArchitect:
    """Translate a Stage 1 packet into buildable 1-2 day tickets."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        agent_connector: AgentConnector | None = None,
    ) -> None:
        self.root = repo_root(root)
        self.agent_connector = agent_connector

    def build_ticket_bundle(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        *,
        artifact_id: str,
        eval_manifest_id: str,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        created_at = timestamp or utc_now()
        policy_artifact = policy_decision["artifact"]
        tickets, model_fingerprint = self._tickets_for(spec_packet, policy_decision)

        artifact = {
            "id": artifact_id,
            "version": 1,
            "source_stage": "ticketing",
            "next_stage": "build",
            "status": "ready",
            "risk_tier": policy_artifact["risk_tier"],
            "execution_lane": policy_artifact["execution_lane"],
            "owner_agent": "Ticket Architect",
            "policy_decision_id": policy_artifact["id"],
            "budget_class": policy_artifact["budget_class"],
            "rollback_class": policy_artifact["rollback_class"],
            "approval_requirements": list(policy_artifact["approval_requirements"]),
            "created_at": created_at,
            "updated_at": created_at,
        }
        if model_fingerprint is not None:
            artifact["model_fingerprint"] = model_fingerprint

        return {
            "artifact": artifact,
            "spec_packet_id": spec_packet["artifact"]["id"],
            "eval_manifest_id": eval_manifest_id,
            "tickets": tickets,
            "dependency_graph": self._dependency_graph(tickets),
            "budget_summary": {
                "total_token_budget_usd": self._budget_summary(policy_decision),
            },
        }

    def _tickets_for(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], str | None]:
        factors = {factor["name"] for factor in spec_packet["risk_profile"]["factors"]}
        ticket_slices = self._ticket_slices(spec_packet)
        ticket_drafts, model_fingerprint = self._agent_ticket_drafts(
            spec_packet,
            policy_decision,
            ticket_slices,
        )
        ticket_ids = [
            build_identifier(
                "ticket",
                f"{spec_packet['artifact']['id']}-{ticket_slice.slug}",
                max_length=60,
            )
            for ticket_slice in ticket_slices
        ]
        tickets: list[dict[str, Any]] = []
        for index, ticket_slice in enumerate(ticket_slices):
            draft = ticket_drafts.get(ticket_slice.slug)
            dependencies = [ticket_ids[index - 1]] if index > 0 else []
            scope = self._scope_items(
                draft["scope"] if draft is not None else ticket_slice.scope_items,
                policy_decision,
            )
            tickets.append(
                {
                    "id": ticket_ids[index],
                    "title": self._ticket_title(spec_packet, ticket_slice),
                    "kind": ticket_slice.kind,
                    "summary": (
                        draft["summary"]
                        if draft is not None
                        else self._summary(spec_packet, ticket_slice)
                    ),
                    "scope": scope,
                    "definition_of_done": self._definition_of_done(
                        draft["definition_of_done"]
                        if draft is not None
                        else ticket_slice.definition_of_done,
                        spec_packet,
                        policy_decision,
                    ),
                    "known_edge_cases": self._known_edge_cases(
                        spec_packet,
                        ticket_slice,
                        drafted_edge_cases=(
                            draft["known_edge_cases"] if draft is not None else None
                        ),
                    ),
                    "non_goals": list(spec_packet["summary"]["non_goals"]),
                    "dependencies": dependencies,
                    "eta_days": self._eta_days(scope, ticket_slice.kind),
                    "risk_tier": policy_decision["artifact"]["risk_tier"],
                    "execution_lane": policy_decision["lane_assignment"]["lane"],
                    "required_eval_tiers": list(policy_decision["required_eval_tiers"]),
                    "allowed_tools": self._allowed_tools(ticket_slice.kind),
                    "secret_scope": self._secret_scope(factors),
                    "rollback_strategy": self._rollback_strategy(
                        policy_decision["artifact"]["rollback_class"]
                    ),
                    "acceptance_criteria_refs": [
                        criterion["id"] for criterion in spec_packet["acceptance_criteria"]
                    ],
                }
            )
        return tickets, model_fingerprint

    @staticmethod
    def _ticket_title(spec_packet: dict[str, Any], ticket_slice: TicketSlice) -> str:
        source_title = normalize_whitespace(spec_packet["source"]["title"]).rstrip(".")
        return f"Implement {source_title}: {ticket_slice.title_suffix}"

    @staticmethod
    def _summary(spec_packet: dict[str, Any], ticket_slice: TicketSlice) -> str:
        capability = normalize_whitespace(spec_packet["summary"]["proposed_capability"])
        return f"{capability} Focus this ticket on {ticket_slice.title_suffix.lower()}."

    @staticmethod
    def _definition_of_done(
        drafted_done_items: tuple[str, ...] | list[str],
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
    ) -> list[str]:
        done_items = list(drafted_done_items)
        done_items.extend(
            criterion["description"] for criterion in spec_packet["acceptance_criteria"]
        )
        done_items.append(
            f"Required eval tiers pass for this ticket: {', '.join(policy_decision['required_eval_tiers'])}."
        )
        done_items.append("Rollback instructions are documented and tested at the ticket scope.")
        return TicketArchitect._dedupe(done_items)

    @staticmethod
    def _known_edge_cases(
        spec_packet: dict[str, Any],
        ticket_slice: TicketSlice,
        *,
        drafted_edge_cases: list[str] | None = None,
    ) -> list[str]:
        if drafted_edge_cases:
            return TicketArchitect._dedupe(drafted_edge_cases)[:4]
        factors = {factor["name"] for factor in spec_packet["risk_profile"]["factors"]}
        edge_cases: list[str] = []
        if ticket_slice.slug == "contract" and "external_api_contract_change" in factors:
            edge_cases.append(
                "Legacy callers should receive a deterministic contract error for unsupported request shapes."
            )
        if ticket_slice.slug == "integration" and "new_tool_permission" in factors:
            edge_cases.append(
                "Tool/schema mismatches must fail closed instead of running with partial permissions."
            )
        if ticket_slice.slug == "integration" and "model_behavior_change" in factors:
            edge_cases.append(
                "Golden-output regressions must stay within the configured quality threshold."
            )
        if ticket_slice.slug == "security" and "auth_or_permissions" in factors:
            edge_cases.append(
                "Permission checks must fail closed when new flags, claims, or tool scopes are missing."
            )
        if ticket_slice.slug == "data" and "irreversible_migration" in factors:
            edge_cases.append(
                "Migration retries must stay idempotent and preserve rollback or recovery instructions."
            )
        if ticket_slice.slug == "billing" and "billing_impact" in factors:
            edge_cases.append(
                "Cost accounting and quota enforcement should preserve the current default behavior."
            )
        if "low_test_coverage_area" in factors:
            edge_cases.append(
                "The touched area needs focused regression coverage before relying on broad suites."
            )
        return TicketArchitect._dedupe(edge_cases) or [
            "Feature-flag rollback should restore the previous behavior without manual cleanup."
        ]

    @staticmethod
    def _allowed_tools(kind: str) -> list[str]:
        if kind == "frontend":
            return ["git", "python", "pytest"]
        return ["git", "python", "pytest"]

    @staticmethod
    def _secret_scope(factors: set[str]) -> str:
        if {"auth_or_permissions", "sensitive_data_access", "billing_impact"} & factors:
            return "read_only"
        return "none"

    @staticmethod
    def _rollback_strategy(rollback_class: str) -> str:
        if rollback_class == "immediate_flag_disable":
            return "Disable the feature flag and redeploy if needed."
        if rollback_class == "reversible_deploy":
            return "Disable rollout traffic, revert the deploy, and preserve compatibility fallbacks."
        if rollback_class == "manual_recovery_required":
            return "Follow the manual recovery runbook before re-enabling writes or customer traffic."
        return "No rollback required beyond standard queue management."

    @staticmethod
    def _dependency_graph(tickets: list[dict[str, Any]]) -> list[dict[str, str]]:
        edges: list[dict[str, str]] = []
        for ticket in tickets:
            for dependency_id in ticket.get("dependencies", []):
                edges.append({"from": dependency_id, "to": ticket["id"], "type": "blocks"})
        return edges

    @staticmethod
    def _budget_summary(policy_decision: dict[str, Any]) -> float:
        max_budget = float(policy_decision["budget_policy"]["max_token_budget_usd"])
        return round(min(max_budget, max_budget * 0.8), 2)

    @classmethod
    def _ticket_slices(cls, spec_packet: dict[str, Any]) -> list[TicketSlice]:
        surfaces = set(spec_packet["summary"]["affected_surfaces"])
        factors = {factor["name"] for factor in spec_packet["risk_profile"]["factors"]}
        slices: list[TicketSlice] = []

        if "auth" in surfaces or {"auth_or_permissions", "sensitive_data_access"} & factors:
            slices.append(
                TicketSlice(
                    slug="security",
                    title_suffix="security boundary updates",
                    kind="security",
                    surfaces=("auth",) if "auth" in surfaces else tuple(),
                    scope_items=(
                        "Preserve permission boundaries and failure-closed behavior around the new path.",
                        "Add explicit authorization and denial-path coverage for the new capability.",
                    ),
                    definition_of_done=(
                        "Permission changes are explicit, least-privilege, and covered by negative-path tests.",
                    ),
                )
            )

        if {"api_contract", "sdk"} & surfaces:
            contract_surfaces = tuple(
                surface for surface in ("api_contract", "sdk") if surface in surfaces
            )
            slices.append(
                TicketSlice(
                    slug="contract",
                    title_suffix="contract compatibility",
                    kind="backend",
                    surfaces=contract_surfaces,
                    scope_items=(
                        "Update request and response contracts with compatibility-safe fallbacks.",
                        "Refresh adapter or SDK-facing boundaries touched by the new contract shape.",
                        "Document unsupported request shapes and deterministic contract failures.",
                    ),
                    definition_of_done=(
                        "Contract callers can adopt the new behavior without breaking existing request shapes.",
                    ),
                )
            )

        if {"tool_runtime", "model_routing", "anthropic_integration"} & surfaces:
            integration_surfaces = tuple(
                surface
                for surface in ("tool_runtime", "model_routing", "anthropic_integration")
                if surface in surfaces
            )
            slices.append(
                TicketSlice(
                    slug="integration",
                    title_suffix="runtime integration wiring",
                    kind="llm_integration",
                    surfaces=integration_surfaces,
                    scope_items=(
                        "Wire tool/runtime integration, schema handling, and retry-safe execution behavior.",
                        "Adjust model selection or Anthropic routing behavior needed for the new capability.",
                        "Keep the feature behind a flag or equivalent rollout control.",
                    ),
                    definition_of_done=(
                        "Runtime integration paths are deterministic, retry-safe, and guarded for rollout.",
                    ),
                )
            )

        if "control_plane" in surfaces:
            slices.append(
                TicketSlice(
                    slug="control-plane",
                    title_suffix="operator control-plane updates",
                    kind="frontend",
                    surfaces=("control_plane",),
                    scope_items=(
                        "Expose operator-facing controls or configuration needed for rollout.",
                        "Document the flag, setting, or UI surface that controls this capability.",
                    ),
                    definition_of_done=(
                        "Operators can enable, disable, and observe the new path without code changes.",
                    ),
                )
            )

        if "billing" in surfaces:
            slices.append(
                TicketSlice(
                    slug="billing",
                    title_suffix="cost and quota handling",
                    kind="infra",
                    surfaces=("billing",),
                    scope_items=(
                        "Capture cost, accounting, or quota implications in the implementation path.",
                        "Record the expected latency and cost signals needed for rollout.",
                    ),
                    definition_of_done=(
                        "Cost-sensitive behavior is measurable and preserves current quota safeguards.",
                    ),
                )
            )

        if "irreversible_migration" in factors:
            slices.append(
                TicketSlice(
                    slug="data",
                    title_suffix="migration safety",
                    kind="data",
                    surfaces=tuple(),
                    scope_items=(
                        "Isolate schema or data migration work behind reversible checkpoints where possible.",
                        "Write recovery instructions for partial progress or retry scenarios.",
                    ),
                    definition_of_done=(
                        "Migration or data backfill steps are idempotent and explicitly recoverable.",
                    ),
                )
            )

        if slices:
            return slices

        return [
            TicketSlice(
                slug="implementation",
                title_suffix="implementation work",
                kind=cls._ticket_kind(spec_packet["summary"]["affected_surfaces"], factors),
                surfaces=tuple(spec_packet["summary"]["affected_surfaces"]),
                scope_items=(
                    "Implement the acceptance criteria without expanding beyond the current Stage 1 scope.",
                ),
                definition_of_done=(
                    "The implementation satisfies the scoped acceptance criteria and preserves rollback behavior.",
                ),
            )
        ]

    @staticmethod
    def _ticket_kind(surfaces: list[str], factors: set[str]) -> str:
        if "auth_or_permissions" in factors or "auth" in surfaces:
            return "security"
        if "irreversible_migration" in factors:
            return "data"
        if "control_plane" in surfaces:
            return "frontend"
        if "tool_runtime" in surfaces or "model_routing" in surfaces:
            return "llm_integration"
        if "billing" in surfaces:
            return "infra"
        return "backend"

    @staticmethod
    def _scope_items(
        drafted_scope_items: tuple[str, ...] | list[str],
        policy_decision: dict[str, Any],
    ) -> list[str]:
        items = list(drafted_scope_items)
        items.extend(
            [
                "Implement the acceptance criteria without expanding beyond the current Stage 1 scope.",
                f"Prepare the lane-required eval coverage for {', '.join(policy_decision['required_eval_tiers'])}.",
            ]
        )
        return TicketArchitect._dedupe(items)[:5]

    def _agent_ticket_drafts(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        ticket_slices: list[TicketSlice],
    ) -> tuple[dict[str, dict[str, Any]], str | None]:
        if self.agent_connector is None or not ticket_slices:
            return {}, None
        seed_tickets = [
            {
                "slug": ticket_slice.slug,
                "kind": ticket_slice.kind,
                "title_suffix": ticket_slice.title_suffix,
                "seed_summary": self._summary(spec_packet, ticket_slice),
                "seed_scope": list(ticket_slice.scope_items),
                "seed_definition_of_done": list(ticket_slice.definition_of_done),
                "seed_known_edge_cases": self._known_edge_cases(spec_packet, ticket_slice),
            }
            for ticket_slice in ticket_slices
        ]
        task = AgentTask(
            name="stage2_ticket_drafting",
            instructions=(
                "You are the Stage 2 senior engineer agent. "
                "Draft ticket-specific summaries, scope bullets, definition-of-done bullets, "
                "and edge cases for the exact ticket slices provided. "
                "Keep each ticket buildable in 1-2 days, preserve the slug for each slice, "
                "stay inside the supplied acceptance criteria and non-goals, and avoid generic boilerplate."
            ),
            input_document={
                "source_title": spec_packet["source"]["title"],
                "problem": spec_packet["summary"]["problem"],
                "proposed_capability": spec_packet["summary"]["proposed_capability"],
                "acceptance_criteria": [
                    criterion["description"] for criterion in spec_packet["acceptance_criteria"]
                ],
                "non_goals": spec_packet["summary"]["non_goals"],
                "risk_factors": [
                    factor["name"] for factor in spec_packet["risk_profile"]["factors"]
                ],
                "required_eval_tiers": policy_decision["required_eval_tiers"],
                "ticket_slices": seed_tickets,
            },
            output_schema=self._ticket_draft_schema(ticket_slices),
        )
        try:
            result = self.agent_connector.run_task(task)
        except FactoryConnectorError as exc:
            raise TicketingError(f"Agent-assisted ticket drafting failed: {exc}") from exc

        drafts = result.output_document.get("tickets")
        if not isinstance(drafts, list):
            raise TicketingError("Agent-assisted ticket drafting did not return a tickets list.")
        draft_by_slug: dict[str, dict[str, Any]] = {}
        expected_slugs = {ticket_slice.slug for ticket_slice in ticket_slices}
        for draft in drafts:
            if not isinstance(draft, dict):
                raise TicketingError("Agent-assisted ticket draft entries must be objects.")
            slug = str(draft.get("slug") or "")
            if slug not in expected_slugs:
                raise TicketingError(
                    f"Agent-assisted ticket draft returned an unexpected slug: {slug or '<empty>'}."
                )
            if slug in draft_by_slug:
                raise TicketingError(
                    f"Agent-assisted ticket draft returned duplicate slug: {slug}."
                )
            draft_by_slug[slug] = {
                "summary": normalize_whitespace(str(draft["summary"])),
                "scope": self._dedupe([normalize_whitespace(item) for item in draft["scope"]])[:3],
                "definition_of_done": self._dedupe(
                    [normalize_whitespace(item) for item in draft["definition_of_done"]]
                )[:3],
                "known_edge_cases": self._dedupe(
                    [normalize_whitespace(item) for item in draft["known_edge_cases"]]
                )[:3],
            }
        missing = expected_slugs - set(draft_by_slug)
        if missing:
            raise TicketingError(
                "Agent-assisted ticket draft did not cover every required ticket slice: "
                + ", ".join(sorted(missing))
            )
        return draft_by_slug, result.model_fingerprint

    @staticmethod
    def _ticket_draft_schema(ticket_slices: list[TicketSlice]) -> dict[str, Any]:
        slugs = [ticket_slice.slug for ticket_slice in ticket_slices]
        return {
            "type": "object",
            "additionalProperties": False,
            "required": ["tickets"],
            "properties": {
                "tickets": {
                    "type": "array",
                    "minItems": len(ticket_slices),
                    "maxItems": len(ticket_slices),
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [
                            "slug",
                            "summary",
                            "scope",
                            "definition_of_done",
                            "known_edge_cases",
                        ],
                        "properties": {
                            "slug": {"type": "string", "enum": slugs},
                            "summary": {"type": "string", "minLength": 1},
                            "scope": {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": 3,
                                "items": {"type": "string", "minLength": 1},
                            },
                            "definition_of_done": {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": 3,
                                "items": {"type": "string", "minLength": 1},
                            },
                            "known_edge_cases": {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": 3,
                                "items": {"type": "string", "minLength": 1},
                            },
                        },
                    },
                }
            },
        }

    @staticmethod
    def _eta_days(scope: list[str], kind: str) -> float:
        base = 0.55 + (0.15 * len(scope))
        if kind in {"llm_integration", "security", "data"}:
            base += 0.25
        return min(2.0, round(base, 2))

    @staticmethod
    def _dedupe(items: list[str]) -> list[str]:
        deduped: list[str] = []
        for item in items:
            if item not in deduped:
                deduped.append(item)
        return deduped


class EvalEngineer:
    """Generate tiered eval manifests from lane policy."""

    CHECK_LIBRARY: dict[str, dict[str, Any]] = {
        "lint": {"name": "Lint", "kind": "lint", "timeout_minutes": 5},
        "typecheck": {"name": "Typecheck", "kind": "typecheck", "timeout_minutes": 10},
        "unit": {"name": "Unit tests", "kind": "unit", "timeout_minutes": 15},
        "contract": {"name": "Contract validation", "kind": "contract", "timeout_minutes": 10},
        "focused_llm_golden_set": {
            "name": "Focused golden-set quality check",
            "kind": "llm_quality",
            "timeout_minutes": 15,
        },
        "integration": {"name": "Integration tests", "kind": "integration", "timeout_minutes": 30},
        "migration_safety": {
            "name": "Migration safety checks",
            "kind": "migration_safety",
            "timeout_minutes": 20,
        },
        "targeted_regression": {
            "name": "Targeted regression suite",
            "kind": "llm_quality",
            "timeout_minutes": 20,
        },
        "latency_benchmark": {
            "name": "Latency benchmark",
            "kind": "latency",
            "timeout_minutes": 15,
        },
        "cost_benchmark": {
            "name": "Cost benchmark",
            "kind": "cost",
            "timeout_minutes": 15,
        },
        "broader_llm_regression": {
            "name": "Broader LLM regression suite",
            "kind": "llm_quality",
            "timeout_minutes": 30,
        },
        "adversarial_prompt_suite": {
            "name": "Adversarial prompt suite",
            "kind": "adversarial",
            "timeout_minutes": 40,
        },
        "long_running_integration": {
            "name": "Long-running integration suite",
            "kind": "integration",
            "timeout_minutes": 60,
        },
        "full_golden_dataset": {
            "name": "Full golden-dataset quality suite",
            "kind": "llm_quality",
            "timeout_minutes": 45,
        },
        "cross_model_comparison": {
            "name": "Cross-model comparison suite",
            "kind": "llm_quality",
            "timeout_minutes": 45,
        },
        "synthetic_shadow_replay": {
            "name": "Synthetic shadow replay",
            "kind": "integration",
            "timeout_minutes": 60,
        },
        "canary_error_rate": {
            "name": "Canary error-rate check",
            "kind": "integration",
            "timeout_minutes": 20,
        },
        "canary_latency": {
            "name": "Canary latency check",
            "kind": "latency",
            "timeout_minutes": 20,
        },
        "canary_cost": {
            "name": "Canary cost check",
            "kind": "cost",
            "timeout_minutes": 20,
        },
        "rollback_probe": {
            "name": "Rollback probe",
            "kind": "migration_safety",
            "timeout_minutes": 15,
        },
        "business_kpi_proxy": {
            "name": "Business KPI proxy check",
            "kind": "llm_quality",
            "timeout_minutes": 30,
        },
        "live_quality_sampling": {
            "name": "Live quality sampling",
            "kind": "llm_quality",
            "timeout_minutes": 30,
        },
    }

    def __init__(self, root: Path | None = None) -> None:
        self.root = repo_root(root)
        policy_path = self.root / "factory" / "policies" / "eval-tiers.yaml"
        with policy_path.open("r", encoding="utf-8") as handle:
            self.eval_policy = yaml.safe_load(handle)

    def build_eval_manifest(
        self,
        ticket_bundle: dict[str, Any],
        policy_decision: dict[str, Any],
        *,
        artifact_id: str,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        created_at = timestamp or utc_now()
        policy_artifact = policy_decision["artifact"]
        tiers: list[dict[str, Any]] = []
        for tier_name in policy_decision["required_eval_tiers"]:
            if tier_name not in self.eval_policy["tiers"]:
                raise TicketingError(f"Unknown eval tier in policy decision: {tier_name}")
            tiers.append(
                self._build_tier(
                    ticket_bundle["artifact"]["id"],
                    tier_name,
                    self.eval_policy["tiers"][tier_name],
                )
            )
        return {
            "artifact": {
                "id": artifact_id,
                "version": 1,
                "source_stage": "eval_generation",
                "next_stage": "build",
                "status": "ready",
                "risk_tier": policy_artifact["risk_tier"],
                "execution_lane": policy_artifact["execution_lane"],
                "owner_agent": "Eval Engineer",
                "policy_decision_id": policy_artifact["id"],
                "budget_class": policy_artifact["budget_class"],
                "rollback_class": policy_artifact["rollback_class"],
                "approval_requirements": list(policy_artifact["approval_requirements"]),
                "created_at": created_at,
                "updated_at": created_at,
            },
            "target_type": "ticket_bundle",
            "target_id": ticket_bundle["artifact"]["id"],
            "tiers": tiers,
        }

    def _build_tier(
        self,
        ticket_bundle_id: str,
        tier_name: str,
        tier_policy: dict[str, Any],
    ) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        for check_name in tier_policy["required_checks"]:
            checks.append(
                self._build_check(ticket_bundle_id, tier_name, check_name, tier_policy, required=True)
            )
        for check_name in tier_policy["optional_checks"]:
            checks.append(
                self._build_check(ticket_bundle_id, tier_name, check_name, tier_policy, required=False)
            )
        return {"name": tier_name, "checks": checks}

    def _build_check(
        self,
        ticket_bundle_id: str,
        tier_name: str,
        check_name: str,
        tier_policy: dict[str, Any],
        *,
        required: bool,
    ) -> dict[str, Any]:
        if check_name not in self.CHECK_LIBRARY:
            raise TicketingError(f"Unknown eval check template: {check_name}")

        template = self.CHECK_LIBRARY[check_name]
        check_id = build_identifier(
            "check",
            f"{tier_name}-{check_name}-{ticket_bundle_id}",
            max_length=64,
        )
        thresholds = tier_policy["thresholds"]
        check = {
            "id": check_id,
            "name": template["name"],
            "kind": template["kind"],
            "required": required,
            "timeout_minutes": min(
                int(template["timeout_minutes"]),
                int(tier_policy["max_runtime_minutes"]),
            ),
            "pass_condition": self._pass_condition(template["kind"], thresholds, required, check_name),
        }
        if not required:
            check["waivable"] = True
        baseline_ref = self._baseline_ref(ticket_bundle_id, tier_name, template["kind"], check_name)
        if baseline_ref is not None:
            check["baseline_ref"] = baseline_ref
        return check

    @staticmethod
    def _baseline_ref(
        ticket_bundle_id: str,
        tier_name: str,
        kind: str,
        check_name: str,
    ) -> str | None:
        if kind not in {"latency", "cost", "llm_quality"}:
            return None
        return build_identifier(
            "baseline",
            f"{tier_name}-{check_name}-{ticket_bundle_id}",
            max_length=72,
        )

    @staticmethod
    def _pass_condition(
        kind: str,
        thresholds: dict[str, Any],
        required: bool,
        check_name: str,
    ) -> str:
        if kind == "lint":
            return "lint exits 0"
        if kind == "typecheck":
            return "type checks exit 0"
        if kind == "unit":
            return "ticket-scoped unit tests pass"
        if kind == "contract":
            return "contract validation exits 0"
        if kind == "integration":
            if check_name == "canary_error_rate":
                return (
                    "canary traffic shows 0 critical failures and stays within the configured error budget"
                )
            return "ticket-scoped integration checks pass"
        if kind == "migration_safety":
            return "rollback and migration safety checks pass"
        if kind == "adversarial":
            return "adversarial failure count remains at 0"
        if kind == "latency":
            return (
                f"latency regression remains under {thresholds['latency_regression_pct']} percent"
            )
        if kind == "cost":
            return f"cost regression remains under {thresholds['cost_regression_pct']} percent"
        quality_limit = thresholds["llm_quality_score_delta_max"]
        if required:
            return f"quality score delta remains at or below {quality_limit}"
        return f"quality score delta remains at or below {quality_limit} when the optional suite is run"


class Stage2TicketingPipeline:
    """Run Ticket Architect -> Eval Engineer -> controller transition to TICKETED."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        controller: FactoryController | None = None,
        ticket_architect: TicketArchitect | None = None,
        eval_engineer: EvalEngineer | None = None,
        agent_connector: AgentConnector | None = None,
    ) -> None:
        self.root = repo_root(root)
        self.controller = controller or FactoryController()
        self.ticket_architect = ticket_architect or TicketArchitect(
            self.root,
            agent_connector=agent_connector,
        )
        self.eval_engineer = eval_engineer or EvalEngineer(self.root)
        self.validators = load_validators(self.root)

    def process(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        work_item: WorkItem,
        *,
        ticket_bundle_id: str | None = None,
        eval_manifest_id: str | None = None,
    ) -> Stage2TicketingResult:
        self._validate_document("spec-packet", spec_packet)
        self._validate_document("policy-decision", policy_decision)
        self._validate_document("work-item", work_item.to_document())
        self._validate_consistency(spec_packet, policy_decision, work_item)

        if policy_decision["decision"] != "active_build_candidate":
            raise TicketingEligibilityError(
                f"Only active_build_candidate items can advance to ticketing; got {policy_decision['decision']}."
            )
        if work_item.state is not ControllerState.POLICY_ASSIGNED:
            raise TicketingEligibilityError(
                f"Work item must be in POLICY_ASSIGNED before ticketing; got {work_item.state.value}."
            )

        timestamp = utc_now()
        ticket_artifact_id = ticket_bundle_id or build_identifier(
            "ticket-bundle", spec_packet["artifact"]["id"], max_length=64
        )
        eval_artifact_id = eval_manifest_id or build_identifier(
            "eval", spec_packet["artifact"]["id"], max_length=64
        )

        working_item = deepcopy(work_item)
        self.controller.apply_event(
            working_item,
            event=ControllerEvent.TICKET_GENERATION_STARTED,
            occurred_at=timestamp,
        )

        ticket_bundle = self.ticket_architect.build_ticket_bundle(
            spec_packet,
            policy_decision,
            artifact_id=ticket_artifact_id,
            eval_manifest_id=eval_artifact_id,
            timestamp=timestamp,
        )
        eval_manifest = self.eval_engineer.build_eval_manifest(
            ticket_bundle,
            policy_decision,
            artifact_id=eval_artifact_id,
            timestamp=timestamp,
        )

        self._validate_document("ticket-bundle", ticket_bundle)
        self._validate_document("eval-manifest", eval_manifest)

        self.controller.apply_event(
            working_item,
            event=ControllerEvent.TICKET_BUNDLE_VALID,
            artifact_id=ticket_bundle["artifact"]["id"],
            occurred_at=ticket_bundle["artifact"]["updated_at"],
        )
        self._validate_document("work-item", working_item.to_document())

        return Stage2TicketingResult(
            spec_packet=spec_packet,
            policy_decision=policy_decision,
            ticket_bundle=ticket_bundle,
            eval_manifest=eval_manifest,
            work_item=working_item,
        )

    def _validate_document(self, schema_name: str, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.validators[schema_name], document)
        if errors:
            raise TicketingError(f"{schema_name} failed validation: {'; '.join(errors)}")

    @staticmethod
    def _validate_consistency(
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        work_item: WorkItem,
    ) -> None:
        spec_artifact_id = spec_packet["artifact"]["id"]
        policy_artifact_id = policy_decision["artifact"]["id"]
        source = spec_packet["source"]

        if policy_decision["spec_packet_id"] != spec_artifact_id:
            raise TicketingConsistencyError(
                "policy-decision spec_packet_id does not match the supplied spec-packet artifact id"
            )
        if spec_packet["relevance"]["decision"] != policy_decision["decision"]:
            raise TicketingConsistencyError(
                "spec-packet relevance decision does not match the supplied policy decision"
            )
        if spec_packet["risk_profile"]["risk_score"] != policy_decision["risk_score"]:
            raise TicketingConsistencyError(
                "spec-packet risk score does not match the supplied policy decision"
            )
        if work_item.source_provider != source["provider"]:
            raise TicketingConsistencyError(
                "work-item source_provider does not match the supplied spec-packet source"
            )
        if work_item.source_external_id != source["external_id"]:
            raise TicketingConsistencyError(
                "work-item source_external_id does not match the supplied spec-packet source"
            )
        if work_item.policy_decision_id != policy_artifact_id:
            raise TicketingConsistencyError(
                "work-item policy_decision_id does not match the supplied policy decision artifact id"
            )
        lane = policy_decision.get("lane_assignment", {}).get("lane")
        if lane is not None and work_item.execution_lane != lane:
            raise TicketingConsistencyError(
                "work-item execution_lane does not match the supplied policy decision lane"
            )
        if work_item.state is ControllerState.POLICY_ASSIGNED and work_item.current_artifact_id != policy_artifact_id:
            raise TicketingConsistencyError(
                "work-item current_artifact_id does not point at the supplied policy decision artifact"
            )
