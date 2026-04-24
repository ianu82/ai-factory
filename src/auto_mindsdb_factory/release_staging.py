from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contracts import load_validators, validation_errors_for
from .controller import ControllerEvent, ControllerState, FactoryController, WorkItem
from .intake import build_identifier, repo_root, utc_now


class ReleaseStagingError(RuntimeError):
    """Base class for Stage 7 release-staging failures."""


class ReleaseStagingEligibilityError(ReleaseStagingError):
    """Raised when a work item cannot enter Stage 7 release staging."""


class ReleaseStagingConsistencyError(ReleaseStagingError):
    """Raised when Stage 6 artifacts disagree about the rollout candidate."""


@dataclass(slots=True)
class Stage7ReleaseResult:
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
    merge_decision: dict[str, Any] | None
    promotion_decision: dict[str, Any]
    work_item: WorkItem

    def to_document(self) -> dict[str, Any]:
        document = {
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
            "promotion_decision": self.promotion_decision,
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
        if self.merge_decision is not None:
            document["merge_decision"] = self.merge_decision
        return document


class ReleaseManager:
    """Coordinate staging soak and production promotion."""

    PROMPT_CONTRACT_ID = "release_manager.v1"
    _TRAFFIC_MODE_BY_STRATEGY = {
        "short_soak_or_shadow": "shadow",
        "canary_then_soak": "canary",
        "staged_rollout_with_signoff": "staged_rollout",
    }
    _ENVIRONMENT_BY_TRAFFIC_MODE = {
        "shadow": "shadow",
        "canary": "canary",
        "staged_rollout": "staging",
    }
    _ERROR_RATE_LIMIT_BY_LANE = {
        "fast": 1.0,
        "guarded": 0.75,
        "restricted": 0.5,
    }
    _FALLBACK_RATE_LIMIT_BY_LANE = {
        "fast": 5.0,
        "guarded": 3.0,
        "restricted": 2.0,
    }
    _BUSINESS_KPI_FLOOR_BY_LANE = {
        "fast": 0.9,
        "guarded": 0.93,
        "restricted": 0.95,
    }

    def build_promotion_decision(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        pr_packet: dict[str, Any],
        eval_report: dict[str, Any],
        security_review: dict[str, Any],
        latency_baseline: dict[str, Any],
        *,
        artifact_id: str,
        build_attempt: int,
        approved_release_reviewers: list[str] | None = None,
        observed_soak_minutes: int | None = None,
        observed_request_samples: int | None = None,
        metric_overrides: dict[str, float | int] | None = None,
        rollback_tested: bool = True,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        created_at = timestamp or utc_now()
        policy_artifact = policy_decision["artifact"]
        deployment_policy = policy_decision["deployment_policy"]
        lane = policy_decision["lane_assignment"]["lane"]
        traffic_mode = self._TRAFFIC_MODE_BY_STRATEGY[deployment_policy["strategy"]]
        environment = self._ENVIRONMENT_BY_TRAFFIC_MODE[traffic_mode]
        soak_minutes = (
            observed_soak_minutes
            if observed_soak_minutes is not None
            else max(
                deployment_policy["minimum_soak_minutes"],
                60,
            )
        )
        request_samples = (
            observed_request_samples
            if observed_request_samples is not None
            else max(
                deployment_policy["minimum_request_samples"],
                latency_baseline["sampling_plan"]["sample_size"],
            )
        )
        metrics = self._metrics(
            lane,
            latency_baseline,
            metric_overrides=metric_overrides,
        )
        threshold_breaches = self._threshold_breaches(
            lane,
            latency_baseline,
            deployment_policy,
            soak_minutes=soak_minutes,
            request_samples=request_samples,
            metrics=metrics,
            rollback_tested=rollback_tested,
        )
        passed_checks = self._passed_checks(
            lane,
            latency_baseline,
            deployment_policy,
            soak_minutes=soak_minutes,
            request_samples=request_samples,
            metrics=metrics,
            rollback_tested=rollback_tested,
        )
        approvals_granted = self._approvals_granted(
            policy_decision,
            approved_release_reviewers or [],
        )
        pending_approvals = self._pending_release_approvals(policy_decision, approvals_granted)
        watch_items = self._watch_items(
            deployment_policy,
            traffic_mode,
            pending_approvals,
        )
        decision = self._promotion_decision(
            policy_decision,
            threshold_breaches=threshold_breaches,
            approvals_granted=approvals_granted,
            pending_approvals=pending_approvals,
        )
        evaluated_pr_artifact_version = int(pr_packet["artifact"]["version"])
        resulting_pr_artifact_version = evaluated_pr_artifact_version + 1

        return {
            "artifact": {
                "id": artifact_id,
                "version": 1,
                "source_stage": "release_staging",
                "next_stage": self._next_stage(decision["status"]),
                "status": self._artifact_status(decision["status"]),
                "risk_tier": policy_artifact["risk_tier"],
                "execution_lane": policy_artifact["execution_lane"],
                "owner_agent": "Release Manager",
                "policy_decision_id": policy_artifact["id"],
                "model_fingerprint": self.PROMPT_CONTRACT_ID,
                "budget_class": policy_artifact["budget_class"],
                "rollback_class": policy_artifact["rollback_class"],
                "approval_requirements": list(policy_artifact["approval_requirements"]),
                "blocking_issues": list(threshold_breaches),
                "created_at": created_at,
                "updated_at": created_at,
            },
            "spec_packet_id": spec_packet["artifact"]["id"],
            "pr_packet_id": pr_packet["artifact"]["id"],
            "security_review_id": security_review["artifact"]["id"],
            "eval_report_id": eval_report["artifact"]["id"],
            "latency_baseline_id": latency_baseline["artifact"]["id"],
            "build_attempt": build_attempt,
            "evaluated_pr_artifact_version": evaluated_pr_artifact_version,
            "resulting_pr_artifact_version": resulting_pr_artifact_version,
            "release_runbook": {
                "feature_flag": self._feature_flag_name(spec_packet),
                "strategy": deployment_policy["strategy"],
                "traffic_mode": traffic_mode,
                "staged_steps": self._staged_steps(traffic_mode),
                "auto_promote_allowed": bool(deployment_policy["auto_promote_allowed"]),
            },
            "rollback_plan": {
                "tested": rollback_tested,
                "blast_radius": spec_packet["risk_profile"]["blast_radius"],
                "rollback_class": spec_packet["risk_profile"]["rollback_class"],
                "trigger_conditions": self._rollback_triggers(lane),
                "steps": self._rollback_steps(pr_packet["summary"]["rollback_notes"]),
            },
            "staging_report": {
                "environment": environment,
                "traffic_mode": traffic_mode,
                "soak_minutes_observed": soak_minutes,
                "minimum_soak_minutes": deployment_policy["minimum_soak_minutes"],
                "request_samples_observed": request_samples,
                "minimum_request_samples": deployment_policy["minimum_request_samples"],
                "feature_flag_enabled": bool(deployment_policy["feature_flag_required"]),
                "rollback_tested": rollback_tested,
                "metrics": metrics,
                "threshold_breaches": threshold_breaches,
            },
            "promotion_decision": decision,
            "summary": {
                "threshold_breaches": threshold_breaches,
                "watch_items": watch_items,
                "passed_checks": passed_checks,
            },
        }

    def finalize_pr_packet(
        self,
        pr_packet: dict[str, Any],
        promotion_decision: dict[str, Any],
        *,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        updated = deepcopy(pr_packet)
        updated_at = timestamp or utc_now()
        decision = promotion_decision["promotion_decision"]
        summary = promotion_decision["summary"]
        blocking_findings = summary["threshold_breaches"]
        non_blocking = [
            finding
            for finding in updated["reviewer_report"]["non_blocking_findings"]
            if not finding.startswith("Release manager ")
        ]
        non_blocking.extend(summary["watch_items"])

        updated["artifact"]["version"] = int(updated["artifact"]["version"]) + 1
        updated["artifact"]["owner_agent"] = "Release Manager"
        updated["artifact"]["model_fingerprint"] = self.PROMPT_CONTRACT_ID
        updated["artifact"]["updated_at"] = updated_at

        if decision["status"] == "promoted":
            updated["artifact"]["status"] = "approved"
            updated["artifact"]["next_stage"] = "production_monitoring"
            updated["artifact"]["blocking_issues"] = []
            non_blocking.append("Release manager promoted the feature after staging soak.")
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
            updated["artifact"]["next_stage"] = "human_release_signoff"
            updated["artifact"]["blocking_issues"] = []
            non_blocking.append(
                "Release manager completed the staging soak and is waiting on release approval."
            )
            updated["reviewer_report"] = {
                "approved": True,
                "blocking_findings": [],
                "non_blocking_findings": self._dedupe(non_blocking),
            }
            updated["merge_readiness"] = {
                "reviewable": True,
                "mergeable": False,
                "blockers": ["Release approval is still pending."],
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

    @staticmethod
    def _feature_flag_name(spec_packet: dict[str, Any]) -> str:
        seed = spec_packet["source"]["external_id"].replace("anthropic-", "")
        return f"anthropic_{seed.replace('-', '_')}"

    @staticmethod
    def _staged_steps(traffic_mode: str) -> list[str]:
        if traffic_mode == "shadow":
            return [
                "Deploy behind a feature flag in staging.",
                "Mirror representative traffic without user-visible effects.",
                "Promote after soak thresholds hold.",
            ]
        if traffic_mode == "canary":
            return [
                "Deploy behind a feature flag in staging.",
                "Route a bounded canary slice through the new path.",
                "Promote after soak thresholds and sample size both hold.",
            ]
        return [
            "Deploy behind a feature flag in staging.",
            "Exercise staged rollout checkpoints with explicit approvals.",
            "Promote only after all checkpoints pass.",
        ]

    @staticmethod
    def _rollback_triggers(lane: str) -> list[str]:
        triggers = [
            "Error rate exceeds the configured budget.",
            "Latency regresses past the allowed baseline delta.",
            "Cost per call regresses past the allowed baseline delta.",
        ]
        if lane != "fast":
            triggers.append("Quality score or fallback frequency regresses unexpectedly.")
        return triggers

    @staticmethod
    def _rollback_steps(rollback_notes: str) -> list[str]:
        return [
            "Disable the rollout feature flag immediately.",
            rollback_notes,
            "Verify the old path serves traffic before resuming promotion.",
        ]

    def _metrics(
        self,
        lane: str,
        latency_baseline: dict[str, Any],
        *,
        metric_overrides: dict[str, float | int] | None = None,
    ) -> dict[str, float | int]:
        baseline = latency_baseline["baseline"]
        thresholds = latency_baseline["thresholds"]
        quality_floor = baseline["expected_quality_score"] - thresholds["max_quality_score_delta"]
        metrics: dict[str, float | int] = {
            "error_rate_pct": 0.2 if lane == "fast" else 0.3 if lane == "guarded" else 0.35,
            "p95_latency_ms": int(round(baseline["expected_p95_latency_ms"] * 0.94)),
            "cost_per_call_usd": round(baseline["expected_cost_per_call_usd"] * 0.96, 4),
            "quality_score": round(max(quality_floor + 0.03, baseline["expected_quality_score"]), 2),
            "fallback_rate_pct": 0.4 if lane == "fast" else 0.8 if lane == "guarded" else 1.0,
            "business_kpi_proxy": 0.97 if lane == "fast" else 0.96 if lane == "guarded" else 0.95,
        }
        if metric_overrides:
            metrics.update(metric_overrides)
        return metrics

    def _threshold_breaches(
        self,
        lane: str,
        latency_baseline: dict[str, Any],
        deployment_policy: dict[str, Any],
        *,
        soak_minutes: int,
        request_samples: int,
        metrics: dict[str, float | int],
        rollback_tested: bool,
    ) -> list[str]:
        threshold_breaches: list[str] = []
        baseline = latency_baseline["baseline"]
        thresholds = latency_baseline["thresholds"]
        max_latency = baseline["expected_p95_latency_ms"] * (
            1 + (thresholds["max_latency_regression_pct"] / 100)
        )
        max_cost = baseline["expected_cost_per_call_usd"] * (
            1 + (thresholds["max_cost_regression_pct"] / 100)
        )
        min_quality = baseline["expected_quality_score"] - thresholds["max_quality_score_delta"]
        if soak_minutes < deployment_policy["minimum_soak_minutes"]:
            threshold_breaches.append("Minimum soak window was not met in staging.")
        if request_samples < deployment_policy["minimum_request_samples"]:
            threshold_breaches.append("Minimum request sample threshold was not met in staging.")
        if float(metrics["error_rate_pct"]) > self._ERROR_RATE_LIMIT_BY_LANE[lane]:
            threshold_breaches.append("Error rate exceeded the rollout budget during staging soak.")
        if int(metrics["p95_latency_ms"]) > max_latency:
            threshold_breaches.append("Latency exceeded the allowed regression threshold during staging soak.")
        if float(metrics["cost_per_call_usd"]) > max_cost:
            threshold_breaches.append("Cost per call exceeded the allowed regression threshold during staging soak.")
        if float(metrics["quality_score"]) < min_quality:
            threshold_breaches.append("Quality score fell below the minimum allowed floor during staging soak.")
        if float(metrics["fallback_rate_pct"]) > self._FALLBACK_RATE_LIMIT_BY_LANE[lane]:
            threshold_breaches.append("Fallback rate exceeded the rollout budget during staging soak.")
        if float(metrics["business_kpi_proxy"]) < self._BUSINESS_KPI_FLOOR_BY_LANE[lane]:
            threshold_breaches.append("Business KPI proxy regressed below the minimum rollout floor.")
        if not rollback_tested:
            threshold_breaches.append("Rollback path was not tested before promotion.")
        return threshold_breaches

    def _passed_checks(
        self,
        lane: str,
        latency_baseline: dict[str, Any],
        deployment_policy: dict[str, Any],
        *,
        soak_minutes: int,
        request_samples: int,
        metrics: dict[str, float | int],
        rollback_tested: bool,
    ) -> list[str]:
        baseline = latency_baseline["baseline"]
        thresholds = latency_baseline["thresholds"]
        max_latency = baseline["expected_p95_latency_ms"] * (
            1 + (thresholds["max_latency_regression_pct"] / 100)
        )
        max_cost = baseline["expected_cost_per_call_usd"] * (
            1 + (thresholds["max_cost_regression_pct"] / 100)
        )
        min_quality = baseline["expected_quality_score"] - thresholds["max_quality_score_delta"]
        checks: list[tuple[bool, str]] = [
            (soak_minutes >= deployment_policy["minimum_soak_minutes"], "Minimum soak duration met."),
            (request_samples >= deployment_policy["minimum_request_samples"], "Minimum request sample threshold met."),
            (float(metrics["error_rate_pct"]) <= self._ERROR_RATE_LIMIT_BY_LANE[lane], "Error rate stayed within the rollout budget."),
            (int(metrics["p95_latency_ms"]) <= max_latency, "Latency stayed within the allowed regression threshold."),
            (float(metrics["cost_per_call_usd"]) <= max_cost, "Cost stayed within the allowed regression threshold."),
            (float(metrics["quality_score"]) >= min_quality, "Quality score stayed above the minimum rollout floor."),
            (float(metrics["fallback_rate_pct"]) <= self._FALLBACK_RATE_LIMIT_BY_LANE[lane], "Fallback rate stayed within the rollout budget."),
            (float(metrics["business_kpi_proxy"]) >= self._BUSINESS_KPI_FLOOR_BY_LANE[lane], "Business KPI proxy stayed above the minimum rollout floor."),
            (rollback_tested, "Rollback path was tested before promotion."),
        ]
        return [label for passed, label in checks if passed]

    @staticmethod
    def _approvals_granted(
        policy_decision: dict[str, Any],
        approved_release_reviewers: list[str],
    ) -> list[str]:
        if "release" not in policy_decision["required_approvals"]:
            return []
        return ["release"] if approved_release_reviewers else []

    @staticmethod
    def _pending_release_approvals(
        policy_decision: dict[str, Any],
        approvals_granted: list[str],
    ) -> list[str]:
        required = {"release"} if "release" in policy_decision["required_approvals"] else set()
        return sorted(required - set(approvals_granted))

    @staticmethod
    def _watch_items(
        deployment_policy: dict[str, Any],
        traffic_mode: str,
        pending_approvals: list[str],
    ) -> list[str]:
        watch_items = [
            f"Traffic mode in effect: {traffic_mode}.",
            "Production dashboards should stay pinned during the initial rollout window.",
        ]
        if pending_approvals:
            watch_items.append("Release approval is still pending after a successful staging soak.")
        if not deployment_policy["auto_promote_allowed"]:
            watch_items.append("Lane policy disables autonomous production promotion.")
        return watch_items

    @staticmethod
    def _promotion_decision(
        policy_decision: dict[str, Any],
        *,
        threshold_breaches: list[str],
        approvals_granted: list[str],
        pending_approvals: list[str],
    ) -> dict[str, Any]:
        if threshold_breaches:
            return {
                "status": "blocked",
                "mode": "auto",
                "approvals_granted": approvals_granted,
                "pending_approvals": pending_approvals,
                "rationale": "Staging soak detected threshold breaches, so promotion is blocked.",
            }
        if pending_approvals:
            return {
                "status": "pending_human",
                "mode": "human_required",
                "approvals_granted": approvals_granted,
                "pending_approvals": pending_approvals,
                "rationale": "Staging soak passed, but lane policy still requires explicit release approval.",
            }
        return {
            "status": "promoted",
            "mode": "human_approved" if approvals_granted else "auto",
            "approvals_granted": approvals_granted,
            "pending_approvals": [],
            "rationale": (
                "Staging soak thresholds passed and the required release approval was granted."
                if approvals_granted
                else "Staging soak thresholds passed and autonomous promotion is allowed for this lane."
            ),
        }

    @staticmethod
    def _artifact_status(decision_status: str) -> str:
        if decision_status == "promoted":
            return "approved"
        if decision_status == "pending_human":
            return "ready"
        return "blocked"

    @staticmethod
    def _next_stage(decision_status: str) -> str:
        if decision_status == "promoted":
            return "production_monitoring"
        if decision_status == "pending_human":
            return "human_release_signoff"
        return "build"

    @staticmethod
    def _dedupe(items: list[str]) -> list[str]:
        deduped: list[str] = []
        for item in items:
            if item not in deduped:
                deduped.append(item)
        return deduped


class Stage7ReleaseStagingPipeline:
    """Run the staging soak and promotion decision flow."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        controller: FactoryController | None = None,
        release_manager: ReleaseManager | None = None,
    ) -> None:
        self.root = repo_root(root)
        self.controller = controller or FactoryController()
        self.release_manager = release_manager or ReleaseManager()
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
        merge_decision: dict[str, Any] | None = None,
        promotion_decision_id: str | None = None,
        approved_release_reviewers: list[str] | None = None,
        observed_soak_minutes: int | None = None,
        observed_request_samples: int | None = None,
        metric_overrides: dict[str, float | int] | None = None,
        rollback_tested: bool = True,
    ) -> Stage7ReleaseResult:
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
        if merge_decision is not None:
            self._validate_document("merge-decision", merge_decision)
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
            merge_decision,
            work_item,
        )

        timestamp = utc_now()
        evaluated_pr_artifact_version = int(pr_packet["artifact"]["version"])
        artifact_id = promotion_decision_id or self._default_promotion_decision_id(
            spec_packet["artifact"]["id"],
            work_item.attempt_count,
            evaluated_pr_artifact_version,
        )
        promotion_decision = self.release_manager.build_promotion_decision(
            spec_packet,
            policy_decision,
            pr_packet,
            eval_report,
            security_review,
            latency_baseline,
            artifact_id=artifact_id,
            build_attempt=work_item.attempt_count,
            approved_release_reviewers=approved_release_reviewers,
            observed_soak_minutes=observed_soak_minutes,
            observed_request_samples=observed_request_samples,
            metric_overrides=metric_overrides,
            rollback_tested=rollback_tested,
            timestamp=timestamp,
        )
        updated_pr_packet = self.release_manager.finalize_pr_packet(
            pr_packet,
            promotion_decision,
            timestamp=timestamp,
        )

        self._validate_document("promotion-decision", promotion_decision)
        self._validate_document("pr-packet", updated_pr_packet)
        self._validate_generated_consistency(
            updated_pr_packet,
            security_review,
            merge_decision,
            eval_report,
            promotion_decision,
            work_item,
        )

        working_item = deepcopy(work_item)
        if work_item.state in {
            ControllerState.SECURITY_APPROVED,
            ControllerState.MERGED,
        }:
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.STAGING_SOAK_STARTED,
                artifact_id=promotion_decision["artifact"]["id"],
                occurred_at=promotion_decision["artifact"]["created_at"],
            )
        decision_status = promotion_decision["promotion_decision"]["status"]
        if decision_status == "blocked":
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.STAGING_SOAK_FAILED,
                artifact_id=promotion_decision["artifact"]["id"],
                occurred_at=promotion_decision["artifact"]["updated_at"],
            )
        elif decision_status == "promoted":
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.PRODUCTION_PROMOTED,
                artifact_id=promotion_decision["artifact"]["id"],
                occurred_at=promotion_decision["artifact"]["updated_at"],
            )
        elif decision_status == "pending_human":
            working_item.current_artifact_id = promotion_decision["artifact"]["id"]
            working_item.updated_at = promotion_decision["artifact"]["updated_at"]
        self._validate_document("work-item", working_item.to_document())

        return Stage7ReleaseResult(
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
            promotion_decision=promotion_decision,
            work_item=working_item,
        )

    def _validate_document(self, schema_name: str, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.validators[schema_name], document)
        if errors:
            raise ReleaseStagingError(
                f"{schema_name} failed validation: {'; '.join(errors)}"
            )

    @staticmethod
    def _default_promotion_decision_id(
        spec_packet_id: str,
        build_attempt: int,
        evaluated_pr_artifact_version: int,
    ) -> str:
        return build_identifier(
            "promotion-decision",
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
        merge_decision: dict[str, Any] | None,
        work_item: WorkItem,
    ) -> None:
        spec_packet_id = spec_packet["artifact"]["id"]
        policy_artifact = policy_decision["artifact"]
        lane = policy_decision["lane_assignment"]["lane"]

        if policy_decision["decision"] != "active_build_candidate":
            raise ReleaseStagingEligibilityError(
                "Only active_build_candidate items can enter Stage 7 release staging."
            )
        if work_item.state not in {
            ControllerState.SECURITY_APPROVED,
            ControllerState.MERGED,
            ControllerState.STAGING_SOAK,
        }:
            raise ReleaseStagingEligibilityError(
                "Work item must be in SECURITY_APPROVED, MERGED, or STAGING_SOAK before Stage 7; "
                f"got {work_item.state.value}."
            )
        if work_item.attempt_count < 1:
            raise ReleaseStagingEligibilityError(
                "Work item must have at least one build attempt before Stage 7 runs."
            )
        if security_review["signoff"]["status"] != "approved":
            raise ReleaseStagingEligibilityError(
                "Stage 7 only runs after security review is approved."
            )
        if work_item.state is ControllerState.SECURITY_APPROVED and work_item.current_artifact_id != security_review["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "work-item current_artifact_id must match the security-review when Stage 7 starts."
            )
        if (
            work_item.state is ControllerState.SECURITY_APPROVED
            and "merge" in policy_decision["required_approvals"]
        ):
            raise ReleaseStagingEligibilityError(
                "Stage 7 cannot start from SECURITY_APPROVED when lane policy still "
                "requires merge approval; run merge orchestration first."
            )
        if work_item.state is ControllerState.MERGED:
            if merge_decision is None:
                raise ReleaseStagingConsistencyError(
                    "Stage 7 requires merge-decision input when starting from MERGED."
                )
            if work_item.current_artifact_id != merge_decision["artifact"]["id"]:
                raise ReleaseStagingConsistencyError(
                    "work-item current_artifact_id must match the merge-decision when Stage 7 starts from MERGED."
                )
        if work_item.state is ControllerState.STAGING_SOAK and not work_item.current_artifact_id:
            raise ReleaseStagingConsistencyError(
                "work-item in STAGING_SOAK must retain the previous promotion artifact id."
            )
        if work_item.policy_decision_id != policy_artifact["id"]:
            raise ReleaseStagingConsistencyError(
                "work-item policy_decision_id does not match the policy decision artifact."
            )
        if work_item.execution_lane != lane:
            raise ReleaseStagingConsistencyError(
                "work-item execution_lane does not match the policy lane."
            )
        if work_item.risk_score != policy_decision["risk_score"]:
            raise ReleaseStagingConsistencyError(
                "work-item risk_score does not match the policy decision."
            )
        if work_item.source_provider != spec_packet["source"]["provider"]:
            raise ReleaseStagingConsistencyError(
                "work-item source_provider does not match the provided spec-packet."
            )
        if work_item.source_external_id != spec_packet["source"]["external_id"]:
            raise ReleaseStagingConsistencyError(
                "work-item source_external_id does not match the provided spec-packet."
            )
        if policy_decision["spec_packet_id"] != spec_packet_id:
            raise ReleaseStagingConsistencyError(
                "policy-decision does not reference the provided spec-packet."
            )
        if ticket_bundle["spec_packet_id"] != spec_packet_id:
            raise ReleaseStagingConsistencyError(
                "ticket-bundle does not reference the provided spec-packet."
            )
        if ticket_bundle["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "ticket-bundle eval_manifest_id does not match the provided eval-manifest."
            )
        if eval_manifest["target_id"] != ticket_bundle["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "eval-manifest target_id does not match the provided ticket-bundle."
            )
        if pr_packet["spec_packet_id"] != spec_packet_id:
            raise ReleaseStagingConsistencyError(
                "pr-packet spec_packet_id does not match the provided spec-packet."
            )
        if pr_packet["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "pr-packet eval_manifest_id does not match the provided eval-manifest."
            )
        if prompt_contract["spec_packet_id"] != spec_packet_id:
            raise ReleaseStagingConsistencyError(
                "prompt-contract spec_packet_id does not match the provided spec-packet."
            )
        if prompt_contract["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "prompt-contract pr_packet_id does not match the provided pr-packet."
            )
        if tool_schema["spec_packet_id"] != spec_packet_id:
            raise ReleaseStagingConsistencyError(
                "tool-schema spec_packet_id does not match the provided spec-packet."
            )
        if tool_schema["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "tool-schema prompt_contract_id does not match the provided prompt contract."
            )
        if golden_dataset["spec_packet_id"] != spec_packet_id:
            raise ReleaseStagingConsistencyError(
                "golden-dataset spec_packet_id does not match the provided spec-packet."
            )
        if golden_dataset["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "golden-dataset prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["spec_packet_id"] != spec_packet_id:
            raise ReleaseStagingConsistencyError(
                "latency-baseline spec_packet_id does not match the provided spec-packet."
            )
        if latency_baseline["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "latency-baseline prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "latency-baseline pr_packet_id does not match the provided pr-packet."
            )
        if eval_report["spec_packet_id"] != spec_packet_id:
            raise ReleaseStagingConsistencyError(
                "eval-report spec_packet_id does not match the provided spec-packet."
            )
        if eval_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "eval-report pr_packet_id does not match the provided pr-packet."
            )
        if security_review["spec_packet_id"] != spec_packet_id:
            raise ReleaseStagingConsistencyError(
                "security-review spec_packet_id does not match the provided spec-packet."
            )
        if security_review["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "security-review pr_packet_id does not match the provided pr-packet."
            )
        if security_review["eval_report_id"] != eval_report["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "security-review eval_report_id does not match the provided eval-report."
            )
        if merge_decision is not None:
            if merge_decision["spec_packet_id"] != spec_packet_id:
                raise ReleaseStagingConsistencyError(
                    "merge-decision spec_packet_id does not match the provided spec-packet."
                )
            if merge_decision["pr_packet_id"] != pr_packet["artifact"]["id"]:
                raise ReleaseStagingConsistencyError(
                    "merge-decision pr_packet_id does not match the provided pr-packet."
                )
            if merge_decision["security_review_id"] != security_review["artifact"]["id"]:
                raise ReleaseStagingConsistencyError(
                    "merge-decision security_review_id does not match the provided security-review."
                )
        if work_item.attempt_count != security_review["build_attempt"]:
            raise ReleaseStagingConsistencyError(
                "work-item attempt_count does not match the security-review build attempt."
            )
        if int(pr_packet["artifact"]["version"]) < security_review["resulting_pr_artifact_version"]:
            raise ReleaseStagingConsistencyError(
                "pr-packet version cannot be earlier than the security-review resulting version."
            )
        if work_item.state is ControllerState.SECURITY_APPROVED:
            if int(pr_packet["artifact"]["version"]) != security_review["resulting_pr_artifact_version"]:
                raise ReleaseStagingConsistencyError(
                    "pr-packet version must match the security-review resulting version when Stage 7 starts."
                )
            if not pr_packet["merge_readiness"]["mergeable"]:
                raise ReleaseStagingEligibilityError(
                    "pr-packet must be mergeable before Stage 7 starts from SECURITY_APPROVED."
                )
        if work_item.state is ControllerState.MERGED:
            if merge_decision is None:
                raise ReleaseStagingConsistencyError(
                    "MERGED work items require merge-decision input before Stage 7."
                )
            if merge_decision["merge_decision"]["status"] != "merged":
                raise ReleaseStagingEligibilityError(
                    "Stage 7 only runs after merge orchestration reports a merged decision."
                )
            if int(pr_packet["artifact"]["version"]) != merge_decision["resulting_pr_artifact_version"]:
                raise ReleaseStagingConsistencyError(
                    "pr-packet version must match the merge-decision resulting version when Stage 7 starts from MERGED."
                )
            if pr_packet.get("merge_execution", {}).get("status") != "merged":
                raise ReleaseStagingEligibilityError(
                    "Stage 7 requires pr-packet merge_execution.status=merged after merge orchestration."
                )
        if work_item.state is ControllerState.STAGING_SOAK and not pr_packet["reviewer_report"]["approved"]:
            raise ReleaseStagingEligibilityError(
                "pr-packet must remain approved while Stage 7 is waiting on release promotion."
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
                raise ReleaseStagingConsistencyError(
                    f"{artifact_name} policy_decision_id does not match the policy decision artifact."
                )
            if artifact["execution_lane"] != lane:
                raise ReleaseStagingConsistencyError(
                    f"{artifact_name} execution_lane does not match the policy lane."
                )
            if artifact["risk_tier"] != policy_artifact["risk_tier"]:
                raise ReleaseStagingConsistencyError(
                    f"{artifact_name} risk_tier does not match the policy decision artifact."
                )
            if artifact["budget_class"] != policy_artifact["budget_class"]:
                raise ReleaseStagingConsistencyError(
                    f"{artifact_name} budget_class does not match the policy decision artifact."
                )
            if artifact["rollback_class"] != policy_artifact["rollback_class"]:
                raise ReleaseStagingConsistencyError(
                    f"{artifact_name} rollback_class does not match the policy decision artifact."
                )
            if sorted(artifact["approval_requirements"]) != sorted(
                policy_artifact["approval_requirements"]
            ):
                raise ReleaseStagingConsistencyError(
                    f"{artifact_name} approval_requirements do not match the policy decision artifact."
                )
        if merge_decision is not None:
            artifact = merge_decision["artifact"]
            if artifact["policy_decision_id"] != policy_artifact["id"]:
                raise ReleaseStagingConsistencyError(
                    "merge-decision policy_decision_id does not match the policy decision artifact."
                )
            if artifact["execution_lane"] != lane:
                raise ReleaseStagingConsistencyError(
                    "merge-decision execution_lane does not match the policy lane."
                )
            if artifact["risk_tier"] != policy_artifact["risk_tier"]:
                raise ReleaseStagingConsistencyError(
                    "merge-decision risk_tier does not match the policy decision artifact."
                )
            if artifact["budget_class"] != policy_artifact["budget_class"]:
                raise ReleaseStagingConsistencyError(
                    "merge-decision budget_class does not match the policy decision artifact."
                )
            if artifact["rollback_class"] != policy_artifact["rollback_class"]:
                raise ReleaseStagingConsistencyError(
                    "merge-decision rollback_class does not match the policy decision artifact."
                )
            if sorted(artifact["approval_requirements"]) != sorted(
                policy_artifact["approval_requirements"]
            ):
                raise ReleaseStagingConsistencyError(
                    "merge-decision approval_requirements do not match the policy decision artifact."
                )

    @staticmethod
    def _validate_generated_consistency(
        pr_packet: dict[str, Any],
        security_review: dict[str, Any],
        merge_decision: dict[str, Any] | None,
        eval_report: dict[str, Any],
        promotion_decision: dict[str, Any],
        work_item: WorkItem,
    ) -> None:
        if promotion_decision["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "promotion-decision pr_packet_id does not match the updated pr-packet artifact."
            )
        if promotion_decision["security_review_id"] != security_review["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "promotion-decision security_review_id does not match the provided security-review."
            )
        if merge_decision is not None:
            if promotion_decision["evaluated_pr_artifact_version"] < merge_decision["resulting_pr_artifact_version"]:
                raise ReleaseStagingConsistencyError(
                    "promotion-decision evaluated_pr_artifact_version cannot be earlier than the merge-decision result."
                )
        if promotion_decision["eval_report_id"] != eval_report["artifact"]["id"]:
            raise ReleaseStagingConsistencyError(
                "promotion-decision eval_report_id does not match the provided eval-report."
            )
        if promotion_decision["build_attempt"] != work_item.attempt_count:
            raise ReleaseStagingConsistencyError(
                "promotion-decision build_attempt does not match the work-item attempt count."
            )
        if promotion_decision["evaluated_pr_artifact_version"] + 1 != promotion_decision["resulting_pr_artifact_version"]:
            raise ReleaseStagingConsistencyError(
                "promotion-decision resulting_pr_artifact_version must be exactly one greater than the evaluated version."
            )
        if promotion_decision["resulting_pr_artifact_version"] != pr_packet["artifact"]["version"]:
            raise ReleaseStagingConsistencyError(
                "promotion-decision resulting_pr_artifact_version does not match the updated pr-packet version."
            )

        decision_status = promotion_decision["promotion_decision"]["status"]
        if decision_status == "promoted":
            if not pr_packet["merge_readiness"]["mergeable"]:
                raise ReleaseStagingConsistencyError(
                    "promoted release candidate must leave the pr-packet mergeable."
                )
        elif decision_status == "pending_human":
            if pr_packet["merge_readiness"]["mergeable"]:
                raise ReleaseStagingConsistencyError(
                    "pending human release approval cannot leave the pr-packet mergeable."
                )
            if not pr_packet["merge_readiness"]["reviewable"]:
                raise ReleaseStagingConsistencyError(
                    "pending human release approval must keep the pr-packet reviewable."
                )
        else:
            if pr_packet["merge_readiness"]["mergeable"]:
                raise ReleaseStagingConsistencyError(
                    "blocked release promotion cannot leave the pr-packet mergeable."
                )
            if pr_packet["reviewer_report"]["approved"]:
                raise ReleaseStagingConsistencyError(
                    "blocked release promotion cannot leave the reviewer_report approved."
                )
