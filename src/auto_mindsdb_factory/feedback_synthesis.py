from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contracts import load_validators, validation_errors_for
from .controller import ControllerEvent, ControllerState, FactoryController, WorkItem
from .intake import build_identifier, repo_root, utc_now


class FeedbackSynthesisError(RuntimeError):
    """Base class for Stage 9 feedback-synthesis failures."""


class FeedbackSynthesisEligibilityError(FeedbackSynthesisError):
    """Raised when a work item cannot enter Stage 9 feedback synthesis."""


class FeedbackSynthesisConsistencyError(FeedbackSynthesisError):
    """Raised when Stage 8 artifacts disagree about the feedback candidate."""


@dataclass(slots=True)
class Stage9FeedbackResult:
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
    monitoring_report: dict[str, Any]
    feedback_report: dict[str, Any]
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
            "monitoring_report": self.monitoring_report,
            "feedback_report": self.feedback_report,
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


class FeedbackSynthesizer:
    """Roll up production learnings into a reusable feedback artifact."""

    PROMPT_CONTRACT_ID = "feedback_synthesizer.v1"
    OWNER_AGENT = "Feedback Synthesizer"
    _OPEN_INCIDENT_STAGES = {"feedback_synthesis", "human_incident_response"}

    def build_feedback_report(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        ticket_bundle: dict[str, Any],
        eval_manifest: dict[str, Any],
        pr_packet: dict[str, Any],
        latency_baseline: dict[str, Any],
        eval_report: dict[str, Any],
        security_review: dict[str, Any],
        promotion_decision: dict[str, Any],
        monitoring_report: dict[str, Any],
        *,
        artifact_id: str,
        build_attempt: int,
        feedback_window_days: int = 7,
        unexpected_user_behaviors: list[str] | None = None,
        positive_surprises: list[str] | None = None,
        spec_mismatches: list[str] | None = None,
        eval_misses: list[str] | None = None,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        created_at = timestamp or utc_now()
        policy_artifact = policy_decision["artifact"]
        feature_title = self._feature_title(spec_packet)
        open_incident_stage = self._open_incident_stage(pr_packet)
        production_incidents = self._production_incidents(
            pr_packet,
            monitoring_report,
            open_incident_stage=open_incident_stage,
        )
        incident_follow_up = bool(production_incidents)
        feedback_mode = "incident_follow_up" if incident_follow_up else "weekly_rollup"

        derived_spec_mismatches = self._derived_spec_mismatches(
            feature_title,
            production_incidents=production_incidents,
            open_incident_stage=open_incident_stage,
        )
        derived_eval_misses = self._derived_eval_misses(
            eval_report,
            production_incidents=production_incidents,
        )
        derived_positive_surprises = self._derived_positive_surprises(
            latency_baseline,
            monitoring_report,
        )

        signals = {
            "production_incidents": production_incidents,
            "unexpected_user_behaviors": self._dedupe(unexpected_user_behaviors or []),
            "spec_mismatches": self._dedupe((spec_mismatches or []) + derived_spec_mismatches),
            "eval_misses": self._dedupe((eval_misses or []) + derived_eval_misses),
            "positive_surprises": self._dedupe((positive_surprises or []) + derived_positive_surprises),
        }
        incident_learning_packets = self._incident_learning_packets(
            feature_title,
            monitoring_report,
            signals["production_incidents"],
            open_incident_stage=open_incident_stage,
        )
        spec_feedback = self._spec_feedback(
            signals["production_incidents"],
            signals["spec_mismatches"],
            signals["unexpected_user_behaviors"],
            signals["positive_surprises"],
        )
        eval_improvements = self._eval_improvements(
            signals["production_incidents"],
            signals["eval_misses"],
        )
        guardrail_recommendations = self._guardrail_recommendations(
            policy_decision,
            monitoring_report,
            open_incident_stage=open_incident_stage,
        )
        backlog_candidates = self._backlog_candidates(
            feedback_mode=feedback_mode,
            spec_feedback=spec_feedback,
            eval_improvements=eval_improvements,
            guardrail_recommendations=guardrail_recommendations,
        )
        upstream_feedback = self._upstream_feedback(
            feature_title,
            signals["production_incidents"],
            signals["unexpected_user_behaviors"],
            signals["positive_surprises"],
        )
        rendered_outputs = self._rendered_outputs(
            feature_title,
            feedback_mode=feedback_mode,
            signals=signals,
            incident_learning_packets=incident_learning_packets,
            spec_feedback=spec_feedback,
            eval_improvements=eval_improvements,
            guardrail_recommendations=guardrail_recommendations,
            backlog_candidates=backlog_candidates,
            upstream_feedback=upstream_feedback,
        )
        summary = self._summary(
            signals=signals,
            incident_learning_packets=incident_learning_packets,
            eval_improvements=eval_improvements,
            guardrail_recommendations=guardrail_recommendations,
            backlog_candidates=backlog_candidates,
        )

        return {
            "artifact": {
                "id": artifact_id,
                "version": 1,
                "source_stage": "feedback_synthesis",
                "next_stage": "stage1_intake",
                "status": "ready",
                "risk_tier": policy_artifact["risk_tier"],
                "execution_lane": policy_artifact["execution_lane"],
                "owner_agent": self.OWNER_AGENT,
                "policy_decision_id": policy_artifact["id"],
                "model_fingerprint": self.PROMPT_CONTRACT_ID,
                "budget_class": policy_artifact["budget_class"],
                "rollback_class": policy_artifact["rollback_class"],
                "approval_requirements": list(policy_artifact["approval_requirements"]),
                "blocking_issues": list(signals["production_incidents"]),
                "created_at": created_at,
                "updated_at": created_at,
            },
            "spec_packet_id": spec_packet["artifact"]["id"],
            "ticket_bundle_id": ticket_bundle["artifact"]["id"],
            "eval_manifest_id": eval_manifest["artifact"]["id"],
            "pr_packet_id": pr_packet["artifact"]["id"],
            "monitoring_report_id": monitoring_report["artifact"]["id"],
            "promotion_decision_id": promotion_decision["artifact"]["id"],
            "security_review_id": security_review["artifact"]["id"],
            "eval_report_id": eval_report["artifact"]["id"],
            "latency_baseline_id": latency_baseline["artifact"]["id"],
            "build_attempt": build_attempt,
            "analyzed_pr_artifact_version": int(pr_packet["artifact"]["version"]),
            "feedback_window": {
                "mode": feedback_mode,
                "monitored_environment": "production",
                "lookback_days": feedback_window_days,
                "trigger": "incident_signal" if incident_follow_up else "scheduled_rollup",
                "source_monitoring_status": monitoring_report["monitoring_decision"]["status"],
            },
            "signals": signals,
            "incident_learning_packets": incident_learning_packets,
            "spec_feedback": spec_feedback,
            "eval_improvements": eval_improvements,
            "guardrail_recommendations": guardrail_recommendations,
            "backlog_candidates": backlog_candidates,
            "upstream_feedback": upstream_feedback,
            "rendered_outputs": rendered_outputs,
            "summary": summary,
        }

    @classmethod
    def _open_incident_stage(cls, pr_packet: dict[str, Any]) -> str | None:
        artifact = pr_packet["artifact"]
        next_stage = artifact["next_stage"]
        if (
            artifact["owner_agent"] == "SRE Sentinel"
            and artifact["status"] == "blocked"
            and next_stage in cls._OPEN_INCIDENT_STAGES
        ):
            return next_stage
        return None

    def _production_incidents(
        self,
        pr_packet: dict[str, Any],
        monitoring_report: dict[str, Any],
        *,
        open_incident_stage: str | None,
    ) -> list[str]:
        incidents = list(monitoring_report["incident"]["regressions"])
        if incidents:
            return self._dedupe(incidents)
        if open_incident_stage is not None:
            return self._dedupe(list(pr_packet["reviewer_report"]["blocking_findings"]))
        return []

    @staticmethod
    def _feature_title(spec_packet: dict[str, Any]) -> str:
        return spec_packet["source"]["title"]

    @staticmethod
    def _derived_spec_mismatches(
        feature_title: str,
        *,
        production_incidents: list[str],
        open_incident_stage: str | None,
    ) -> list[str]:
        mismatches: list[str] = []
        if production_incidents:
            mismatches.append(
                f"Update acceptance criteria for '{feature_title}' so production guardrails and rollback expectations are explicit."
            )
        if open_incident_stage == "human_incident_response":
            mismatches.append(
                "Clarify when manual incident response is expected so rollout ownership is explicit before launch."
            )
        return mismatches

    @staticmethod
    def _derived_eval_misses(
        eval_report: dict[str, Any],
        *,
        production_incidents: list[str],
    ) -> list[str]:
        if not production_incidents or not eval_report["summary"]["merge_gate_passed"]:
            return []
        return [
            f"Merge-gating eval coverage missed this production regression: {incident}"
            for incident in production_incidents
        ]

    @staticmethod
    def _derived_positive_surprises(
        latency_baseline: dict[str, Any],
        monitoring_report: dict[str, Any],
    ) -> list[str]:
        if monitoring_report["monitoring_decision"]["status"] != "healthy":
            return []
        baseline = latency_baseline["baseline"]
        observed = monitoring_report["observed_metrics"]
        surprises: list[str] = []
        if observed["quality_score"] > round(baseline["expected_quality_score"] + 0.01, 2):
            surprises.append("Production quality score exceeded the planned baseline.")
        if observed["p95_latency_ms"] < int(round(baseline["expected_p95_latency_ms"] * 0.95)):
            surprises.append("Production latency landed below the planned baseline.")
        if observed["cost_per_call_usd"] < round(
            baseline["expected_cost_per_call_usd"] * 0.95,
            4,
        ):
            surprises.append("Production cost per call landed below the planned baseline.")
        return surprises

    def _incident_learning_packets(
        self,
        feature_title: str,
        monitoring_report: dict[str, Any],
        production_incidents: list[str],
        *,
        open_incident_stage: str | None,
    ) -> list[dict[str, Any]]:
        if not production_incidents:
            return []

        severity = monitoring_report["incident"]["severity"]
        if severity == "none":
            severity = "high" if open_incident_stage == "human_incident_response" else "medium"
        summary = monitoring_report["monitoring_decision"]["rationale"]
        if monitoring_report["monitoring_decision"]["status"] == "healthy":
            summary = (
                "A prior production incident remains open even though the latest monitoring window was healthy."
            )
        action_items = self._dedupe(
            list(monitoring_report["summary"]["follow_up_actions"])
            + [
                f"Backfill acceptance and eval coverage for: {incident}"
                for incident in production_incidents
            ]
        )
        return [
            {
                "id": build_identifier(
                    "incident-learning",
                    f"{feature_title}-{severity}",
                    max_length=72,
                ),
                "title": f"Incident learning for {feature_title}",
                "severity": severity,
                "status": (
                    "requires_human_follow_up"
                    if open_incident_stage == "human_incident_response"
                    else "captured"
                ),
                "summary": summary,
                "action_items": action_items,
            }
        ]

    @staticmethod
    def _spec_feedback(
        production_incidents: list[str],
        spec_mismatches: list[str],
        unexpected_user_behaviors: list[str],
        positive_surprises: list[str],
    ) -> dict[str, list[str]]:
        acceptance_updates = [
            f"Add an acceptance criterion that prevents: {incident}"
            for incident in production_incidents
        ]
        if positive_surprises:
            acceptance_updates.append(
                "Capture the unexpectedly strong production behavior as an explicit baseline target."
            )
        return {
            "corrections": spec_mismatches,
            "acceptance_updates": acceptance_updates,
            "open_questions": unexpected_user_behaviors,
        }

    def _eval_improvements(
        self,
        production_incidents: list[str],
        eval_misses: list[str],
    ) -> list[dict[str, Any]]:
        signals = eval_misses or production_incidents
        improvements: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for signal in signals:
            item = self._eval_improvement_item(signal)
            if item["id"] in seen_ids:
                continue
            seen_ids.add(item["id"])
            improvements.append(item)
        return improvements

    @staticmethod
    def _eval_improvement_item(signal: str) -> dict[str, Any]:
        lowered = signal.lower()
        metric = "production-regression"
        title = "Backfill production regression coverage"
        target_tier = "pre_merge"
        check_kind = "integration"
        if "error rate" in lowered:
            metric = "error-rate"
            title = "Backfill error-rate regression coverage"
        elif "latency" in lowered:
            metric = "latency"
            title = "Backfill latency regression coverage"
            check_kind = "latency"
        elif "cost" in lowered:
            metric = "cost"
            title = "Backfill cost regression coverage"
            check_kind = "cost"
            target_tier = "nightly"
        elif "quality" in lowered:
            metric = "quality"
            title = "Backfill output-quality regression coverage"
            check_kind = "llm_quality"
            target_tier = "nightly"
        elif "fallback" in lowered:
            metric = "fallback-rate"
            title = "Backfill fallback-rate regression coverage"
        elif "business kpi" in lowered:
            metric = "business-kpi"
            title = "Backfill business-KPI regression coverage"
            check_kind = "llm_quality"
            target_tier = "post_deploy"
        elif "security anomaly" in lowered:
            metric = "security-anomaly"
            title = "Backfill security anomaly regression coverage"
            check_kind = "adversarial"
        return {
            "id": build_identifier(
                "eval-improvement",
                metric,
                max_length=72,
            ),
            "title": title,
            "summary": signal,
            "target_tier": target_tier,
            "check_kind": check_kind,
        }

    @staticmethod
    def _guardrail_recommendations(
        policy_decision: dict[str, Any],
        monitoring_report: dict[str, Any],
        *,
        open_incident_stage: str | None,
    ) -> list[dict[str, Any]]:
        recommendations: list[dict[str, Any]] = []
        lane = policy_decision["lane_assignment"]["lane"]
        mitigation = monitoring_report["mitigation"]

        if mitigation["result"] == "blocked_by_policy":
            recommendations.append(
                {
                    "id": build_identifier(
                        "guardrail",
                        f"{lane}-rollback-autonomy-review",
                        max_length=72,
                    ),
                    "title": f"Review rollback autonomy for {lane} lane production incidents",
                    "summary": (
                        "Human-only mitigation slowed response even though the rollout remained feature-flagged."
                    ),
                    "review_required": True,
                }
            )
        if mitigation["action"] == "kill_switch":
            recommendations.append(
                {
                    "id": build_identifier(
                        "guardrail",
                        "critical-anomaly-kill-switch-drill",
                        max_length=72,
                    ),
                    "title": "Drill the critical-anomaly kill-switch path",
                    "summary": "Keep the kill-switch path exercised as part of operational readiness.",
                    "review_required": False,
                }
            )
        if open_incident_stage == "feedback_synthesis":
            recommendations.append(
                {
                    "id": build_identifier(
                        "guardrail",
                        "incident-follow-up-linkage",
                        max_length=72,
                    ),
                    "title": "Keep incident follow-up linked to production monitoring",
                    "summary": "Do not clear incident backlog until corrective work has shipped and soaked.",
                    "review_required": False,
                }
            )
        return recommendations

    @staticmethod
    def _backlog_candidates(
        *,
        feedback_mode: str,
        spec_feedback: dict[str, list[str]],
        eval_improvements: list[dict[str, Any]],
        guardrail_recommendations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        if spec_feedback["corrections"] or spec_feedback["acceptance_updates"]:
            candidates.append(
                {
                    "id": build_identifier("backlog", "spec-feedback", max_length=72),
                    "title": "Fold production learnings back into the spec",
                    "category": "spec_correction",
                    "priority": "high" if feedback_mode == "incident_follow_up" else "low",
                    "rationale": (
                        "Production learnings changed either the accepted behavior or the rollout assumptions."
                    ),
                }
            )
        for improvement in eval_improvements:
            candidates.append(
                {
                    "id": build_identifier(
                        "backlog",
                        f"eval-{improvement['id']}",
                        max_length=72,
                    ),
                    "title": improvement["title"],
                    "category": "eval_gap",
                    "priority": "high" if feedback_mode == "incident_follow_up" else "medium",
                    "rationale": improvement["summary"],
                }
            )
        for recommendation in guardrail_recommendations:
            candidates.append(
                {
                    "id": build_identifier(
                        "backlog",
                        f"guardrail-{recommendation['id']}",
                        max_length=72,
                    ),
                    "title": recommendation["title"],
                    "category": "guardrail_change",
                    "priority": "high" if recommendation["review_required"] else "medium",
                    "rationale": recommendation["summary"],
                }
            )
        return candidates

    @staticmethod
    def _upstream_feedback(
        feature_title: str,
        production_incidents: list[str],
        unexpected_user_behaviors: list[str],
        positive_surprises: list[str],
    ) -> dict[str, Any]:
        messages: list[str] = []
        if production_incidents:
            messages.append(
                f"Production follow-up for '{feature_title}': {production_incidents[0]}"
            )
        if unexpected_user_behaviors:
            messages.append(
                f"Unexpected user behavior on '{feature_title}': {unexpected_user_behaviors[0]}"
            )
        if positive_surprises:
            messages.append(
                f"Positive production surprise on '{feature_title}': {positive_surprises[0]}"
            )
        return {
            "should_send": bool(messages),
            "messages": messages,
        }

    @staticmethod
    def _rendered_outputs(
        feature_title: str,
        *,
        feedback_mode: str,
        signals: dict[str, list[str]],
        incident_learning_packets: list[dict[str, Any]],
        spec_feedback: dict[str, list[str]],
        eval_improvements: list[dict[str, Any]],
        guardrail_recommendations: list[dict[str, Any]],
        backlog_candidates: list[dict[str, Any]],
        upstream_feedback: dict[str, Any],
    ) -> dict[str, Any]:
        weekly_lines = [
            f"# Weekly Factory Report: {feature_title}",
            "",
            f"- Mode: {feedback_mode}",
            f"- Production incidents: {len(signals['production_incidents'])}",
            f"- Unexpected user behaviors: {len(signals['unexpected_user_behaviors'])}",
            f"- Eval improvements: {len(eval_improvements)}",
            f"- Backlog candidates: {len(backlog_candidates)}",
        ]
        if signals["positive_surprises"]:
            weekly_lines.extend(
                [
                    "",
                    "## Positive Surprises",
                    *[f"- {item}" for item in signals["positive_surprises"]],
                ]
            )
        if signals["production_incidents"]:
            weekly_lines.extend(
                [
                    "",
                    "## Incident Signals",
                    *[f"- {item}" for item in signals["production_incidents"]],
                ]
            )
        if backlog_candidates:
            weekly_lines.extend(
                [
                    "",
                    "## Backlog Candidates",
                    *[f"- {item['title']}: {item['rationale']}" for item in backlog_candidates],
                ]
            )

        spec_lines = [
            f"# Spec Feedback: {feature_title}",
            "",
            "## Corrections",
        ]
        corrections = spec_feedback["corrections"] or ["- No spec corrections were synthesized this window."]
        if corrections and isinstance(corrections[0], str) and corrections[0].startswith("- "):
            spec_lines.extend(corrections)
        else:
            spec_lines.extend(f"- {item}" for item in corrections)
        spec_lines.extend(["", "## Acceptance Updates"])
        acceptance_updates = spec_feedback["acceptance_updates"] or [
            "- No acceptance updates were synthesized this window."
        ]
        if acceptance_updates and acceptance_updates[0].startswith("- "):
            spec_lines.extend(acceptance_updates)
        else:
            spec_lines.extend(f"- {item}" for item in acceptance_updates)

        packet_markdown = [
            "\n".join(
                [
                    f"# {packet['title']}",
                    "",
                    f"- Severity: {packet['severity']}",
                    f"- Status: {packet['status']}",
                    "",
                    packet["summary"],
                    "",
                    "## Action Items",
                    *[f"- {item}" for item in packet["action_items"]],
                ]
            )
            for packet in incident_learning_packets
        ]
        if not packet_markdown and upstream_feedback["messages"]:
            packet_markdown.append(
                "\n".join(
                    [
                        f"# Feedback Follow-up: {feature_title}",
                        "",
                        "## Upstream Feedback",
                        *[f"- {item}" for item in upstream_feedback["messages"]],
                    ]
                )
            )

        return {
            "weekly_factory_report_md": "\n".join(weekly_lines),
            "spec_feedback_md": "\n".join(spec_lines),
            "incident_learning_packets_md": packet_markdown,
        }

    @staticmethod
    def _summary(
        *,
        signals: dict[str, list[str]],
        incident_learning_packets: list[dict[str, Any]],
        eval_improvements: list[dict[str, Any]],
        guardrail_recommendations: list[dict[str, Any]],
        backlog_candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        themes: list[str] = []
        if signals["production_incidents"]:
            themes.append("production_incident")
        if signals["unexpected_user_behaviors"]:
            themes.append("user_behavior")
        if signals["spec_mismatches"]:
            themes.append("spec_correction")
        if eval_improvements:
            themes.append("eval_gap")
        if guardrail_recommendations:
            themes.append("guardrail_change")
        if signals["positive_surprises"]:
            themes.append("positive_signal")
        if not themes:
            themes.append("steady_state")

        top_actions = []
        for packet in incident_learning_packets:
            top_actions.extend(packet["action_items"][:2])
        top_actions.extend(item["title"] for item in backlog_candidates[:3])
        top_actions.extend(item["title"] for item in guardrail_recommendations[:2])
        top_actions = FeedbackSynthesizer._dedupe(top_actions)[:5]

        learning_count = sum(
            len(values)
            for key, values in signals.items()
            if key != "positive_surprises"
        ) + len(signals["positive_surprises"])
        learning_count += len(eval_improvements) + len(guardrail_recommendations)

        return {
            "themes": themes,
            "top_actions": top_actions,
            "learning_count": learning_count,
            "incident_count": len(incident_learning_packets),
        }

    @staticmethod
    def _dedupe(items: list[str]) -> list[str]:
        deduped: list[str] = []
        for item in items:
            if item not in deduped:
                deduped.append(item)
        return deduped


class Stage9FeedbackSynthesisPipeline:
    """Capture Stage 9 learnings and link them back to Stage 1 inputs."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        controller: FactoryController | None = None,
        feedback_synthesizer: FeedbackSynthesizer | None = None,
    ) -> None:
        self.root = repo_root(root)
        self.controller = controller or FactoryController()
        self.feedback_synthesizer = feedback_synthesizer or FeedbackSynthesizer()
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
        promotion_decision: dict[str, Any],
        monitoring_report: dict[str, Any],
        work_item: WorkItem,
        *,
        merge_decision: dict[str, Any] | None = None,
        feedback_report_id: str | None = None,
        feedback_window_days: int = 7,
        unexpected_user_behaviors: list[str] | None = None,
        positive_surprises: list[str] | None = None,
        spec_mismatches: list[str] | None = None,
        eval_misses: list[str] | None = None,
    ) -> Stage9FeedbackResult:
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
        self._validate_document("promotion-decision", promotion_decision)
        self._validate_document("monitoring-report", monitoring_report)
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
            promotion_decision,
            monitoring_report,
            merge_decision,
            work_item,
        )

        timestamp = utc_now()
        artifact_id = feedback_report_id or self._default_feedback_report_id(
            spec_packet["artifact"]["id"],
            work_item.attempt_count,
            int(pr_packet["artifact"]["version"]),
        )
        feedback_report = self.feedback_synthesizer.build_feedback_report(
            spec_packet,
            policy_decision,
            ticket_bundle,
            eval_manifest,
            pr_packet,
            latency_baseline,
            eval_report,
            security_review,
            promotion_decision,
            monitoring_report,
            artifact_id=artifact_id,
            build_attempt=work_item.attempt_count,
            feedback_window_days=feedback_window_days,
            unexpected_user_behaviors=unexpected_user_behaviors,
            positive_surprises=positive_surprises,
            spec_mismatches=spec_mismatches,
            eval_misses=eval_misses,
            timestamp=timestamp,
        )

        self._validate_document("feedback-report", feedback_report)
        self._validate_generated_consistency(
            pr_packet,
            monitoring_report,
            feedback_report,
            work_item,
        )

        working_item = deepcopy(work_item)
        self.controller.apply_event(
            working_item,
            event=ControllerEvent.FEEDBACK_SYNTHESIZED,
            artifact_id=feedback_report["artifact"]["id"],
            occurred_at=feedback_report["artifact"]["updated_at"],
        )
        self._validate_document("work-item", working_item.to_document())

        return Stage9FeedbackResult(
            spec_packet=spec_packet,
            policy_decision=policy_decision,
            ticket_bundle=ticket_bundle,
            eval_manifest=eval_manifest,
            pr_packet=pr_packet,
            prompt_contract=prompt_contract,
            tool_schema=tool_schema,
            golden_dataset=golden_dataset,
            latency_baseline=latency_baseline,
            eval_report=eval_report,
            security_review=security_review,
            merge_decision=merge_decision,
            promotion_decision=promotion_decision,
            monitoring_report=monitoring_report,
            feedback_report=feedback_report,
            work_item=working_item,
        )

    def _validate_document(self, schema_name: str, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.validators[schema_name], document)
        if errors:
            raise FeedbackSynthesisError(
                f"{schema_name} failed validation: {'; '.join(errors)}"
            )

    @staticmethod
    def _default_feedback_report_id(
        spec_packet_id: str,
        build_attempt: int,
        analyzed_pr_artifact_version: int,
    ) -> str:
        return build_identifier(
            "feedback-report",
            f"attempt-{build_attempt}-prv-{analyzed_pr_artifact_version}-{spec_packet_id}",
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
        promotion_decision: dict[str, Any],
        monitoring_report: dict[str, Any],
        merge_decision: dict[str, Any] | None,
        work_item: WorkItem,
    ) -> None:
        spec_packet_id = spec_packet["artifact"]["id"]
        policy_artifact = policy_decision["artifact"]
        lane = policy_decision["lane_assignment"]["lane"]

        if policy_decision["decision"] != "active_build_candidate":
            raise FeedbackSynthesisEligibilityError(
                "Only active_build_candidate items can enter Stage 9 feedback synthesis."
            )
        if work_item.state is not ControllerState.PRODUCTION_MONITORING:
            raise FeedbackSynthesisEligibilityError(
                "Work item must be in PRODUCTION_MONITORING before Stage 9 runs; "
                f"got {work_item.state.value}."
            )
        if work_item.attempt_count < 1:
            raise FeedbackSynthesisEligibilityError(
                "Work item must have at least one build attempt before Stage 9 runs."
            )
        if monitoring_report["build_attempt"] != work_item.attempt_count:
            raise FeedbackSynthesisConsistencyError(
                "monitoring-report build_attempt does not match work-item attempt_count."
            )
        if (
            "merge" in policy_decision["required_approvals"]
            and merge_decision is None
        ):
            raise FeedbackSynthesisConsistencyError(
                "Stage 9 requires merge-decision input for lanes that require merge approval."
            )
        if work_item.policy_decision_id != policy_artifact["id"]:
            raise FeedbackSynthesisConsistencyError(
                "work-item policy_decision_id does not match the policy decision artifact."
            )
        if work_item.execution_lane != lane:
            raise FeedbackSynthesisConsistencyError(
                "work-item execution_lane does not match the policy lane."
            )
        if work_item.risk_score != policy_decision["risk_score"]:
            raise FeedbackSynthesisConsistencyError(
                "work-item risk_score does not match the policy decision."
            )
        if work_item.source_provider != spec_packet["source"]["provider"]:
            raise FeedbackSynthesisConsistencyError(
                "work-item source_provider does not match the provided spec-packet."
            )
        if work_item.source_external_id != spec_packet["source"]["external_id"]:
            raise FeedbackSynthesisConsistencyError(
                "work-item source_external_id does not match the provided spec-packet."
            )
        if work_item.current_artifact_id in {None, promotion_decision["artifact"]["id"]}:
            raise FeedbackSynthesisConsistencyError(
                "work-item current_artifact_id must already reflect monitoring or feedback output before Stage 9 runs."
            )
        if (
            work_item.current_artifact_id != monitoring_report["artifact"]["id"]
            and not Stage9FeedbackSynthesisPipeline._has_recorded_feedback_snapshot(
                work_item,
                work_item.current_artifact_id,
            )
        ):
            raise FeedbackSynthesisConsistencyError(
                "work-item current_artifact_id must reference the latest monitoring artifact or a recorded earlier feedback-report."
            )
        if policy_decision["spec_packet_id"] != spec_packet_id:
            raise FeedbackSynthesisConsistencyError(
                "policy-decision does not reference the provided spec-packet."
            )
        if ticket_bundle["spec_packet_id"] != spec_packet_id:
            raise FeedbackSynthesisConsistencyError(
                "ticket-bundle does not reference the provided spec-packet."
            )
        if ticket_bundle["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "ticket-bundle eval_manifest_id does not match the provided eval-manifest."
            )
        if eval_manifest["target_id"] != ticket_bundle["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "eval-manifest target_id does not match the provided ticket-bundle."
            )
        if pr_packet["spec_packet_id"] != spec_packet_id:
            raise FeedbackSynthesisConsistencyError(
                "pr-packet spec_packet_id does not match the provided spec-packet."
            )
        if pr_packet["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "pr-packet eval_manifest_id does not match the provided eval-manifest."
            )
        if prompt_contract["spec_packet_id"] != spec_packet_id:
            raise FeedbackSynthesisConsistencyError(
                "prompt-contract spec_packet_id does not match the provided spec-packet."
            )
        if prompt_contract["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "prompt-contract pr_packet_id does not match the provided pr-packet."
            )
        if tool_schema["spec_packet_id"] != spec_packet_id:
            raise FeedbackSynthesisConsistencyError(
                "tool-schema spec_packet_id does not match the provided spec-packet."
            )
        if tool_schema["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "tool-schema prompt_contract_id does not match the provided prompt contract."
            )
        if golden_dataset["spec_packet_id"] != spec_packet_id:
            raise FeedbackSynthesisConsistencyError(
                "golden-dataset spec_packet_id does not match the provided spec-packet."
            )
        if golden_dataset["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "golden-dataset prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["spec_packet_id"] != spec_packet_id:
            raise FeedbackSynthesisConsistencyError(
                "latency-baseline spec_packet_id does not match the provided spec-packet."
            )
        if latency_baseline["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "latency-baseline prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "latency-baseline pr_packet_id does not match the provided pr-packet."
            )
        if eval_report["spec_packet_id"] != spec_packet_id:
            raise FeedbackSynthesisConsistencyError(
                "eval-report spec_packet_id does not match the provided spec-packet."
            )
        if eval_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "eval-report pr_packet_id does not match the provided pr-packet."
            )
        if security_review["spec_packet_id"] != spec_packet_id:
            raise FeedbackSynthesisConsistencyError(
                "security-review spec_packet_id does not match the provided spec-packet."
            )
        if security_review["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "security-review pr_packet_id does not match the provided pr-packet."
            )
        if security_review["eval_report_id"] != eval_report["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "security-review eval_report_id does not match the provided eval-report."
            )
        if promotion_decision["spec_packet_id"] != spec_packet_id:
            raise FeedbackSynthesisConsistencyError(
                "promotion-decision spec_packet_id does not match the provided spec-packet."
            )
        if promotion_decision["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "promotion-decision pr_packet_id does not match the provided pr-packet."
            )
        if promotion_decision["security_review_id"] != security_review["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "promotion-decision security_review_id does not match the provided security-review."
            )
        if promotion_decision["eval_report_id"] != eval_report["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "promotion-decision eval_report_id does not match the provided eval-report."
            )
        if promotion_decision["latency_baseline_id"] != latency_baseline["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "promotion-decision latency_baseline_id does not match the provided latency-baseline."
            )
        if monitoring_report["spec_packet_id"] != spec_packet_id:
            raise FeedbackSynthesisConsistencyError(
                "monitoring-report spec_packet_id does not match the provided spec-packet."
            )
        if monitoring_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "monitoring-report pr_packet_id does not match the provided pr-packet."
            )
        if monitoring_report["promotion_decision_id"] != promotion_decision["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "monitoring-report promotion_decision_id does not match the provided promotion-decision."
            )
        if monitoring_report["security_review_id"] != security_review["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "monitoring-report security_review_id does not match the provided security-review."
            )
        if monitoring_report["eval_report_id"] != eval_report["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "monitoring-report eval_report_id does not match the provided eval-report."
            )
        if monitoring_report["latency_baseline_id"] != latency_baseline["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "monitoring-report latency_baseline_id does not match the provided latency-baseline."
            )
        if merge_decision is not None:
            if merge_decision["spec_packet_id"] != spec_packet_id:
                raise FeedbackSynthesisConsistencyError(
                    "merge-decision spec_packet_id does not match the provided spec-packet."
                )
            if merge_decision["pr_packet_id"] != pr_packet["artifact"]["id"]:
                raise FeedbackSynthesisConsistencyError(
                    "merge-decision pr_packet_id does not match the provided pr-packet."
                )
            if merge_decision["security_review_id"] != security_review["artifact"]["id"]:
                raise FeedbackSynthesisConsistencyError(
                    "merge-decision security_review_id does not match the provided security-review."
                )
            if merge_decision["merge_decision"]["status"] != "merged":
                raise FeedbackSynthesisEligibilityError(
                    "Stage 9 only runs after merge orchestration reports a merged decision."
                )
            if (
                promotion_decision["evaluated_pr_artifact_version"]
                < merge_decision["resulting_pr_artifact_version"]
            ):
                raise FeedbackSynthesisConsistencyError(
                    "promotion-decision evaluated_pr_artifact_version cannot be earlier than the merge-decision result."
                )
            if pr_packet.get("merge_execution", {}).get("status") != "merged":
                raise FeedbackSynthesisEligibilityError(
                    "Stage 9 requires pr-packet merge_execution.status=merged for lanes that require merge approval."
                )
        if int(pr_packet["artifact"]["version"]) != monitoring_report["resulting_pr_artifact_version"]:
            raise FeedbackSynthesisConsistencyError(
                "pr-packet version must match the monitoring-report resulting version for Stage 9 synthesis."
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
            ("promotion-decision", promotion_decision["artifact"]),
            ("monitoring-report", monitoring_report["artifact"]),
        ):
            if artifact["policy_decision_id"] != policy_artifact["id"]:
                raise FeedbackSynthesisConsistencyError(
                    f"{artifact_name} policy_decision_id does not match the policy decision artifact."
                )
            if artifact["execution_lane"] != lane:
                raise FeedbackSynthesisConsistencyError(
                    f"{artifact_name} execution_lane does not match the policy lane."
                )
            if artifact["risk_tier"] != policy_artifact["risk_tier"]:
                raise FeedbackSynthesisConsistencyError(
                    f"{artifact_name} risk_tier does not match the policy decision artifact."
                )
            if artifact["budget_class"] != policy_artifact["budget_class"]:
                raise FeedbackSynthesisConsistencyError(
                    f"{artifact_name} budget_class does not match the policy decision artifact."
                )
            if artifact["rollback_class"] != policy_artifact["rollback_class"]:
                raise FeedbackSynthesisConsistencyError(
                    f"{artifact_name} rollback_class does not match the policy decision artifact."
                )
            if sorted(artifact["approval_requirements"]) != sorted(
                policy_artifact["approval_requirements"]
            ):
                raise FeedbackSynthesisConsistencyError(
                    f"{artifact_name} approval_requirements do not match the policy decision artifact."
                )
        if merge_decision is not None:
            artifact = merge_decision["artifact"]
            if artifact["policy_decision_id"] != policy_artifact["id"]:
                raise FeedbackSynthesisConsistencyError(
                    "merge-decision policy_decision_id does not match the policy decision artifact."
                )
            if artifact["execution_lane"] != lane:
                raise FeedbackSynthesisConsistencyError(
                    "merge-decision execution_lane does not match the policy lane."
                )
            if artifact["risk_tier"] != policy_artifact["risk_tier"]:
                raise FeedbackSynthesisConsistencyError(
                    "merge-decision risk_tier does not match the policy decision artifact."
                )
            if artifact["budget_class"] != policy_artifact["budget_class"]:
                raise FeedbackSynthesisConsistencyError(
                    "merge-decision budget_class does not match the policy decision artifact."
                )
            if artifact["rollback_class"] != policy_artifact["rollback_class"]:
                raise FeedbackSynthesisConsistencyError(
                    "merge-decision rollback_class does not match the policy decision artifact."
                )
            if sorted(artifact["approval_requirements"]) != sorted(
                policy_artifact["approval_requirements"]
            ):
                raise FeedbackSynthesisConsistencyError(
                    "merge-decision approval_requirements do not match the policy decision artifact."
                )

    @staticmethod
    def _validate_generated_consistency(
        pr_packet: dict[str, Any],
        monitoring_report: dict[str, Any],
        feedback_report: dict[str, Any],
        work_item: WorkItem,
    ) -> None:
        if feedback_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "feedback-report pr_packet_id does not match the provided pr-packet."
            )
        if feedback_report["monitoring_report_id"] != monitoring_report["artifact"]["id"]:
            raise FeedbackSynthesisConsistencyError(
                "feedback-report monitoring_report_id does not match the provided monitoring-report."
            )
        if feedback_report["build_attempt"] != work_item.attempt_count:
            raise FeedbackSynthesisConsistencyError(
                "feedback-report build_attempt does not match the work-item attempt count."
            )
        if feedback_report["analyzed_pr_artifact_version"] != pr_packet["artifact"]["version"]:
            raise FeedbackSynthesisConsistencyError(
                "feedback-report analyzed_pr_artifact_version does not match the provided pr-packet version."
            )
        mode = feedback_report["feedback_window"]["mode"]
        has_packets = bool(feedback_report["incident_learning_packets"])
        if mode == "incident_follow_up" and not has_packets:
            raise FeedbackSynthesisConsistencyError(
                "incident follow-up feedback must emit at least one incident learning packet."
            )
        if mode == "weekly_rollup" and has_packets:
            raise FeedbackSynthesisConsistencyError(
                "weekly rollup feedback cannot emit incident learning packets."
            )

    @staticmethod
    def _has_recorded_feedback_snapshot(
        work_item: WorkItem,
        artifact_id: str | None,
    ) -> bool:
        if artifact_id is None:
            return False
        return any(
            record.event == ControllerEvent.FEEDBACK_SYNTHESIZED.value
            and record.artifact_id == artifact_id
            for record in work_item.history
        )
