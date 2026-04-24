from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contracts import load_validators, validation_errors_for
from .controller import ControllerEvent, ControllerState, FactoryController, WorkItem
from .intake import build_identifier, repo_root, utc_now
from .release_staging import ReleaseManager


class ProductionMonitoringError(RuntimeError):
    """Base class for Stage 8 production-monitoring failures."""


class ProductionMonitoringEligibilityError(ProductionMonitoringError):
    """Raised when a work item cannot enter Stage 8 production monitoring."""


class ProductionMonitoringConsistencyError(ProductionMonitoringError):
    """Raised when Stage 7 artifacts disagree about the production candidate."""


@dataclass(slots=True)
class Stage8ProductionMonitoringResult:
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


class SRESentinel:
    """Watch production health and decide whether automated mitigation is needed."""

    PROMPT_CONTRACT_ID = "sre_sentinel.v1"
    OWNER_AGENT = "SRE Sentinel"
    _OPEN_INCIDENT_STAGES = {"feedback_synthesis", "human_incident_response"}
    _SEVERITY_RANK = {"medium": 1, "high": 2, "critical": 3}
    _ALERT_METADATA = {
        "error_rate_pct": {
            "title": "Production error rate regression",
            "message": "Error rate exceeded the allowed production threshold.",
            "severity": "high",
        },
        "p95_latency_ms": {
            "title": "Production latency regression",
            "message": "Latency exceeded the allowed production threshold.",
            "severity": "medium",
        },
        "cost_per_call_usd": {
            "title": "Production cost regression",
            "message": "Cost per call exceeded the allowed production threshold.",
            "severity": "medium",
        },
        "quality_score": {
            "title": "Production quality regression",
            "message": "Quality score fell below the allowed production floor.",
            "severity": "high",
        },
        "fallback_rate_pct": {
            "title": "Fallback rate regression",
            "message": "Fallback rate exceeded the allowed production threshold.",
            "severity": "high",
        },
        "business_kpi_proxy": {
            "title": "Business KPI regression",
            "message": "Business KPI proxy regressed below the allowed production floor.",
            "severity": "high",
        },
        "security_anomaly": {
            "title": "Critical production security anomaly",
            "message": "A severe safety or security anomaly was detected in production output.",
            "severity": "critical",
        },
    }

    def __init__(self, release_manager: ReleaseManager | None = None) -> None:
        self.release_manager = release_manager or ReleaseManager()

    def build_monitoring_report(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        pr_packet: dict[str, Any],
        eval_report: dict[str, Any],
        security_review: dict[str, Any],
        promotion_decision: dict[str, Any],
        latency_baseline: dict[str, Any],
        *,
        artifact_id: str,
        build_attempt: int,
        observed_window_minutes: int | None = None,
        metric_overrides: dict[str, float | int] | None = None,
        security_anomaly: bool = False,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        created_at = timestamp or utc_now()
        policy_artifact = policy_decision["artifact"]
        lane = policy_decision["lane_assignment"]["lane"]
        rollout_mode = promotion_decision["release_runbook"]["traffic_mode"]
        observed_window = observed_window_minutes if observed_window_minutes is not None else 240
        thresholds = self._thresholds(lane, latency_baseline)
        observed_metrics = self._metrics(
            lane,
            latency_baseline,
            metric_overrides=metric_overrides,
        )
        regressions = self._regressions(
            observed_metrics,
            thresholds,
            security_anomaly=security_anomaly,
        )
        severity = self._incident_severity(regressions)
        mitigation = self._mitigation(
            policy_decision,
            regressions=regressions,
            security_anomaly=security_anomaly,
        )
        decision = self._monitoring_decision(
            regressions=regressions,
            mitigation=mitigation,
        )
        alert_status = (
            "mitigated"
            if decision["status"] == "auto_mitigated"
            else "escalated"
        )
        alerts = self._alerts(
            regressions,
            thresholds,
            alert_status=alert_status,
            autonomous_mitigation_eligible=bool(mitigation["autonomous_allowed"]),
        )
        detection_time_minutes = (
            min(observed_window, 45) if regressions else observed_window
        )
        page_targets = ["lead_engineer", "core_builder"] if regressions else []
        monitored_pr_artifact_version = int(pr_packet["artifact"]["version"])
        resulting_pr_artifact_version = monitored_pr_artifact_version + 1

        return {
            "artifact": {
                "id": artifact_id,
                "version": 1,
                "source_stage": "production_monitoring",
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
                "blocking_issues": [regression["message"] for regression in regressions],
                "created_at": created_at,
                "updated_at": created_at,
            },
            "spec_packet_id": spec_packet["artifact"]["id"],
            "pr_packet_id": pr_packet["artifact"]["id"],
            "promotion_decision_id": promotion_decision["artifact"]["id"],
            "security_review_id": security_review["artifact"]["id"],
            "eval_report_id": eval_report["artifact"]["id"],
            "latency_baseline_id": latency_baseline["artifact"]["id"],
            "build_attempt": build_attempt,
            "monitored_pr_artifact_version": monitored_pr_artifact_version,
            "resulting_pr_artifact_version": resulting_pr_artifact_version,
            "monitoring_window": {
                "environment": "production",
                "traffic_mode": rollout_mode,
                "observed_window_minutes": observed_window,
                "detection_time_minutes": detection_time_minutes,
                "regression_detection_slo_hours": 4,
            },
            "thresholds": thresholds,
            "observed_metrics": observed_metrics,
            "alerts": alerts,
            "incident": {
                "status": self._incident_status(decision["status"]),
                "severity": severity,
                "page_targets": page_targets,
                "detection_time_minutes": detection_time_minutes,
                "regressions": [regression["message"] for regression in regressions],
                "security_anomaly": security_anomaly,
            },
            "mitigation": mitigation,
            "monitoring_decision": decision,
            "summary": {
                "regressions": [regression["message"] for regression in regressions],
                "watch_items": self._watch_items(
                    decision["status"],
                    rollout_mode=rollout_mode,
                    mitigation=mitigation,
                ),
                "follow_up_actions": self._follow_up_actions(
                    decision["status"],
                    mitigation=mitigation,
                ),
            },
        }

    def finalize_pr_packet(
        self,
        pr_packet: dict[str, Any],
        monitoring_report: dict[str, Any],
        *,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        updated = deepcopy(pr_packet)
        updated_at = timestamp or utc_now()
        decision = monitoring_report["monitoring_decision"]
        summary = monitoring_report["summary"]
        blocking_findings = summary["regressions"]
        open_incident_stage = self._open_incident_stage(pr_packet)
        prior_blockers = list(pr_packet["reviewer_report"]["blocking_findings"])
        non_blocking = [
            finding
            for finding in updated["reviewer_report"]["non_blocking_findings"]
            if not finding.startswith(f"{self.OWNER_AGENT} ")
        ]
        non_blocking.extend(summary["watch_items"])
        non_blocking.extend(monitoring_report["mitigation"]["notes"])

        updated["artifact"]["version"] = int(updated["artifact"]["version"]) + 1
        updated["artifact"]["owner_agent"] = self.OWNER_AGENT
        updated["artifact"]["model_fingerprint"] = self.PROMPT_CONTRACT_ID
        updated["artifact"]["updated_at"] = updated_at

        if decision["status"] == "healthy":
            if open_incident_stage is not None:
                blockers = self._dedupe(prior_blockers) or [
                    "A prior production incident remains open until follow-up work completes."
                ]
                non_blocking.append(
                    "SRE Sentinel observed stable production metrics after a prior incident, "
                    "but the incident follow-up remains open."
                )
                updated["artifact"]["status"] = "blocked"
                updated["artifact"]["next_stage"] = open_incident_stage
                updated["artifact"]["blocking_issues"] = blockers
                updated["reviewer_report"] = {
                    "approved": False,
                    "blocking_findings": blockers,
                    "non_blocking_findings": self._dedupe(non_blocking),
                }
                updated["merge_readiness"] = {
                    "reviewable": False,
                    "mergeable": False,
                    "blockers": blockers,
                }
                return updated
            updated["artifact"]["status"] = "approved"
            updated["artifact"]["next_stage"] = "production_monitoring"
            updated["artifact"]["blocking_issues"] = []
            non_blocking.append(
                "SRE Sentinel observed stable production metrics inside the monitoring window."
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

        updated["artifact"]["status"] = "blocked"
        updated["artifact"]["next_stage"] = (
            "human_incident_response"
            if decision["status"] == "human_escalated"
            or open_incident_stage == "human_incident_response"
            else "feedback_synthesis"
        )
        blockers = self._dedupe(prior_blockers + blocking_findings)
        updated["artifact"]["blocking_issues"] = blockers
        updated["reviewer_report"] = {
            "approved": False,
            "blocking_findings": blockers,
            "non_blocking_findings": self._dedupe(non_blocking),
        }
        updated["merge_readiness"] = {
            "reviewable": False,
            "mergeable": False,
            "blockers": blockers,
        }
        return updated

    def _thresholds(
        self,
        lane: str,
        latency_baseline: dict[str, Any],
    ) -> dict[str, float | int]:
        baseline = latency_baseline["baseline"]
        thresholds = latency_baseline["thresholds"]
        return {
            "max_error_rate_pct": self.release_manager._ERROR_RATE_LIMIT_BY_LANE[lane],
            "max_p95_latency_ms": int(
                round(
                    baseline["expected_p95_latency_ms"]
                    * (1 + (thresholds["max_latency_regression_pct"] / 100))
                )
            ),
            "max_cost_per_call_usd": round(
                baseline["expected_cost_per_call_usd"]
                * (1 + (thresholds["max_cost_regression_pct"] / 100)),
                4,
            ),
            "min_quality_score": round(
                baseline["expected_quality_score"] - thresholds["max_quality_score_delta"],
                2,
            ),
            "max_fallback_rate_pct": self.release_manager._FALLBACK_RATE_LIMIT_BY_LANE[lane],
            "min_business_kpi_proxy": self.release_manager._BUSINESS_KPI_FLOOR_BY_LANE[lane],
        }

    @staticmethod
    def _metrics(
        lane: str,
        latency_baseline: dict[str, Any],
        *,
        metric_overrides: dict[str, float | int] | None = None,
    ) -> dict[str, float | int]:
        baseline = latency_baseline["baseline"]
        metrics: dict[str, float | int] = {
            "error_rate_pct": 0.15 if lane == "fast" else 0.25 if lane == "guarded" else 0.3,
            "p95_latency_ms": int(round(baseline["expected_p95_latency_ms"] * 0.97)),
            "cost_per_call_usd": round(baseline["expected_cost_per_call_usd"] * 0.98, 4),
            "quality_score": round(min(1.0, baseline["expected_quality_score"] + 0.01), 2),
            "fallback_rate_pct": 0.3 if lane == "fast" else 0.7 if lane == "guarded" else 0.9,
            "business_kpi_proxy": 0.98 if lane == "fast" else 0.97 if lane == "guarded" else 0.96,
        }
        if metric_overrides:
            metrics.update(metric_overrides)
        return metrics

    def _regressions(
        self,
        observed_metrics: dict[str, float | int],
        thresholds: dict[str, float | int],
        *,
        security_anomaly: bool,
    ) -> list[dict[str, Any]]:
        regressions: list[dict[str, Any]] = []
        if security_anomaly:
            metadata = self._ALERT_METADATA["security_anomaly"]
            regressions.append(
                {
                    "metric": "security_anomaly",
                    "title": metadata["title"],
                    "message": metadata["message"],
                    "severity": metadata["severity"],
                    "observed_value": True,
                    "threshold_value": False,
                }
            )
        for metric_name, comparator in (
            ("error_rate_pct", lambda observed, threshold: float(observed) > float(threshold)),
            ("p95_latency_ms", lambda observed, threshold: int(observed) > int(threshold)),
            ("cost_per_call_usd", lambda observed, threshold: float(observed) > float(threshold)),
            ("quality_score", lambda observed, threshold: float(observed) < float(threshold)),
            ("fallback_rate_pct", lambda observed, threshold: float(observed) > float(threshold)),
            ("business_kpi_proxy", lambda observed, threshold: float(observed) < float(threshold)),
        ):
            threshold_key = {
                "error_rate_pct": "max_error_rate_pct",
                "p95_latency_ms": "max_p95_latency_ms",
                "cost_per_call_usd": "max_cost_per_call_usd",
                "quality_score": "min_quality_score",
                "fallback_rate_pct": "max_fallback_rate_pct",
                "business_kpi_proxy": "min_business_kpi_proxy",
            }[metric_name]
            if comparator(observed_metrics[metric_name], thresholds[threshold_key]):
                metadata = self._ALERT_METADATA[metric_name]
                regressions.append(
                    {
                        "metric": metric_name,
                        "title": metadata["title"],
                        "message": metadata["message"],
                        "severity": metadata["severity"],
                        "observed_value": observed_metrics[metric_name],
                        "threshold_value": thresholds[threshold_key],
                    }
                )
        return regressions

    def _alerts(
        self,
        regressions: list[dict[str, Any]],
        thresholds: dict[str, float | int],
        *,
        alert_status: str,
        autonomous_mitigation_eligible: bool,
    ) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        for regression in regressions:
            metric = regression["metric"]
            threshold_reference = regression["threshold_value"]
            alerts.append(
                {
                    "id": build_identifier(
                        "alert",
                        f"{metric}-{regression['severity']}-{regression['title']}",
                        max_length=72,
                    ),
                    "title": regression["title"],
                    "metric": metric,
                    "severity": regression["severity"],
                    "status": alert_status,
                    "summary": regression["message"],
                    "observed_value": regression["observed_value"],
                    "threshold_value": threshold_reference,
                    "auto_mitigation_eligible": autonomous_mitigation_eligible or metric == "security_anomaly",
                }
            )
        return alerts

    def _mitigation(
        self,
        policy_decision: dict[str, Any],
        *,
        regressions: list[dict[str, Any]],
        security_anomaly: bool,
    ) -> dict[str, Any]:
        if not regressions:
            return {
                "action": "none",
                "autonomous_allowed": True,
                "executed": False,
                "result": "not_needed",
                "notes": ["No production mitigation was needed."],
            }

        feature_flag_required = bool(policy_decision["deployment_policy"]["feature_flag_required"])
        rollback_autonomy = policy_decision["autonomy_policy"]["rollback"]
        action = "kill_switch" if security_anomaly else (
            "feature_flag_disable" if feature_flag_required else "rollback"
        )
        autonomous_allowed = bool(security_anomaly or rollback_autonomy == "autonomous")
        if autonomous_allowed:
            return {
                "action": action,
                "autonomous_allowed": True,
                "executed": True,
                "result": "successful",
                "notes": [
                    "SRE Sentinel executed the pre-approved mitigation automatically.",
                    (
                        "Critical anomaly triggered the production kill-switch."
                        if action == "kill_switch"
                        else "The feature flag was disabled to stop the regression path."
                    ),
                ],
            }
        return {
            "action": action,
            "autonomous_allowed": False,
            "executed": False,
            "result": "blocked_by_policy",
            "notes": [
                "Lane policy blocked autonomous rollback, so human incident response is required.",
                "Lead engineer and core builder were paged for manual mitigation.",
            ],
        }

    def _monitoring_decision(
        self,
        *,
        regressions: list[dict[str, Any]],
        mitigation: dict[str, Any],
    ) -> dict[str, Any]:
        if not regressions:
            return {
                "status": "healthy",
                "mode": "auto",
                "rationale": "Production metrics stayed within the allowed thresholds during the monitoring window.",
            }
        if mitigation["result"] == "successful":
            return {
                "status": "auto_mitigated",
                "mode": "auto",
                "rationale": "Production regression was detected and mitigated automatically within the approved policy.",
            }
        return {
            "status": "human_escalated",
            "mode": "human_required",
            "rationale": "Production regression was detected, but policy requires human incident response.",
        }

    def _incident_severity(
        self,
        regressions: list[dict[str, Any]],
    ) -> str:
        if not regressions:
            return "none"
        highest = max(
            regressions,
            key=lambda regression: self._SEVERITY_RANK[regression["severity"]],
        )
        return highest["severity"]

    @staticmethod
    def _incident_status(decision_status: str) -> str:
        if decision_status == "healthy":
            return "none"
        if decision_status == "auto_mitigated":
            return "mitigated"
        return "escalated"

    @staticmethod
    def _watch_items(
        decision_status: str,
        *,
        rollout_mode: str,
        mitigation: dict[str, Any],
    ) -> list[str]:
        watch_items = [
            f"Production rollout mode remains {rollout_mode}.",
            "Alert routing keeps the lead engineer first and the core builder second.",
        ]
        if decision_status == "healthy":
            watch_items.append("No alert thresholds fired during this production monitoring window.")
        elif decision_status == "auto_mitigated":
            watch_items.append("Autonomous mitigation succeeded; keep dashboards pinned until the feature is stable again.")
        else:
            watch_items.append("Human incident response is required before reenabling production traffic.")
        if mitigation["action"] == "kill_switch":
            watch_items.append("A kill-switch event was executed because a critical anomaly was detected.")
        return watch_items

    @staticmethod
    def _follow_up_actions(
        decision_status: str,
        *,
        mitigation: dict[str, Any],
    ) -> list[str]:
        if decision_status == "healthy":
            return ["Continue monitoring against the four-hour regression-detection SLO."]
        if decision_status == "auto_mitigated":
            return [
                "Create an incident learning packet for Stage 9.",
                "Open remediation work for the core builder before reenabling the feature.",
                mitigation["notes"][-1],
            ]
        return [
            "Lead engineer acknowledged the page.",
            "Core builder joins manual incident response.",
            "Create an incident learning packet for Stage 9 after mitigation completes.",
        ]

    @staticmethod
    def _artifact_status(decision_status: str) -> str:
        return "approved" if decision_status == "healthy" else "blocked"

    @staticmethod
    def _next_stage(decision_status: str) -> str:
        if decision_status == "healthy":
            return "production_monitoring"
        if decision_status == "auto_mitigated":
            return "feedback_synthesis"
        return "human_incident_response"

    @staticmethod
    def _open_incident_stage(pr_packet: dict[str, Any]) -> str | None:
        artifact = pr_packet["artifact"]
        next_stage = artifact["next_stage"]
        if (
            artifact["owner_agent"] == SRESentinel.OWNER_AGENT
            and artifact["status"] == "blocked"
            and next_stage in SRESentinel._OPEN_INCIDENT_STAGES
        ):
            return next_stage
        return None

    @staticmethod
    def _dedupe(items: list[str]) -> list[str]:
        deduped: list[str] = []
        for item in items:
            if item not in deduped:
                deduped.append(item)
        return deduped


class Stage8ProductionMonitoringPipeline:
    """Run production monitoring checks against a promoted release candidate."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        controller: FactoryController | None = None,
        sre_sentinel: SRESentinel | None = None,
    ) -> None:
        self.root = repo_root(root)
        self.controller = controller or FactoryController()
        self.sre_sentinel = sre_sentinel or SRESentinel()
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
        work_item: WorkItem,
        *,
        merge_decision: dict[str, Any] | None = None,
        monitoring_report_id: str | None = None,
        observed_window_minutes: int | None = None,
        metric_overrides: dict[str, float | int] | None = None,
        security_anomaly: bool = False,
    ) -> Stage8ProductionMonitoringResult:
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
            merge_decision,
            work_item,
        )

        timestamp = utc_now()
        monitored_pr_artifact_version = int(pr_packet["artifact"]["version"])
        artifact_id = monitoring_report_id or self._default_monitoring_report_id(
            spec_packet["artifact"]["id"],
            work_item.attempt_count,
            monitored_pr_artifact_version,
        )
        monitoring_report = self.sre_sentinel.build_monitoring_report(
            spec_packet,
            policy_decision,
            pr_packet,
            eval_report,
            security_review,
            promotion_decision,
            latency_baseline,
            artifact_id=artifact_id,
            build_attempt=work_item.attempt_count,
            observed_window_minutes=observed_window_minutes,
            metric_overrides=metric_overrides,
            security_anomaly=security_anomaly,
            timestamp=timestamp,
        )
        updated_pr_packet = self.sre_sentinel.finalize_pr_packet(
            pr_packet,
            monitoring_report,
            timestamp=timestamp,
        )

        self._validate_document("monitoring-report", monitoring_report)
        self._validate_document("pr-packet", updated_pr_packet)
        self._validate_generated_consistency(
            updated_pr_packet,
            promotion_decision,
            monitoring_report,
            work_item,
        )

        working_item = deepcopy(work_item)
        event = (
            ControllerEvent.PRODUCTION_HEALTH_CHECK_RECORDED
            if monitoring_report["monitoring_decision"]["status"] == "healthy"
            else ControllerEvent.PRODUCTION_INCIDENT_RECORDED
        )
        self.controller.apply_event(
            working_item,
            event=event,
            artifact_id=monitoring_report["artifact"]["id"],
            occurred_at=monitoring_report["artifact"]["updated_at"],
        )
        self._validate_document("work-item", working_item.to_document())

        return Stage8ProductionMonitoringResult(
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
            monitoring_report=monitoring_report,
            work_item=working_item,
        )

    def _validate_document(self, schema_name: str, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.validators[schema_name], document)
        if errors:
            raise ProductionMonitoringError(
                f"{schema_name} failed validation: {'; '.join(errors)}"
            )

    @staticmethod
    def _default_monitoring_report_id(
        spec_packet_id: str,
        build_attempt: int,
        monitored_pr_artifact_version: int,
    ) -> str:
        return build_identifier(
            "monitoring-report",
            f"attempt-{build_attempt}-prv-{monitored_pr_artifact_version}-{spec_packet_id}",
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
        merge_decision: dict[str, Any] | None,
        work_item: WorkItem,
    ) -> None:
        spec_packet_id = spec_packet["artifact"]["id"]
        policy_artifact = policy_decision["artifact"]
        lane = policy_decision["lane_assignment"]["lane"]

        if policy_decision["decision"] != "active_build_candidate":
            raise ProductionMonitoringEligibilityError(
                "Only active_build_candidate items can enter Stage 8 production monitoring."
            )
        if work_item.state is not ControllerState.PRODUCTION_MONITORING:
            raise ProductionMonitoringEligibilityError(
                "Work item must be in PRODUCTION_MONITORING before Stage 8 runs; "
                f"got {work_item.state.value}."
            )
        if promotion_decision["promotion_decision"]["status"] != "promoted":
            raise ProductionMonitoringEligibilityError(
                "Stage 8 only runs after a promoted release decision."
            )
        if (
            "merge" in policy_decision["required_approvals"]
            and merge_decision is None
        ):
            raise ProductionMonitoringConsistencyError(
                "Stage 8 requires merge-decision input for lanes that require merge approval."
            )
        if security_review["signoff"]["status"] != "approved":
            raise ProductionMonitoringEligibilityError(
                "Stage 8 only runs after security review is approved."
            )
        if work_item.attempt_count < 1:
            raise ProductionMonitoringEligibilityError(
                "Work item must have at least one build attempt before Stage 8 runs."
            )
        if work_item.policy_decision_id != policy_artifact["id"]:
            raise ProductionMonitoringConsistencyError(
                "work-item policy_decision_id does not match the policy decision artifact."
            )
        if work_item.execution_lane != lane:
            raise ProductionMonitoringConsistencyError(
                "work-item execution_lane does not match the policy lane."
            )
        if work_item.risk_score != policy_decision["risk_score"]:
            raise ProductionMonitoringConsistencyError(
                "work-item risk_score does not match the policy decision."
            )
        if work_item.source_provider != spec_packet["source"]["provider"]:
            raise ProductionMonitoringConsistencyError(
                "work-item source_provider does not match the provided spec-packet."
            )
        if work_item.source_external_id != spec_packet["source"]["external_id"]:
            raise ProductionMonitoringConsistencyError(
                "work-item source_external_id does not match the provided spec-packet."
            )
        if policy_decision["spec_packet_id"] != spec_packet_id:
            raise ProductionMonitoringConsistencyError(
                "policy-decision does not reference the provided spec-packet."
            )
        if ticket_bundle["spec_packet_id"] != spec_packet_id:
            raise ProductionMonitoringConsistencyError(
                "ticket-bundle does not reference the provided spec-packet."
            )
        if ticket_bundle["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "ticket-bundle eval_manifest_id does not match the provided eval-manifest."
            )
        if eval_manifest["target_id"] != ticket_bundle["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "eval-manifest target_id does not match the provided ticket-bundle."
            )
        if pr_packet["spec_packet_id"] != spec_packet_id:
            raise ProductionMonitoringConsistencyError(
                "pr-packet spec_packet_id does not match the provided spec-packet."
            )
        if pr_packet["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "pr-packet eval_manifest_id does not match the provided eval-manifest."
            )
        if prompt_contract["spec_packet_id"] != spec_packet_id:
            raise ProductionMonitoringConsistencyError(
                "prompt-contract spec_packet_id does not match the provided spec-packet."
            )
        if prompt_contract["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "prompt-contract pr_packet_id does not match the provided pr-packet."
            )
        if tool_schema["spec_packet_id"] != spec_packet_id:
            raise ProductionMonitoringConsistencyError(
                "tool-schema spec_packet_id does not match the provided spec-packet."
            )
        if tool_schema["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "tool-schema prompt_contract_id does not match the provided prompt contract."
            )
        if golden_dataset["spec_packet_id"] != spec_packet_id:
            raise ProductionMonitoringConsistencyError(
                "golden-dataset spec_packet_id does not match the provided spec-packet."
            )
        if golden_dataset["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "golden-dataset prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["spec_packet_id"] != spec_packet_id:
            raise ProductionMonitoringConsistencyError(
                "latency-baseline spec_packet_id does not match the provided spec-packet."
            )
        if latency_baseline["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "latency-baseline prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "latency-baseline pr_packet_id does not match the provided pr-packet."
            )
        if eval_report["spec_packet_id"] != spec_packet_id:
            raise ProductionMonitoringConsistencyError(
                "eval-report spec_packet_id does not match the provided spec-packet."
            )
        if eval_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "eval-report pr_packet_id does not match the provided pr-packet."
            )
        if security_review["spec_packet_id"] != spec_packet_id:
            raise ProductionMonitoringConsistencyError(
                "security-review spec_packet_id does not match the provided spec-packet."
            )
        if security_review["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "security-review pr_packet_id does not match the provided pr-packet."
            )
        if security_review["eval_report_id"] != eval_report["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "security-review eval_report_id does not match the provided eval-report."
            )
        if promotion_decision["spec_packet_id"] != spec_packet_id:
            raise ProductionMonitoringConsistencyError(
                "promotion-decision spec_packet_id does not match the provided spec-packet."
            )
        if promotion_decision["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "promotion-decision pr_packet_id does not match the provided pr-packet."
            )
        if promotion_decision["security_review_id"] != security_review["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "promotion-decision security_review_id does not match the provided security-review."
            )
        if promotion_decision["eval_report_id"] != eval_report["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "promotion-decision eval_report_id does not match the provided eval-report."
            )
        if promotion_decision["latency_baseline_id"] != latency_baseline["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "promotion-decision latency_baseline_id does not match the provided latency-baseline."
            )
        if merge_decision is not None:
            if merge_decision["spec_packet_id"] != spec_packet_id:
                raise ProductionMonitoringConsistencyError(
                    "merge-decision spec_packet_id does not match the provided spec-packet."
                )
            if merge_decision["pr_packet_id"] != pr_packet["artifact"]["id"]:
                raise ProductionMonitoringConsistencyError(
                    "merge-decision pr_packet_id does not match the provided pr-packet."
                )
            if merge_decision["security_review_id"] != security_review["artifact"]["id"]:
                raise ProductionMonitoringConsistencyError(
                    "merge-decision security_review_id does not match the provided security-review."
                )
            if merge_decision["merge_decision"]["status"] != "merged":
                raise ProductionMonitoringEligibilityError(
                    "Stage 8 only runs after merge orchestration reports a merged decision."
                )
            if (
                promotion_decision["evaluated_pr_artifact_version"]
                < merge_decision["resulting_pr_artifact_version"]
            ):
                raise ProductionMonitoringConsistencyError(
                    "promotion-decision evaluated_pr_artifact_version cannot be earlier than the merge-decision result."
                )
            if pr_packet.get("merge_execution", {}).get("status") != "merged":
                raise ProductionMonitoringEligibilityError(
                    "Stage 8 requires pr-packet merge_execution.status=merged for lanes that require merge approval."
                )
        if work_item.attempt_count != promotion_decision["build_attempt"]:
            raise ProductionMonitoringConsistencyError(
                "work-item attempt_count does not match the promotion-decision build attempt."
            )
        if int(pr_packet["artifact"]["version"]) < promotion_decision["resulting_pr_artifact_version"]:
            raise ProductionMonitoringConsistencyError(
                "pr-packet version cannot be earlier than the promotion-decision resulting version."
            )
        if (
            int(pr_packet["artifact"]["version"]) == promotion_decision["resulting_pr_artifact_version"]
            and work_item.current_artifact_id != promotion_decision["artifact"]["id"]
        ):
            raise ProductionMonitoringConsistencyError(
                "work-item current_artifact_id must match the promotion-decision on the first Stage 8 run."
            )
        if (
            int(pr_packet["artifact"]["version"]) > promotion_decision["resulting_pr_artifact_version"]
            and not work_item.current_artifact_id
        ):
            raise ProductionMonitoringConsistencyError(
                "work-item in repeated Stage 8 monitoring must retain the latest monitoring artifact id."
            )
        if int(pr_packet["artifact"]["version"]) > promotion_decision["resulting_pr_artifact_version"]:
            if work_item.current_artifact_id == promotion_decision["artifact"]["id"]:
                raise ProductionMonitoringConsistencyError(
                    "work-item current_artifact_id cannot stay pinned to the promotion-decision after Stage 8 has already run."
                )
            if pr_packet["artifact"]["owner_agent"] != SRESentinel.OWNER_AGENT:
                raise ProductionMonitoringConsistencyError(
                    "Repeated Stage 8 monitoring requires the pr-packet owner_agent to already be SRE Sentinel."
                )
            if pr_packet["artifact"]["model_fingerprint"] != SRESentinel.PROMPT_CONTRACT_ID:
                raise ProductionMonitoringConsistencyError(
                    "Repeated Stage 8 monitoring requires the pr-packet model_fingerprint to match SRE Sentinel."
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
        ):
            if artifact["policy_decision_id"] != policy_artifact["id"]:
                raise ProductionMonitoringConsistencyError(
                    f"{artifact_name} policy_decision_id does not match the policy decision artifact."
                )
            if artifact["execution_lane"] != lane:
                raise ProductionMonitoringConsistencyError(
                    f"{artifact_name} execution_lane does not match the policy lane."
                )
            if artifact["risk_tier"] != policy_artifact["risk_tier"]:
                raise ProductionMonitoringConsistencyError(
                    f"{artifact_name} risk_tier does not match the policy decision artifact."
                )
            if artifact["budget_class"] != policy_artifact["budget_class"]:
                raise ProductionMonitoringConsistencyError(
                    f"{artifact_name} budget_class does not match the policy decision artifact."
                )
            if artifact["rollback_class"] != policy_artifact["rollback_class"]:
                raise ProductionMonitoringConsistencyError(
                    f"{artifact_name} rollback_class does not match the policy decision artifact."
                )
            if sorted(artifact["approval_requirements"]) != sorted(
                policy_artifact["approval_requirements"]
            ):
                raise ProductionMonitoringConsistencyError(
                    f"{artifact_name} approval_requirements do not match the policy decision artifact."
                )
        if merge_decision is not None:
            artifact = merge_decision["artifact"]
            if artifact["policy_decision_id"] != policy_artifact["id"]:
                raise ProductionMonitoringConsistencyError(
                    "merge-decision policy_decision_id does not match the policy decision artifact."
                )
            if artifact["execution_lane"] != lane:
                raise ProductionMonitoringConsistencyError(
                    "merge-decision execution_lane does not match the policy lane."
                )
            if artifact["risk_tier"] != policy_artifact["risk_tier"]:
                raise ProductionMonitoringConsistencyError(
                    "merge-decision risk_tier does not match the policy decision artifact."
                )
            if artifact["budget_class"] != policy_artifact["budget_class"]:
                raise ProductionMonitoringConsistencyError(
                    "merge-decision budget_class does not match the policy decision artifact."
                )
            if artifact["rollback_class"] != policy_artifact["rollback_class"]:
                raise ProductionMonitoringConsistencyError(
                    "merge-decision rollback_class does not match the policy decision artifact."
                )
            if sorted(artifact["approval_requirements"]) != sorted(
                policy_artifact["approval_requirements"]
            ):
                raise ProductionMonitoringConsistencyError(
                    "merge-decision approval_requirements do not match the policy decision artifact."
                )

    @staticmethod
    def _validate_generated_consistency(
        pr_packet: dict[str, Any],
        promotion_decision: dict[str, Any],
        monitoring_report: dict[str, Any],
        work_item: WorkItem,
    ) -> None:
        if monitoring_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "monitoring-report pr_packet_id does not match the updated pr-packet artifact."
            )
        if monitoring_report["promotion_decision_id"] != promotion_decision["artifact"]["id"]:
            raise ProductionMonitoringConsistencyError(
                "monitoring-report promotion_decision_id does not match the provided promotion-decision."
            )
        if monitoring_report["build_attempt"] != work_item.attempt_count:
            raise ProductionMonitoringConsistencyError(
                "monitoring-report build_attempt does not match the work-item attempt count."
            )
        if monitoring_report["monitored_pr_artifact_version"] + 1 != monitoring_report["resulting_pr_artifact_version"]:
            raise ProductionMonitoringConsistencyError(
                "monitoring-report resulting_pr_artifact_version must be exactly one greater than the monitored version."
            )
        if monitoring_report["resulting_pr_artifact_version"] != pr_packet["artifact"]["version"]:
            raise ProductionMonitoringConsistencyError(
                "monitoring-report resulting_pr_artifact_version does not match the updated pr-packet version."
            )
        if monitoring_report["monitored_pr_artifact_version"] < promotion_decision["resulting_pr_artifact_version"]:
            raise ProductionMonitoringConsistencyError(
                "monitoring-report monitored_pr_artifact_version cannot be earlier than the promoted release version."
            )

        decision_status = monitoring_report["monitoring_decision"]["status"]
        healthy_with_open_incident = (
            decision_status == "healthy"
            and SRESentinel._open_incident_stage(pr_packet) is not None
        )
        if decision_status == "healthy":
            if healthy_with_open_incident:
                if pr_packet["merge_readiness"]["mergeable"]:
                    raise ProductionMonitoringConsistencyError(
                        "healthy follow-up monitoring cannot reopen a pr-packet while a prior production incident remains open."
                    )
                if pr_packet["reviewer_report"]["approved"]:
                    raise ProductionMonitoringConsistencyError(
                        "healthy follow-up monitoring cannot approve a pr-packet while a prior production incident remains open."
                    )
                if not pr_packet["reviewer_report"]["blocking_findings"]:
                    raise ProductionMonitoringConsistencyError(
                        "healthy follow-up monitoring must retain blocking findings while the prior production incident remains open."
                    )
            else:
                if not pr_packet["merge_readiness"]["mergeable"]:
                    raise ProductionMonitoringConsistencyError(
                        "healthy production monitoring must leave the pr-packet mergeable."
                    )
                if not pr_packet["reviewer_report"]["approved"]:
                    raise ProductionMonitoringConsistencyError(
                        "healthy production monitoring must leave the reviewer_report approved."
                    )
        else:
            if pr_packet["merge_readiness"]["mergeable"]:
                raise ProductionMonitoringConsistencyError(
                    "incident monitoring cannot leave the pr-packet mergeable."
                )
            if pr_packet["reviewer_report"]["approved"]:
                raise ProductionMonitoringConsistencyError(
                    "incident monitoring cannot leave the reviewer_report approved."
                )
