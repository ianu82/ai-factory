from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contracts import load_validators, validation_errors_for
from .controller import ControllerEvent, ControllerState, FactoryController, WorkItem
from .intake import build_identifier, repo_root, utc_now

LOW_RISK_TOOL_PERMISSIONS = {
    "contract_translation",
    "read_only",
    "schema_validation",
    "tool_result_access",
}


class SecurityReviewError(RuntimeError):
    """Base class for Stage 6 security-review failures."""


class SecurityReviewEligibilityError(SecurityReviewError):
    """Raised when a work item cannot enter Stage 6 security review."""


class SecurityReviewConsistencyError(SecurityReviewError):
    """Raised when Stage 5 artifacts disagree about the work item under review."""


@dataclass(slots=True)
class Stage6SecurityReviewResult:
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


class SecuritySentinel:
    """Threat-model and sign-off the mergeable PR candidate."""

    PROMPT_CONTRACT_ID = "security_sentinel.v1"

    def build_security_review(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        ticket_bundle: dict[str, Any],
        pr_packet: dict[str, Any],
        eval_report: dict[str, Any],
        prompt_contract: dict[str, Any],
        tool_schema: dict[str, Any],
        golden_dataset: dict[str, Any],
        *,
        artifact_id: str,
        build_attempt: int,
        approved_security_reviewers: list[str] | None = None,
        blocking_findings: list[str] | None = None,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        created_at = timestamp or utc_now()
        policy_artifact = policy_decision["artifact"]
        ticket_artifact = ticket_bundle["artifact"]
        evaluated_pr_artifact_version = int(pr_packet["artifact"]["version"])
        resulting_pr_artifact_version = evaluated_pr_artifact_version + 1
        explicit_blocking_findings = [finding for finding in blocking_findings or [] if finding]
        approvals_granted = self._approvals_granted(
            policy_decision,
            approved_security_reviewers or [],
        )
        pending_approvals = self._pending_security_approvals(policy_decision, approvals_granted)
        downstream_approvals = self._downstream_approvals(policy_decision)

        least_privilege_validation = self._least_privilege_validation(
            tool_schema,
            prompt_contract,
        )
        threat_domains = self._threat_domains(
            spec_packet,
            ticket_bundle,
            prompt_contract,
            tool_schema,
            golden_dataset,
            least_privilege_validation,
        )
        abuse_cases = self._abuse_cases(golden_dataset)
        security_checks = self._security_checks(
            spec_packet,
            policy_decision,
            pr_packet,
            prompt_contract,
            golden_dataset,
            least_privilege_validation,
            approvals_granted,
            explicit_blocking_findings,
        )
        blocking_summary = self._blocking_findings(threat_domains, security_checks)
        blocking_summary.extend(explicit_blocking_findings)
        blocking_summary = self._dedupe(blocking_summary)
        watch_summary = self._watch_findings(threat_domains, security_checks)

        passed_check_ids = [
            check["id"] for check in security_checks if check["status"] == "passed"
        ]
        failed_check_ids = [
            check["id"] for check in security_checks if check["status"] == "failed"
        ]
        needs_human_review_check_ids = [
            check["id"]
            for check in security_checks
            if check["status"] == "needs_human_review"
        ]

        signoff = self._signoff(
            blocking_summary=blocking_summary,
            pending_approvals=pending_approvals,
            approvals_granted=approvals_granted,
            downstream_approvals=downstream_approvals,
        )

        return {
            "artifact": {
                "id": artifact_id,
                "version": 1,
                "source_stage": "security_review",
                "next_stage": self._next_stage(signoff["status"]),
                "status": self._artifact_status(signoff["status"]),
                "risk_tier": policy_artifact["risk_tier"],
                "execution_lane": policy_artifact["execution_lane"],
                "owner_agent": "Security Sentinel",
                "policy_decision_id": policy_artifact["id"],
                "model_fingerprint": self.PROMPT_CONTRACT_ID,
                "budget_class": policy_artifact["budget_class"],
                "rollback_class": policy_artifact["rollback_class"],
                "approval_requirements": list(policy_artifact["approval_requirements"]),
                "blocking_issues": blocking_summary,
                "created_at": created_at,
                "updated_at": created_at,
            },
            "spec_packet_id": spec_packet["artifact"]["id"],
            "ticket_bundle_id": ticket_artifact["id"],
            "pr_packet_id": pr_packet["artifact"]["id"],
            "eval_report_id": eval_report["artifact"]["id"],
            "prompt_contract_id": prompt_contract["artifact"]["id"],
            "tool_schema_id": tool_schema["artifact"]["id"],
            "golden_dataset_id": golden_dataset["artifact"]["id"],
            "build_attempt": build_attempt,
            "evaluated_pr_artifact_version": evaluated_pr_artifact_version,
            "resulting_pr_artifact_version": resulting_pr_artifact_version,
            "threat_model": {
                "domains": threat_domains,
                "blast_radius": spec_packet["risk_profile"]["blast_radius"],
                "rollback_class": spec_packet["risk_profile"]["rollback_class"],
                "rollback_notes": pr_packet["summary"]["rollback_notes"],
            },
            "abuse_cases": abuse_cases,
            "security_checks": security_checks,
            "least_privilege_validation": least_privilege_validation,
            "signoff": signoff,
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
        security_review: dict[str, Any],
        *,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        updated = deepcopy(pr_packet)
        updated_at = timestamp or utc_now()
        signoff = security_review["signoff"]
        summary = security_review["summary"]
        blocking_findings = summary["blocking_findings"]
        non_blocking = [
            finding
            for finding in updated["reviewer_report"]["non_blocking_findings"]
            if not finding.startswith("Security review ")
        ]
        non_blocking.extend(summary["watch_findings"])

        updated["artifact"]["version"] = int(updated["artifact"]["version"]) + 1
        updated["artifact"]["owner_agent"] = "Security Sentinel"
        updated["artifact"]["model_fingerprint"] = self.PROMPT_CONTRACT_ID
        updated["artifact"]["updated_at"] = updated_at

        if signoff["status"] == "approved":
            updated["artifact"]["status"] = "approved"
            updated["artifact"]["next_stage"] = "release_staging"
            updated["artifact"]["blocking_issues"] = []
            non_blocking.append(
                "Security review approved."
                if signoff["mode"] == "auto"
                else "Security review approved after explicit sign-off."
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

        if signoff["status"] == "pending_human":
            updated["artifact"]["status"] = "ready"
            updated["artifact"]["next_stage"] = "human_security_signoff"
            updated["artifact"]["blocking_issues"] = []
            non_blocking.append(
                "Security review is waiting on explicit human sign-off."
            )
            updated["reviewer_report"] = {
                "approved": True,
                "blocking_findings": [],
                "non_blocking_findings": self._dedupe(non_blocking),
            }
            updated["merge_readiness"] = {
                "reviewable": True,
                "mergeable": False,
                "blockers": ["Security sign-off is still pending."],
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
    def _approvals_granted(
        policy_decision: dict[str, Any],
        approved_security_reviewers: list[str],
    ) -> list[str]:
        if "security" not in policy_decision["required_approvals"]:
            return []
        return ["security"] if approved_security_reviewers else []

    @staticmethod
    def _pending_security_approvals(
        policy_decision: dict[str, Any],
        approvals_granted: list[str],
    ) -> list[str]:
        required = {"security"} if "security" in policy_decision["required_approvals"] else set()
        return sorted(required - set(approvals_granted))

    @staticmethod
    def _downstream_approvals(policy_decision: dict[str, Any]) -> list[str]:
        return [
            approval
            for approval in policy_decision["required_approvals"]
            if approval != "security"
        ]

    def _threat_domains(
        self,
        spec_packet: dict[str, Any],
        ticket_bundle: dict[str, Any],
        prompt_contract: dict[str, Any],
        tool_schema: dict[str, Any],
        golden_dataset: dict[str, Any],
        least_privilege_validation: dict[str, Any],
    ) -> list[dict[str, Any]]:
        factors = {factor["name"] for factor in spec_packet["risk_profile"]["factors"]}
        data_classification = spec_packet["risk_profile"]["data_classification"]
        context_kinds = {
            source["kind"] for source in prompt_contract["context_assembly"]["sources"]
        }
        guardrail_text = " ".join(prompt_contract["context_assembly"]["guardrails"]).lower()
        output_checks = set(prompt_contract["output_validation"]["checks"])
        failure_tags = {
            tag
            for case in golden_dataset["failure_injection_cases"]
            for tag in case["tags"]
        }
        security_ticket_present = any(
            ticket["kind"] == "security" for ticket in ticket_bundle["tickets"]
        )
        risky_secret_scope = any(
            ticket["secret_scope"] in {"scoped_write", "elevated"}
            for ticket in ticket_bundle["tickets"]
        )
        tool_count = len(tool_schema["tools"])
        risky_tools = least_privilege_validation["status"] == "failed"

        domains: list[dict[str, Any]] = []

        auth_blocked = "auth_or_permissions" in factors and not security_ticket_present
        domains.append(
            self._domain(
                "auth_and_authorization",
                "high" if "auth_or_permissions" in factors else "low",
                "blocked" if auth_blocked else "watch" if "auth_or_permissions" in factors else "mitigated",
                (
                    ["Auth or permission changes require a dedicated security implementation slice."]
                    if auth_blocked
                    else ["Auth or permission changes are explicitly represented in ticket scope."]
                    if "auth_or_permissions" in factors
                    else ["No auth or permission boundary changes were detected."]
                ),
                (
                    ["Keep the security-scoped ticket in the implementation set."]
                    if "auth_or_permissions" in factors
                    else ["No additional auth mitigation is required for this change."]
                ),
            )
        )

        secrets_blocked = risky_secret_scope or risky_tools
        domains.append(
            self._domain(
                "secrets_handling",
                "high" if secrets_blocked else "low",
                "blocked" if secrets_blocked else "mitigated",
                (
                    least_privilege_validation["findings"]
                    if secrets_blocked
                    else ["No elevated secret or write scopes were introduced."]
                ),
                (
                    ["Keep tool permissions read-only and fail closed on implicit access."]
                    if not secrets_blocked
                    else ["Reduce permission scope or add explicit approval for elevated access."]
                ),
            )
        )

        sensitive_data = data_classification in {"customer_confidential", "regulated"} or (
            "sensitive_data_access" in factors
        )
        domains.append(
            self._domain(
                "stored_user_data",
                "critical" if data_classification == "regulated" else "high" if sensitive_data else "low",
                "watch" if sensitive_data else "mitigated",
                (
                    [f"Data classification is {data_classification}; storage and access paths stay in scope."]
                    if sensitive_data
                    else ["No sensitive storage or regulated data handling changes were detected."]
                ),
                (
                    ["Retain feature-flagged rollout plus explicit data-access review."]
                    if sensitive_data
                    else ["No additional stored-data mitigation is required."]
                ),
            )
        )

        prompt_injection_blocked = (
            "prompt-injection" not in guardrail_text
            or "prompt_injection_scan" not in output_checks
            or "adversarial" not in failure_tags
        )
        domains.append(
            self._domain(
                "prompt_injection",
                "medium",
                "blocked" if prompt_injection_blocked else "mitigated",
                (
                    ["Prompt-injection controls are missing from guardrails, output validation, or abuse cases."]
                    if prompt_injection_blocked
                    else ["Prompt-injection controls are present in guardrails, validation, and failure cases."]
                ),
                ["Reject or strip hostile content before it reaches control instructions."],
            )
        )

        context_exfiltration_blocked = (
            data_classification != "public"
            and "prompt_injection_scan" not in output_checks
        )
        domains.append(
            self._domain(
                "context_exfiltration",
                "high" if data_classification != "public" else "medium",
                "blocked" if context_exfiltration_blocked else "watch" if "conversation_memory" in context_kinds else "mitigated",
                (
                    ["Sensitive context can reach the model without an explicit exfiltration scan."]
                    if context_exfiltration_blocked
                    else ["Context assembly keeps validation and permission guardrails in place."]
                ),
                ["Keep prompt-injection and schema validation checks enabled on all model outputs."],
            )
        )

        jailbreak_watch = "adversarial" not in failure_tags
        domains.append(
            self._domain(
                "jailbreak_resistance",
                "medium",
                "watch" if jailbreak_watch else "mitigated",
                (
                    ["Adversarial coverage is thinner than the default security bar."]
                    if jailbreak_watch
                    else ["Golden failure cases cover jailbreak-style and hostile instruction attempts."]
                ),
                ["Preserve adversarial probes alongside the golden dataset."],
            )
        )

        tool_invocation_status = (
            "blocked"
            if least_privilege_validation["status"] == "failed"
            else "watch"
            if least_privilege_validation["status"] == "needs_human_review"
            else "mitigated"
        )
        domains.append(
            self._domain(
                "unsafe_tool_invocation",
                "high" if tool_count and tool_invocation_status != "mitigated" else "medium" if tool_count else "low",
                tool_invocation_status,
                (
                    least_privilege_validation["findings"]
                    if tool_count
                    else ["No callable tools are in scope for this change."]
                ),
                (
                    ["Constrain the model to explicit tool IDs with least-privilege permissions."]
                    if tool_count
                    else ["No tool-invocation mitigation is required."]
                ),
            )
        )

        return domains

    @staticmethod
    def _domain(
        name: str,
        severity: str,
        status: str,
        findings: list[str],
        mitigations: list[str],
    ) -> dict[str, Any]:
        return {
            "name": name,
            "severity": severity,
            "status": status,
            "findings": findings,
            "mitigations": mitigations,
        }

    @staticmethod
    def _abuse_cases(golden_dataset: dict[str, Any]) -> list[dict[str, Any]]:
        abuse_cases: list[dict[str, Any]] = []
        for case in golden_dataset["failure_injection_cases"]:
            severity = "high" if {"adversarial", "safety"} & set(case["tags"]) else "medium"
            abuse_cases.append(
                {
                    "id": case["id"],
                    "title": case["attack_or_failure"],
                    "severity": severity,
                    "expected_control": case["expected_outcome"],
                }
            )
        return abuse_cases

    @staticmethod
    def _least_privilege_validation(
        tool_schema: dict[str, Any],
        prompt_contract: dict[str, Any],
    ) -> dict[str, Any]:
        allowed_tool_ids = list(prompt_contract["tool_choice_policy"]["allowed_tool_ids"])
        tool_ids = [tool["id"] for tool in tool_schema["tools"]]
        findings: list[str] = []
        risky_permissions: list[str] = []
        for tool in tool_schema["tools"]:
            for permission in tool["permissions"]:
                normalized = permission.lower()
                if normalized in LOW_RISK_TOOL_PERMISSIONS or normalized.startswith("read"):
                    continue
                if any(token in normalized for token in ("write", "delete", "admin", "elevated")):
                    risky_permissions.append(f"{tool['id']} requires '{permission}'.")
        if sorted(allowed_tool_ids) != sorted(tool_ids):
            findings.append("Prompt contract allowed_tool_ids does not match the tool schema.")
        findings.extend(risky_permissions)
        if risky_permissions:
            status = "failed"
        elif findings:
            status = "needs_human_review"
        else:
            status = "passed"
            findings = ["Tool invocation is constrained to explicit least-privilege permissions."]
        return {
            "status": status,
            "allowed_tool_ids": allowed_tool_ids,
            "findings": findings,
        }

    def _security_checks(
        self,
        spec_packet: dict[str, Any],
        policy_decision: dict[str, Any],
        pr_packet: dict[str, Any],
        prompt_contract: dict[str, Any],
        golden_dataset: dict[str, Any],
        least_privilege_validation: dict[str, Any],
        approvals_granted: list[str],
        explicit_blocking_findings: list[str],
    ) -> list[dict[str, Any]]:
        output_checks = set(prompt_contract["output_validation"]["checks"])
        guardrail_text = " ".join(prompt_contract["context_assembly"]["guardrails"]).lower()
        failure_tags = {
            tag
            for case in golden_dataset["failure_injection_cases"]
            for tag in case["tags"]
        }
        checks = [
            {
                "id": "sec-least-privilege",
                "name": "Least privilege tool validation",
                "status": least_privilege_validation["status"],
                "severity": "high",
                "summary": "; ".join(least_privilege_validation["findings"]),
            },
            {
                "id": "sec-prompt-injection",
                "name": "Prompt injection coverage",
                "status": (
                    "passed"
                    if "prompt-injection" in guardrail_text
                    and "prompt_injection_scan" in output_checks
                    and "adversarial" in failure_tags
                    else "failed"
                ),
                "severity": "high",
                "summary": (
                    "Prompt-injection controls are present in guardrails, validation, and abuse cases."
                    if "prompt-injection" in guardrail_text
                    and "prompt_injection_scan" in output_checks
                    and "adversarial" in failure_tags
                    else "Prompt-injection coverage is missing from one of the required control surfaces."
                ),
            },
            {
                "id": "sec-context-exfiltration",
                "name": "Context exfiltration controls",
                "status": (
                    "passed"
                    if "tool_result_schema_validation" in output_checks
                    and pr_packet["summary"]["rollback_notes"]
                    else "failed"
                ),
                "severity": "medium",
                "summary": (
                    "Context assembly and rollback documentation remain bounded."
                    if "tool_result_schema_validation" in output_checks
                    and pr_packet["summary"]["rollback_notes"]
                    else "Context validation or rollback documentation is incomplete."
                ),
            },
            {
                "id": "sec-rollback-blast-radius",
                "name": "Rollback and blast-radius documentation",
                "status": (
                    "passed"
                    if spec_packet["risk_profile"]["blast_radius"] and pr_packet["summary"]["rollback_notes"]
                    else "failed"
                ),
                "severity": "medium",
                "summary": (
                    "Blast radius and rollback notes are documented."
                    if spec_packet["risk_profile"]["blast_radius"] and pr_packet["summary"]["rollback_notes"]
                    else "Blast radius or rollback notes are missing."
                ),
            },
            {
                "id": "sec-signoff-gate",
                "name": "Security sign-off gate",
                "status": (
                    "passed"
                    if "security" not in policy_decision["required_approvals"]
                    or "security" in approvals_granted
                    else "needs_human_review"
                ),
                "severity": "high",
                "summary": (
                    "No additional human security sign-off is required."
                    if "security" not in policy_decision["required_approvals"]
                    else "Security approval has been granted."
                    if "security" in approvals_granted
                    else "Policy requires explicit human security sign-off."
                ),
            },
        ]
        if explicit_blocking_findings:
            checks.append(
                {
                    "id": "sec-manual-blockers",
                    "name": "Explicit security blocking findings",
                    "status": "failed",
                    "severity": "critical",
                    "summary": "; ".join(explicit_blocking_findings),
                }
            )
        return checks

    @staticmethod
    def _blocking_findings(
        threat_domains: list[dict[str, Any]],
        security_checks: list[dict[str, Any]],
    ) -> list[str]:
        findings: list[str] = []
        for domain in threat_domains:
            if domain["status"] == "blocked":
                findings.extend(domain["findings"])
        for check in security_checks:
            if check["status"] == "failed" and check["severity"] in {"high", "critical"}:
                findings.append(f"{check['name']} failed: {check['summary']}")
        return findings

    @staticmethod
    def _watch_findings(
        threat_domains: list[dict[str, Any]],
        security_checks: list[dict[str, Any]],
    ) -> list[str]:
        findings: list[str] = []
        for domain in threat_domains:
            if domain["status"] == "watch":
                findings.extend(domain["findings"])
        for check in security_checks:
            if check["status"] == "needs_human_review":
                findings.append(f"{check['name']} requires human review: {check['summary']}")
        return SecuritySentinel._dedupe(findings)

    @staticmethod
    def _signoff(
        *,
        blocking_summary: list[str],
        pending_approvals: list[str],
        approvals_granted: list[str],
        downstream_approvals: list[str],
    ) -> dict[str, Any]:
        if blocking_summary:
            return {
                "status": "blocked",
                "mode": "auto",
                "approvals_granted": approvals_granted,
                "pending_approvals": pending_approvals,
                "downstream_approvals": downstream_approvals,
                "rationale": "Security review found blocking issues that must be fixed before merge.",
            }
        if pending_approvals:
            return {
                "status": "pending_human",
                "mode": "human_required",
                "approvals_granted": approvals_granted,
                "pending_approvals": pending_approvals,
                "downstream_approvals": downstream_approvals,
                "rationale": "Policy requires explicit human security sign-off before release promotion can continue.",
            }
        return {
            "status": "approved",
            "mode": "human_approved" if approvals_granted else "auto",
            "approvals_granted": approvals_granted,
            "pending_approvals": [],
            "downstream_approvals": downstream_approvals,
            "rationale": (
                "Security controls passed and the required human security sign-off was granted."
                if approvals_granted
                else "Security controls passed without requiring explicit human intervention."
            ),
        }

    @staticmethod
    def _artifact_status(signoff_status: str) -> str:
        if signoff_status == "approved":
            return "approved"
        if signoff_status == "pending_human":
            return "ready"
        return "blocked"

    @staticmethod
    def _next_stage(signoff_status: str) -> str:
        if signoff_status == "approved":
            return "release_staging"
        if signoff_status == "pending_human":
            return "human_security_signoff"
        return "build"

    @staticmethod
    def _dedupe(items: list[str]) -> list[str]:
        deduped: list[str] = []
        for item in items:
            if item not in deduped:
                deduped.append(item)
        return deduped


class Stage6SecurityReviewPipeline:
    """Run threat analysis and security sign-off for a mergeable PR candidate."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        controller: FactoryController | None = None,
        security_sentinel: SecuritySentinel | None = None,
    ) -> None:
        self.root = repo_root(root)
        self.controller = controller or FactoryController()
        self.security_sentinel = security_sentinel or SecuritySentinel()
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
        work_item: WorkItem,
        *,
        security_review_id: str | None = None,
        approved_security_reviewers: list[str] | None = None,
        blocking_findings: list[str] | None = None,
    ) -> Stage6SecurityReviewResult:
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
            work_item,
        )

        timestamp = utc_now()
        evaluated_pr_artifact_version = int(pr_packet["artifact"]["version"])
        artifact_id = security_review_id or self._default_security_review_id(
            spec_packet["artifact"]["id"],
            work_item.attempt_count,
            evaluated_pr_artifact_version,
        )
        security_review = self.security_sentinel.build_security_review(
            spec_packet,
            policy_decision,
            ticket_bundle,
            pr_packet,
            eval_report,
            prompt_contract,
            tool_schema,
            golden_dataset,
            artifact_id=artifact_id,
            build_attempt=work_item.attempt_count,
            approved_security_reviewers=approved_security_reviewers,
            blocking_findings=blocking_findings,
            timestamp=timestamp,
        )
        updated_pr_packet = self.security_sentinel.finalize_pr_packet(
            pr_packet,
            security_review,
            timestamp=timestamp,
        )

        self._validate_document("security-review", security_review)
        self._validate_document("pr-packet", updated_pr_packet)
        self._validate_generated_consistency(
            updated_pr_packet,
            eval_report,
            security_review,
            work_item,
        )

        working_item = deepcopy(work_item)
        if work_item.state is ControllerState.PR_MERGEABLE:
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.SECURITY_REVIEW_STARTED,
                artifact_id=security_review["artifact"]["id"],
                occurred_at=security_review["artifact"]["created_at"],
            )
        signoff_status = security_review["signoff"]["status"]
        if signoff_status == "blocked":
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.SECURITY_FINDINGS_PRESENT,
                artifact_id=security_review["artifact"]["id"],
                occurred_at=security_review["artifact"]["updated_at"],
            )
        elif signoff_status == "approved":
            if working_item.state is not ControllerState.SECURITY_REVIEWING:
                self.controller.apply_event(
                    working_item,
                    event=ControllerEvent.SECURITY_REVIEW_STARTED,
                    artifact_id=security_review["artifact"]["id"],
                    occurred_at=security_review["artifact"]["created_at"],
                )
            self.controller.apply_event(
                working_item,
                event=ControllerEvent.SECURITY_SIGNOFF_GRANTED,
                artifact_id=security_review["artifact"]["id"],
                occurred_at=security_review["artifact"]["updated_at"],
            )
        elif signoff_status == "pending_human":
            working_item.current_artifact_id = security_review["artifact"]["id"]
            working_item.updated_at = security_review["artifact"]["updated_at"]
        self._validate_document("work-item", working_item.to_document())

        return Stage6SecurityReviewResult(
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
            work_item=working_item,
        )

    def _validate_document(self, schema_name: str, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.validators[schema_name], document)
        if errors:
            raise SecurityReviewError(
                f"{schema_name} failed validation: {'; '.join(errors)}"
            )

    @staticmethod
    def _default_security_review_id(
        spec_packet_id: str,
        build_attempt: int,
        evaluated_pr_artifact_version: int,
    ) -> str:
        return build_identifier(
            "security-review",
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
        work_item: WorkItem,
    ) -> None:
        spec_packet_id = spec_packet["artifact"]["id"]
        policy_artifact = policy_decision["artifact"]
        lane = policy_decision["lane_assignment"]["lane"]

        if policy_decision["decision"] != "active_build_candidate":
            raise SecurityReviewEligibilityError(
                "Only active_build_candidate items can enter Stage 6 security review."
            )
        if work_item.state not in {
            ControllerState.PR_MERGEABLE,
            ControllerState.SECURITY_REVIEWING,
        }:
            raise SecurityReviewEligibilityError(
                "Work item must be in PR_MERGEABLE or SECURITY_REVIEWING before Stage 6; "
                f"got {work_item.state.value}."
            )
        if work_item.attempt_count < 1:
            raise SecurityReviewEligibilityError(
                "Work item must have at least one build attempt before Stage 6 runs."
            )
        if work_item.state is ControllerState.PR_MERGEABLE and work_item.current_artifact_id != eval_report["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "work-item current_artifact_id must match the eval-report when entering Stage 6 from PR_MERGEABLE."
            )
        if work_item.state is ControllerState.SECURITY_REVIEWING and not work_item.current_artifact_id:
            raise SecurityReviewConsistencyError(
                "work-item in SECURITY_REVIEWING must retain the previous security artifact id."
            )
        if work_item.policy_decision_id != policy_artifact["id"]:
            raise SecurityReviewConsistencyError(
                "work-item policy_decision_id does not match the policy decision artifact."
            )
        if work_item.execution_lane != lane:
            raise SecurityReviewConsistencyError(
                "work-item execution_lane does not match the policy lane."
            )
        if work_item.risk_score != policy_decision["risk_score"]:
            raise SecurityReviewConsistencyError(
                "work-item risk_score does not match the policy decision."
            )
        if work_item.source_provider != spec_packet["source"]["provider"]:
            raise SecurityReviewConsistencyError(
                "work-item source_provider does not match the provided spec-packet."
            )
        if work_item.source_external_id != spec_packet["source"]["external_id"]:
            raise SecurityReviewConsistencyError(
                "work-item source_external_id does not match the provided spec-packet."
            )
        if policy_decision["spec_packet_id"] != spec_packet_id:
            raise SecurityReviewConsistencyError(
                "policy-decision does not reference the provided spec-packet."
            )
        if ticket_bundle["spec_packet_id"] != spec_packet_id:
            raise SecurityReviewConsistencyError(
                "ticket-bundle does not reference the provided spec-packet."
            )
        if ticket_bundle["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "ticket-bundle eval_manifest_id does not match the provided eval-manifest."
            )
        if eval_manifest["target_id"] != ticket_bundle["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "eval-manifest target_id does not match the provided ticket-bundle."
            )
        if pr_packet["spec_packet_id"] != spec_packet_id:
            raise SecurityReviewConsistencyError(
                "pr-packet spec_packet_id does not match the provided spec-packet."
            )
        if pr_packet["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "pr-packet eval_manifest_id does not match the provided eval-manifest."
            )
        if prompt_contract["spec_packet_id"] != spec_packet_id:
            raise SecurityReviewConsistencyError(
                "prompt-contract spec_packet_id does not match the provided spec-packet."
            )
        if prompt_contract["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "prompt-contract pr_packet_id does not match the provided pr-packet."
            )
        if tool_schema["spec_packet_id"] != spec_packet_id:
            raise SecurityReviewConsistencyError(
                "tool-schema spec_packet_id does not match the provided spec-packet."
            )
        if tool_schema["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "tool-schema prompt_contract_id does not match the provided prompt contract."
            )
        if golden_dataset["spec_packet_id"] != spec_packet_id:
            raise SecurityReviewConsistencyError(
                "golden-dataset spec_packet_id does not match the provided spec-packet."
            )
        if golden_dataset["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "golden-dataset prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["spec_packet_id"] != spec_packet_id:
            raise SecurityReviewConsistencyError(
                "latency-baseline spec_packet_id does not match the provided spec-packet."
            )
        if latency_baseline["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "latency-baseline prompt_contract_id does not match the provided prompt contract."
            )
        if latency_baseline["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "latency-baseline pr_packet_id does not match the provided pr-packet."
            )
        if eval_report["spec_packet_id"] != spec_packet_id:
            raise SecurityReviewConsistencyError(
                "eval-report spec_packet_id does not match the provided spec-packet."
            )
        if eval_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "eval-report pr_packet_id does not match the provided pr-packet."
            )
        if eval_report["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "eval-report prompt_contract_id does not match the provided prompt contract."
            )
        if eval_report["tool_schema_id"] != tool_schema["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "eval-report tool_schema_id does not match the provided tool schema."
            )
        if eval_report["golden_dataset_id"] != golden_dataset["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "eval-report golden_dataset_id does not match the provided golden dataset."
            )
        if eval_report["latency_baseline_id"] != latency_baseline["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "eval-report latency_baseline_id does not match the provided latency baseline."
            )
        if not eval_report["summary"]["merge_gate_passed"]:
            raise SecurityReviewEligibilityError(
                "Stage 6 only runs after merge-gating eval tiers have passed."
            )
        if work_item.attempt_count != eval_report["build_attempt"]:
            raise SecurityReviewConsistencyError(
                "work-item attempt_count does not match the eval-report build attempt."
            )
        if pr_packet["artifact"]["version"] < eval_report["resulting_pr_artifact_version"]:
            raise SecurityReviewConsistencyError(
                "pr-packet version cannot be earlier than the eval-report resulting version."
            )
        if work_item.state is ControllerState.PR_MERGEABLE and not pr_packet["merge_readiness"]["mergeable"]:
            raise SecurityReviewEligibilityError(
                "pr-packet must be mergeable when Stage 6 starts from PR_MERGEABLE."
            )
        if work_item.state is ControllerState.SECURITY_REVIEWING and not pr_packet["merge_readiness"]["reviewable"]:
            raise SecurityReviewEligibilityError(
                "pr-packet must remain reviewable while waiting on security sign-off."
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
        ):
            if artifact["policy_decision_id"] != policy_artifact["id"]:
                raise SecurityReviewConsistencyError(
                    f"{artifact_name} policy_decision_id does not match the policy decision artifact."
                )
            if artifact["execution_lane"] != lane:
                raise SecurityReviewConsistencyError(
                    f"{artifact_name} execution_lane does not match the policy lane."
                )
            if artifact["risk_tier"] != policy_artifact["risk_tier"]:
                raise SecurityReviewConsistencyError(
                    f"{artifact_name} risk_tier does not match the policy decision artifact."
                )
            if artifact["budget_class"] != policy_artifact["budget_class"]:
                raise SecurityReviewConsistencyError(
                    f"{artifact_name} budget_class does not match the policy decision artifact."
                )
            if artifact["rollback_class"] != policy_artifact["rollback_class"]:
                raise SecurityReviewConsistencyError(
                    f"{artifact_name} rollback_class does not match the policy decision artifact."
                )
            if sorted(artifact["approval_requirements"]) != sorted(
                policy_artifact["approval_requirements"]
            ):
                raise SecurityReviewConsistencyError(
                    f"{artifact_name} approval_requirements do not match the policy decision artifact."
                )

    @staticmethod
    def _validate_generated_consistency(
        pr_packet: dict[str, Any],
        eval_report: dict[str, Any],
        security_review: dict[str, Any],
        work_item: WorkItem,
    ) -> None:
        if security_review["pr_packet_id"] != pr_packet["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "security-review pr_packet_id does not match the updated pr-packet artifact."
            )
        if security_review["eval_report_id"] != eval_report["artifact"]["id"]:
            raise SecurityReviewConsistencyError(
                "security-review eval_report_id does not match the provided eval-report."
            )
        if security_review["build_attempt"] != work_item.attempt_count:
            raise SecurityReviewConsistencyError(
                "security-review build_attempt does not match the work-item attempt count."
            )
        if security_review["evaluated_pr_artifact_version"] + 1 != security_review["resulting_pr_artifact_version"]:
            raise SecurityReviewConsistencyError(
                "security-review resulting_pr_artifact_version must be exactly one greater than the evaluated version."
            )
        if security_review["resulting_pr_artifact_version"] != pr_packet["artifact"]["version"]:
            raise SecurityReviewConsistencyError(
                "security-review resulting_pr_artifact_version does not match the updated pr-packet version."
            )

        signoff_status = security_review["signoff"]["status"]
        if signoff_status == "approved":
            if not pr_packet["merge_readiness"]["mergeable"]:
                raise SecurityReviewConsistencyError(
                    "approved security review must leave the pr-packet mergeable."
                )
        elif signoff_status == "pending_human":
            if pr_packet["merge_readiness"]["mergeable"]:
                raise SecurityReviewConsistencyError(
                    "pending human security sign-off cannot leave the pr-packet mergeable."
                )
            if not pr_packet["merge_readiness"]["reviewable"]:
                raise SecurityReviewConsistencyError(
                    "pending human security sign-off must keep the pr-packet reviewable."
                )
        else:
            if pr_packet["merge_readiness"]["mergeable"]:
                raise SecurityReviewConsistencyError(
                    "blocked security review cannot leave the pr-packet mergeable."
                )
            if pr_packet["reviewer_report"]["approved"]:
                raise SecurityReviewConsistencyError(
                    "blocked security review cannot leave the reviewer_report approved."
                )
