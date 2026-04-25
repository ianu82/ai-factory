from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from .controller import FactoryController
from .eval_common import deferred_tiers, merge_gate_tiers


CONTROLLER_SCENARIO_ARTIFACT_ORDER = [
    "spec-packet",
    "policy-decision",
    "ticket-bundle",
    "eval-manifest",
    "pr-packet",
    "eval-report",
    "security-review",
    "merge-decision",
    "promotion-decision",
    "monitoring-report",
    "feedback-report",
]

POLICY_BOUND_ARTIFACTS = (
    "ticket-bundle",
    "eval-manifest",
    "pr-packet",
    "prompt-contract",
    "tool-schema",
    "golden-dataset",
    "latency-baseline",
    "eval-report",
    "security-review",
    "merge-decision",
    "promotion-decision",
    "monitoring-report",
    "feedback-report",
)

STAGE4_ARTIFACTS = (
    "prompt-contract",
    "tool-schema",
    "golden-dataset",
    "latency-baseline",
)

STAGE5_ARTIFACTS = ("eval-report",)

STAGE6_ARTIFACTS = ("security-review",)

STAGE_MERGE_ARTIFACTS = ("merge-decision",)

STAGE7_ARTIFACTS = ("promotion-decision",)

STAGE8_ARTIFACTS = ("monitoring-report",)

STAGE9_ARTIFACTS = ("feedback-report",)

POLICY_SCHEMA_TARGETS = {
    "relevance-policy": Path("factory/policies/relevance.yaml"),
}

OPEN_MONITORING_INCIDENT_STAGES = {"feedback_synthesis", "human_incident_response"}


def repo_root(root: Path | None = None) -> Path:
    if root is not None:
        return root.resolve()
    return Path(__file__).resolve().parents[2]


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_validators(root: Path) -> dict[str, Draft202012Validator]:
    schema_dir = root / "schemas"
    registry = Registry()
    schema_documents: dict[str, dict[str, Any]] = {}

    for schema_path in sorted(schema_dir.glob("*.schema.json")):
        document = load_json(schema_path)
        Draft202012Validator.check_schema(document)
        schema_name = schema_path.name.removesuffix(".schema.json")
        schema_documents[schema_name] = document
        registry = registry.with_resource(document["$id"], Resource.from_contents(document))

    return {
        schema_name: Draft202012Validator(document, registry=registry)
        for schema_name, document in schema_documents.items()
    }


def validation_errors_for(
    validator: Draft202012Validator,
    instance: dict[str, Any],
) -> list[str]:
    errors = sorted(validator.iter_errors(instance), key=lambda error: error.json_path)
    return [f"{error.json_path or '$'}: {error.message}" for error in errors]


def has_open_monitoring_incident(pr_packet: dict[str, Any]) -> bool:
    artifact = pr_packet["artifact"]
    return (
        artifact["owner_agent"] == "SRE Sentinel"
        and artifact["status"] == "blocked"
        and artifact["next_stage"] in OPEN_MONITORING_INCIDENT_STAGES
    )


def validate_schema_fixture(
    validators: dict[str, Draft202012Validator],
    schema_name: str,
    fixture_path: Path,
) -> list[str]:
    if schema_name not in validators:
        return [f"{fixture_path}: no validator found for schema '{schema_name}'"]
    instance = load_json(fixture_path)
    return validation_errors_for(validators[schema_name], instance)


def validate_policy_documents(
    validators: dict[str, Draft202012Validator],
    root: Path,
) -> list[str]:
    errors: list[str] = []
    for schema_name, relative_path in POLICY_SCHEMA_TARGETS.items():
        if schema_name not in validators:
            errors.append(f"{relative_path}: no validator found for policy schema '{schema_name}'")
            continue
        document_path = root / relative_path
        if not document_path.exists():
            errors.append(f"{document_path}: policy file is missing")
            continue
        document = load_yaml(document_path)
        for message in validation_errors_for(validators[schema_name], document):
            errors.append(f"{document_path}: {message}")
    return errors


def validate_invalid_fixtures(
    validators: dict[str, Draft202012Validator],
    invalid_dir: Path,
) -> list[str]:
    errors: list[str] = []
    if not invalid_dir.exists():
        return errors

    for fixture_path in sorted(invalid_dir.glob("*.json")):
        schema_name = fixture_path.name.split(".", 1)[0]
        if schema_name not in validators:
            errors.append(
                f"{fixture_path}: no validator found for invalid fixture schema '{schema_name}'"
            )
            continue
        fixture_errors = validate_schema_fixture(validators, schema_name, fixture_path)
        if not fixture_errors:
            errors.append(
                f"{fixture_path}: expected schema validation to fail, but it passed"
            )
    return errors


def highest_artifact_id(documents: dict[str, dict[str, Any]]) -> str | None:
    for artifact_name in reversed(CONTROLLER_SCENARIO_ARTIFACT_ORDER):
        if artifact_name in documents:
            return documents[artifact_name]["artifact"]["id"]
    return None


def validate_policy_alignment(
    scenario_name: str,
    documents: dict[str, dict[str, Any]],
    lane_policy: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    spec_packet = documents.get("spec-packet")
    policy_decision = documents.get("policy-decision")
    work_item = documents.get("work-item")

    if spec_packet and policy_decision:
        if spec_packet["relevance"]["decision"] != policy_decision["decision"]:
            errors.append(
                f"{scenario_name}: spec-packet decision and policy-decision decision differ"
            )
        if spec_packet["risk_profile"]["risk_score"] != policy_decision["risk_score"]:
            errors.append(
                f"{scenario_name}: spec-packet risk score and policy-decision risk score differ"
            )

    if policy_decision and policy_decision["decision"] in {
        "backlog_candidate",
        "active_build_candidate",
    }:
        lane = policy_decision["lane_assignment"]["lane"]
        lane_defaults = lane_policy["lanes"][lane]
        default_approvals = sorted(lane_defaults["default_required_approvals"])
        policy_artifact = policy_decision["artifact"]

        if sorted(policy_decision["required_eval_tiers"]) != sorted(
            lane_defaults["required_eval_tiers"]
        ):
            errors.append(
                f"{scenario_name}: policy-decision required_eval_tiers do not match lane policy"
            )
        if sorted(policy_decision["required_approvals"]) != default_approvals:
            errors.append(
                f"{scenario_name}: policy-decision required_approvals do not match lane defaults"
            )
        if policy_decision["budget_policy"] != lane_defaults["build_limits"]:
            errors.append(
                f"{scenario_name}: policy-decision budget_policy does not match lane build limits"
            )
        if policy_decision["deployment_policy"] != lane_defaults["release_policy"]:
            errors.append(
                f"{scenario_name}: policy-decision deployment_policy does not match lane release policy"
            )
        if policy_artifact["execution_lane"] != lane:
            errors.append(
                f"{scenario_name}: policy-decision artifact execution_lane does not match lane_assignment"
            )
        if policy_artifact["budget_class"] != budget_class_from_lane(lane_defaults):
            errors.append(
                f"{scenario_name}: policy-decision artifact budget_class does not match lane policy"
            )
        if policy_artifact["rollback_class"] != lane_defaults["rollback_class"]:
            errors.append(
                f"{scenario_name}: policy-decision artifact rollback_class does not match lane policy"
            )
        if sorted(policy_artifact["approval_requirements"]) != default_approvals:
            errors.append(
                f"{scenario_name}: policy-decision artifact approval_requirements do not match lane defaults"
            )

        policy_artifact_id = policy_decision["artifact"]["id"]
        for artifact_name in POLICY_BOUND_ARTIFACTS:
            if artifact_name not in documents:
                continue
            artifact = documents[artifact_name]["artifact"]
            if artifact["execution_lane"] != lane:
                errors.append(
                    f"{scenario_name}: {artifact_name} execution_lane does not match policy decision"
                )
            if artifact["policy_decision_id"] != policy_artifact_id:
                errors.append(
                    f"{scenario_name}: {artifact_name} policy_decision_id does not match policy artifact id"
                )
            if artifact["risk_tier"] != policy_artifact["risk_tier"]:
                errors.append(
                    f"{scenario_name}: {artifact_name} risk_tier does not match policy decision"
                )
            if artifact["budget_class"] != policy_artifact["budget_class"]:
                errors.append(
                    f"{scenario_name}: {artifact_name} budget_class does not match policy decision"
                )
            if artifact["rollback_class"] != policy_artifact["rollback_class"]:
                errors.append(
                    f"{scenario_name}: {artifact_name} rollback_class does not match policy decision"
                )
            if sorted(artifact["approval_requirements"]) != default_approvals:
                errors.append(
                    f"{scenario_name}: {artifact_name} approval_requirements do not match lane defaults"
                )

        if "ticket-bundle" in documents:
            ticket_bundle = documents["ticket-bundle"]
            if ticket_bundle["spec_packet_id"] != spec_packet["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: ticket-bundle spec_packet_id does not match spec-packet artifact id"
                )
            for ticket in ticket_bundle["tickets"]:
                if ticket["execution_lane"] != lane:
                    errors.append(
                        f"{scenario_name}: ticket {ticket['id']} execution_lane does not match policy lane"
                    )
                if sorted(ticket["required_eval_tiers"]) != sorted(
                    policy_decision["required_eval_tiers"]
                ):
                    errors.append(
                        f"{scenario_name}: ticket {ticket['id']} eval tiers do not match policy"
                    )

        if "ticket-bundle" in documents and "eval-manifest" not in documents:
            errors.append(
                f"{scenario_name}: ticket-bundle is present but eval-manifest is missing"
            )

        if "eval-manifest" in documents:
            eval_manifest = documents["eval-manifest"]
            manifest_tiers = [tier["name"] for tier in eval_manifest["tiers"]]
            if sorted(manifest_tiers) != sorted(policy_decision["required_eval_tiers"]):
                errors.append(
                    f"{scenario_name}: eval-manifest tiers do not match policy-decision tiers"
                )
            if "ticket-bundle" in documents:
                ticket_bundle_id = documents["ticket-bundle"]["artifact"]["id"]
                if documents["ticket-bundle"]["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: ticket-bundle eval_manifest_id does not match eval-manifest artifact id"
                    )
                if eval_manifest["target_type"] != "ticket_bundle":
                    errors.append(
                        f"{scenario_name}: eval-manifest target_type should be ticket_bundle"
                    )
                if eval_manifest["target_id"] != ticket_bundle_id:
                    errors.append(
                        f"{scenario_name}: eval-manifest target_id does not match ticket-bundle artifact id"
                    )

        if "pr-packet" in documents and "ticket-bundle" in documents:
            pr_packet = documents["pr-packet"]
            ticket_ids = {ticket["id"] for ticket in documents["ticket-bundle"]["tickets"]}
            if pr_packet["spec_packet_id"] != spec_packet["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: pr-packet spec_packet_id does not match spec-packet artifact id"
                )
            if "eval-manifest" in documents and pr_packet["eval_manifest_id"] != documents["eval-manifest"]["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: pr-packet eval_manifest_id does not match eval-manifest artifact id"
                )
            if not set(pr_packet["ticket_ids"]).issubset(ticket_ids):
                errors.append(
                    f"{scenario_name}: pr-packet ticket_ids are not a subset of ticket bundle ids"
                )

        present_stage4 = [artifact_name for artifact_name in STAGE4_ARTIFACTS if artifact_name in documents]
        if present_stage4 and len(present_stage4) != len(STAGE4_ARTIFACTS):
            missing = ", ".join(
                artifact_name for artifact_name in STAGE4_ARTIFACTS if artifact_name not in documents
            )
            errors.append(
                f"{scenario_name}: Stage 4 scenario is missing required integration artifacts: {missing}"
            )

        if all(artifact_name in documents for artifact_name in STAGE4_ARTIFACTS):
            prompt_contract = documents["prompt-contract"]
            tool_schema = documents["tool-schema"]
            golden_dataset = documents["golden-dataset"]
            latency_baseline = documents["latency-baseline"]
            pr_packet = documents["pr-packet"]

            if prompt_contract["spec_packet_id"] != spec_packet["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: prompt-contract spec_packet_id does not match spec-packet artifact id"
                )
            if tool_schema["spec_packet_id"] != spec_packet["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: tool-schema spec_packet_id does not match spec-packet artifact id"
                )
            if golden_dataset["spec_packet_id"] != spec_packet["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: golden-dataset spec_packet_id does not match spec-packet artifact id"
                )
            if latency_baseline["spec_packet_id"] != spec_packet["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: latency-baseline spec_packet_id does not match spec-packet artifact id"
                )
            if prompt_contract["pr_packet_id"] != pr_packet["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: prompt-contract pr_packet_id does not match pr-packet artifact id"
                )
            if latency_baseline["pr_packet_id"] != pr_packet["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: latency-baseline pr_packet_id does not match pr-packet artifact id"
                )
            if prompt_contract["tool_schema_id"] != tool_schema["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: prompt-contract tool_schema_id does not match tool-schema artifact id"
                )
            if prompt_contract["golden_dataset_id"] != golden_dataset["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: prompt-contract golden_dataset_id does not match golden-dataset artifact id"
                )
            if tool_schema["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: tool-schema prompt_contract_id does not match prompt-contract artifact id"
                )
            if golden_dataset["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: golden-dataset prompt_contract_id does not match prompt-contract artifact id"
                )
            if latency_baseline["prompt_contract_id"] != prompt_contract["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: latency-baseline prompt_contract_id does not match prompt-contract artifact id"
                )

            tool_ids = [tool["id"] for tool in tool_schema["tools"]]
            if len(tool_ids) != len(set(tool_ids)):
                errors.append(f"{scenario_name}: tool-schema contains duplicate tool ids")
            if sorted(prompt_contract["tool_choice_policy"]["allowed_tool_ids"]) != sorted(tool_ids):
                errors.append(
                    f"{scenario_name}: prompt-contract allowed_tool_ids do not match tool-schema ids"
                )
            if tool_ids and prompt_contract["tool_choice_policy"]["mode"] == "none":
                errors.append(
                    f"{scenario_name}: prompt-contract tool choice mode cannot be none when tools exist"
                )
            if not tool_ids and prompt_contract["tool_choice_policy"]["mode"] != "none":
                errors.append(
                    f"{scenario_name}: prompt-contract tool choice mode must be none when no tools exist"
                )

        if "eval-report" in documents:
            if not all(artifact_name in documents for artifact_name in STAGE4_ARTIFACTS):
                missing = ", ".join(
                    artifact_name for artifact_name in STAGE4_ARTIFACTS if artifact_name not in documents
                )
                errors.append(
                    f"{scenario_name}: eval-report requires the Stage 4 artifacts to be present: {missing}"
                )
            eval_report = documents["eval-report"]
            pr_packet = documents.get("pr-packet")
            eval_manifest = documents.get("eval-manifest")
            if pr_packet is None or eval_manifest is None:
                errors.append(
                    f"{scenario_name}: eval-report requires both pr-packet and eval-manifest"
                )
            else:
                if eval_report["spec_packet_id"] != spec_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: eval-report spec_packet_id does not match spec-packet artifact id"
                    )
                if eval_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: eval-report pr_packet_id does not match pr-packet artifact id"
                    )
                if eval_report["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: eval-report eval_manifest_id does not match eval-manifest artifact id"
                    )
                if "prompt-contract" in documents and eval_report["prompt_contract_id"] != documents["prompt-contract"]["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: eval-report prompt_contract_id does not match prompt-contract artifact id"
                    )
                if "tool-schema" in documents and eval_report["tool_schema_id"] != documents["tool-schema"]["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: eval-report tool_schema_id does not match tool-schema artifact id"
                    )
                if "golden-dataset" in documents and eval_report["golden_dataset_id"] != documents["golden-dataset"]["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: eval-report golden_dataset_id does not match golden-dataset artifact id"
                    )
                if "latency-baseline" in documents and eval_report["latency_baseline_id"] != documents["latency-baseline"]["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: eval-report latency_baseline_id does not match latency-baseline artifact id"
                    )

                expected_merge_gate_tiers = set(merge_gate_tiers(eval_manifest))
                expected_deferred_tiers = set(deferred_tiers(eval_manifest))
                summary = eval_report["summary"]
                if set(summary["merge_gate_tiers"]) != expected_merge_gate_tiers:
                    errors.append(
                        f"{scenario_name}: eval-report merge_gate_tiers do not match the merge-gating eval tiers"
                    )
                if set(summary["deferred_tiers"]) != expected_deferred_tiers:
                    errors.append(
                        f"{scenario_name}: eval-report deferred_tiers do not match the deferred eval tiers"
                    )
                if work_item and eval_report["build_attempt"] != work_item["attempt_count"]:
                    errors.append(
                        f"{scenario_name}: eval-report build_attempt does not match work-item attempt_count"
                    )
                if eval_report["evaluated_pr_artifact_version"] + 1 != eval_report["resulting_pr_artifact_version"]:
                    errors.append(
                        f"{scenario_name}: eval-report resulting_pr_artifact_version must be exactly one greater than the evaluated version"
                    )

                status_by_name = {
                    check["name"]: check["status"]
                    for tier in eval_report["tiers"]
                    for check in tier["checks"]
                    if check["status"] not in {"pending", "deferred", "not_configured"}
                }
                for check in pr_packet["checks"]:
                    if check["name"] in status_by_name and check["status"] != status_by_name[check["name"]]:
                        errors.append(
                            f"{scenario_name}: pr-packet check '{check['name']}' does not match eval-report status"
                        )

                if summary["merge_gate_passed"]:
                    if "security-review" not in documents:
                        if not pr_packet["merge_readiness"]["mergeable"]:
                            errors.append(
                                f"{scenario_name}: passing eval-report should make the pr-packet mergeable"
                            )
                        if work_item and work_item["state"] != "PR_MERGEABLE":
                            errors.append(
                                f"{scenario_name}: passing eval-report should end in PR_MERGEABLE"
                            )
                else:
                    if pr_packet["merge_readiness"]["mergeable"]:
                        errors.append(
                            f"{scenario_name}: failing eval-report cannot leave the pr-packet mergeable"
                        )
                    if work_item and work_item["state"] != "PR_REVISION":
                        errors.append(
                            f"{scenario_name}: failing eval-report should end in PR_REVISION"
                        )

                if "security-review" in documents:
                    security_review = documents["security-review"]
                    if eval_report["resulting_pr_artifact_version"] > security_review["evaluated_pr_artifact_version"]:
                        errors.append(
                            f"{scenario_name}: eval-report resulting_pr_artifact_version cannot be later than the security-review evaluated_pr_artifact_version"
                        )
                elif eval_report["resulting_pr_artifact_version"] != pr_packet["artifact"]["version"]:
                    errors.append(
                        f"{scenario_name}: eval-report resulting_pr_artifact_version does not match the current pr-packet version"
                    )

        if "security-review" in documents:
            if "eval-report" not in documents:
                errors.append(
                    f"{scenario_name}: security-review requires eval-report to be present"
                )
            if not all(artifact_name in documents for artifact_name in STAGE4_ARTIFACTS):
                missing = ", ".join(
                    artifact_name for artifact_name in STAGE4_ARTIFACTS if artifact_name not in documents
                )
                errors.append(
                    f"{scenario_name}: security-review requires the Stage 4 artifacts to be present: {missing}"
                )
            security_review = documents["security-review"]
            pr_packet = documents.get("pr-packet")
            eval_report = documents.get("eval-report")
            ticket_bundle = documents.get("ticket-bundle")
            if pr_packet is None or eval_report is None or ticket_bundle is None:
                errors.append(
                    f"{scenario_name}: security-review requires pr-packet, ticket-bundle, and eval-report"
                )
            else:
                if security_review["spec_packet_id"] != spec_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: security-review spec_packet_id does not match spec-packet artifact id"
                    )
                if security_review["ticket_bundle_id"] != ticket_bundle["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: security-review ticket_bundle_id does not match ticket-bundle artifact id"
                    )
                if security_review["pr_packet_id"] != pr_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: security-review pr_packet_id does not match pr-packet artifact id"
                    )
                if security_review["eval_report_id"] != eval_report["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: security-review eval_report_id does not match eval-report artifact id"
                    )
                if "prompt-contract" in documents and security_review["prompt_contract_id"] != documents["prompt-contract"]["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: security-review prompt_contract_id does not match prompt-contract artifact id"
                    )
                if "tool-schema" in documents and security_review["tool_schema_id"] != documents["tool-schema"]["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: security-review tool_schema_id does not match tool-schema artifact id"
                    )
                if "golden-dataset" in documents and security_review["golden_dataset_id"] != documents["golden-dataset"]["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: security-review golden_dataset_id does not match golden-dataset artifact id"
                    )
                if work_item and security_review["build_attempt"] != work_item["attempt_count"]:
                    errors.append(
                        f"{scenario_name}: security-review build_attempt does not match work-item attempt_count"
                    )
                if security_review["evaluated_pr_artifact_version"] + 1 != security_review["resulting_pr_artifact_version"]:
                    errors.append(
                        f"{scenario_name}: security-review resulting_pr_artifact_version must be exactly one greater than the evaluated version"
                    )
                if security_review["evaluated_pr_artifact_version"] < eval_report["resulting_pr_artifact_version"]:
                    errors.append(
                        f"{scenario_name}: security-review evaluated_pr_artifact_version cannot be earlier than the eval-report result"
                    )
                if (
                    "merge-decision" not in documents
                    and "promotion-decision" not in documents
                    and security_review["resulting_pr_artifact_version"] != pr_packet["artifact"]["version"]
                ):
                    errors.append(
                        f"{scenario_name}: security-review resulting_pr_artifact_version does not match the current pr-packet version"
                    )

                signoff_status = security_review["signoff"]["status"]
                if signoff_status == "approved":
                    if (
                        "merge-decision" not in documents
                        and "promotion-decision" not in documents
                        and not pr_packet["merge_readiness"]["mergeable"]
                    ):
                        errors.append(
                            f"{scenario_name}: approved security-review should leave the pr-packet mergeable"
                        )
                    if (
                        "merge-decision" not in documents
                        and "promotion-decision" not in documents
                        and work_item
                        and work_item["state"] != "SECURITY_APPROVED"
                    ):
                        errors.append(
                            f"{scenario_name}: approved security-review should end in SECURITY_APPROVED"
                        )
                elif signoff_status == "pending_human":
                    if pr_packet["merge_readiness"]["mergeable"]:
                        errors.append(
                            f"{scenario_name}: pending human security-review cannot leave the pr-packet mergeable"
                        )
                    if not pr_packet["merge_readiness"]["reviewable"]:
                        errors.append(
                            f"{scenario_name}: pending human security-review must keep the pr-packet reviewable"
                        )
                    if work_item and work_item["state"] != "SECURITY_REVIEWING":
                        errors.append(
                            f"{scenario_name}: pending human security-review should end in SECURITY_REVIEWING"
                        )
                else:
                    if pr_packet["merge_readiness"]["mergeable"]:
                        errors.append(
                            f"{scenario_name}: blocked security-review cannot leave the pr-packet mergeable"
                        )
                    if work_item and work_item["state"] != "PR_REVISION":
                        errors.append(
                            f"{scenario_name}: blocked security-review should end in PR_REVISION"
                        )

        if "merge-decision" in documents:
            if "security-review" not in documents:
                errors.append(
                    f"{scenario_name}: merge-decision requires security-review to be present"
                )
            merge_decision = documents["merge-decision"]
            pr_packet = documents.get("pr-packet")
            security_review = documents.get("security-review")
            if pr_packet is None or security_review is None:
                errors.append(
                    f"{scenario_name}: merge-decision requires pr-packet and security-review"
                )
            else:
                if merge_decision["spec_packet_id"] != spec_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: merge-decision spec_packet_id does not match spec-packet artifact id"
                    )
                if merge_decision["pr_packet_id"] != pr_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: merge-decision pr_packet_id does not match pr-packet artifact id"
                    )
                if merge_decision["security_review_id"] != security_review["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: merge-decision security_review_id does not match security-review artifact id"
                    )
                if work_item and merge_decision["build_attempt"] != work_item["attempt_count"]:
                    errors.append(
                        f"{scenario_name}: merge-decision build_attempt does not match work-item attempt_count"
                    )
                if merge_decision["evaluated_pr_artifact_version"] + 1 != merge_decision["resulting_pr_artifact_version"]:
                    errors.append(
                        f"{scenario_name}: merge-decision resulting_pr_artifact_version must be exactly one greater than the evaluated version"
                    )
                if merge_decision["evaluated_pr_artifact_version"] < security_review["resulting_pr_artifact_version"]:
                    errors.append(
                        f"{scenario_name}: merge-decision evaluated_pr_artifact_version cannot be earlier than the security-review result"
                    )
                if (
                    "promotion-decision" not in documents
                    and merge_decision["resulting_pr_artifact_version"] != pr_packet["artifact"]["version"]
                ):
                    errors.append(
                        f"{scenario_name}: merge-decision resulting_pr_artifact_version does not match the current pr-packet version"
                    )

                decision_status = merge_decision["merge_decision"]["status"]
                merge_execution = pr_packet.get("merge_execution")
                if not isinstance(merge_execution, dict):
                    errors.append(
                        f"{scenario_name}: merge-decision scenarios require pr-packet merge_execution metadata"
                    )
                elif decision_status == "merged":
                    if merge_execution["status"] != "merged":
                        errors.append(
                            f"{scenario_name}: merged merge-decision must leave merge_execution.status as merged"
                        )
                    if "promotion-decision" not in documents and not pr_packet["merge_readiness"]["mergeable"]:
                        errors.append(
                            f"{scenario_name}: merged merge-decision should leave the pr-packet mergeable"
                        )
                    if (
                        "promotion-decision" not in documents
                        and work_item
                        and work_item["state"] != "MERGED"
                    ):
                        errors.append(
                            f"{scenario_name}: merged merge-decision should end in MERGED"
                        )
                elif decision_status == "pending_human":
                    if merge_execution is not None and merge_execution["status"] != "pending_human":
                        errors.append(
                            f"{scenario_name}: pending human merge-decision must leave merge_execution.status as pending_human"
                        )
                    if pr_packet["merge_readiness"]["mergeable"]:
                        errors.append(
                            f"{scenario_name}: pending human merge-decision cannot leave the pr-packet mergeable"
                        )
                    if not pr_packet["merge_readiness"]["reviewable"]:
                        errors.append(
                            f"{scenario_name}: pending human merge-decision must keep the pr-packet reviewable"
                        )
                    if work_item and work_item["state"] != "MERGE_REVIEWING":
                        errors.append(
                            f"{scenario_name}: pending human merge-decision should end in MERGE_REVIEWING"
                        )
                else:
                    if merge_execution is not None and merge_execution["status"] != "blocked":
                        errors.append(
                            f"{scenario_name}: blocked merge-decision must leave merge_execution.status as blocked"
                        )
                    if pr_packet["merge_readiness"]["mergeable"]:
                        errors.append(
                            f"{scenario_name}: blocked merge-decision cannot leave the pr-packet mergeable"
                        )
                    if work_item and work_item["state"] != "PR_REVISION":
                        errors.append(
                            f"{scenario_name}: blocked merge-decision should end in PR_REVISION"
                        )

        if "promotion-decision" in documents:
            if "security-review" not in documents:
                errors.append(
                    f"{scenario_name}: promotion-decision requires security-review to be present"
                )
            promotion_decision = documents["promotion-decision"]
            pr_packet = documents.get("pr-packet")
            security_review = documents.get("security-review")
            merge_decision = documents.get("merge-decision")
            eval_report = documents.get("eval-report")
            latency_baseline = documents.get("latency-baseline")
            if (
                pr_packet is None
                or security_review is None
                or eval_report is None
                or latency_baseline is None
            ):
                errors.append(
                    f"{scenario_name}: promotion-decision requires pr-packet, security-review, eval-report, and latency-baseline"
                )
            else:
                if promotion_decision["spec_packet_id"] != spec_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: promotion-decision spec_packet_id does not match spec-packet artifact id"
                    )
                if promotion_decision["pr_packet_id"] != pr_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: promotion-decision pr_packet_id does not match pr-packet artifact id"
                    )
                if promotion_decision["security_review_id"] != security_review["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: promotion-decision security_review_id does not match security-review artifact id"
                    )
                if promotion_decision["eval_report_id"] != eval_report["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: promotion-decision eval_report_id does not match eval-report artifact id"
                    )
                if promotion_decision["latency_baseline_id"] != latency_baseline["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: promotion-decision latency_baseline_id does not match latency-baseline artifact id"
                    )
                if work_item and promotion_decision["build_attempt"] != work_item["attempt_count"]:
                    errors.append(
                        f"{scenario_name}: promotion-decision build_attempt does not match work-item attempt_count"
                    )
                if promotion_decision["evaluated_pr_artifact_version"] + 1 != promotion_decision["resulting_pr_artifact_version"]:
                    errors.append(
                        f"{scenario_name}: promotion-decision resulting_pr_artifact_version must be exactly one greater than the evaluated version"
                    )
                if promotion_decision["evaluated_pr_artifact_version"] < security_review["resulting_pr_artifact_version"]:
                    errors.append(
                        f"{scenario_name}: promotion-decision evaluated_pr_artifact_version cannot be earlier than the security-review result"
                    )
                if (
                    merge_decision is not None
                    and promotion_decision["evaluated_pr_artifact_version"] < merge_decision["resulting_pr_artifact_version"]
                ):
                    errors.append(
                        f"{scenario_name}: promotion-decision evaluated_pr_artifact_version cannot be earlier than the merge-decision result"
                    )
                if "merge" in policy_decision["required_approvals"]:
                    if merge_decision is None:
                        errors.append(
                            f"{scenario_name}: promotion-decision scenarios for merge-gated lanes require merge-decision to be present"
                        )
                    elif merge_decision["merge_decision"]["status"] != "merged":
                        errors.append(
                            f"{scenario_name}: promotion-decision scenarios for merge-gated lanes require a merged merge-decision"
                        )
                    if pr_packet.get("merge_execution", {}).get("status") != "merged":
                        errors.append(
                            f"{scenario_name}: promotion-decision scenarios for merge-gated lanes require pr-packet merge_execution.status=merged"
                        )
                if (
                    "monitoring-report" not in documents
                    and promotion_decision["resulting_pr_artifact_version"] != pr_packet["artifact"]["version"]
                ):
                    errors.append(
                        f"{scenario_name}: promotion-decision resulting_pr_artifact_version does not match the current pr-packet version"
                    )

                decision_status = promotion_decision["promotion_decision"]["status"]
                if decision_status == "promoted":
                    if "monitoring-report" not in documents and not pr_packet["merge_readiness"]["mergeable"]:
                        errors.append(
                            f"{scenario_name}: promoted release should leave the pr-packet mergeable"
                        )
                    if (
                        "monitoring-report" not in documents
                        and work_item
                        and work_item["state"] != "PRODUCTION_MONITORING"
                    ):
                        errors.append(
                            f"{scenario_name}: promoted release should end in PRODUCTION_MONITORING"
                        )
                elif decision_status == "pending_human":
                    if pr_packet["merge_readiness"]["mergeable"]:
                        errors.append(
                            f"{scenario_name}: pending human release cannot leave the pr-packet mergeable"
                        )
                    if not pr_packet["merge_readiness"]["reviewable"]:
                        errors.append(
                            f"{scenario_name}: pending human release must keep the pr-packet reviewable"
                        )
                    if work_item and work_item["state"] != "STAGING_SOAK":
                        errors.append(
                            f"{scenario_name}: pending human release should end in STAGING_SOAK"
                        )
                else:
                    if pr_packet["merge_readiness"]["mergeable"]:
                        errors.append(
                            f"{scenario_name}: blocked release cannot leave the pr-packet mergeable"
                        )
                    if work_item and work_item["state"] != "PR_REVISION":
                        errors.append(
                            f"{scenario_name}: blocked release should end in PR_REVISION"
                        )

        if "monitoring-report" in documents:
            if "promotion-decision" not in documents:
                errors.append(
                    f"{scenario_name}: monitoring-report requires promotion-decision to be present"
                )
            monitoring_report = documents["monitoring-report"]
            pr_packet = documents.get("pr-packet")
            promotion_decision = documents.get("promotion-decision")
            merge_decision = documents.get("merge-decision")
            eval_report = documents.get("eval-report")
            security_review = documents.get("security-review")
            latency_baseline = documents.get("latency-baseline")
            if (
                pr_packet is None
                or promotion_decision is None
                or eval_report is None
                or security_review is None
                or latency_baseline is None
            ):
                errors.append(
                    f"{scenario_name}: monitoring-report requires pr-packet, promotion-decision, eval-report, security-review, and latency-baseline"
                )
            else:
                if monitoring_report["spec_packet_id"] != spec_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: monitoring-report spec_packet_id does not match spec-packet artifact id"
                    )
                if monitoring_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: monitoring-report pr_packet_id does not match pr-packet artifact id"
                    )
                if monitoring_report["promotion_decision_id"] != promotion_decision["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: monitoring-report promotion_decision_id does not match promotion-decision artifact id"
                    )
                if monitoring_report["security_review_id"] != security_review["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: monitoring-report security_review_id does not match security-review artifact id"
                    )
                if monitoring_report["eval_report_id"] != eval_report["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: monitoring-report eval_report_id does not match eval-report artifact id"
                    )
                if monitoring_report["latency_baseline_id"] != latency_baseline["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: monitoring-report latency_baseline_id does not match latency-baseline artifact id"
                    )
                if work_item and monitoring_report["build_attempt"] != work_item["attempt_count"]:
                    errors.append(
                        f"{scenario_name}: monitoring-report build_attempt does not match work-item attempt_count"
                    )
                if monitoring_report["monitored_pr_artifact_version"] + 1 != monitoring_report["resulting_pr_artifact_version"]:
                    errors.append(
                        f"{scenario_name}: monitoring-report resulting_pr_artifact_version must be exactly one greater than the monitored version"
                    )
                if monitoring_report["monitored_pr_artifact_version"] < promotion_decision["resulting_pr_artifact_version"]:
                    errors.append(
                        f"{scenario_name}: monitoring-report monitored_pr_artifact_version cannot be earlier than the promoted release version"
                    )
                if "merge" in policy_decision["required_approvals"]:
                    if merge_decision is None:
                        errors.append(
                            f"{scenario_name}: monitoring-report scenarios for merge-gated lanes require merge-decision to be present"
                        )
                    elif merge_decision["merge_decision"]["status"] != "merged":
                        errors.append(
                            f"{scenario_name}: monitoring-report scenarios for merge-gated lanes require a merged merge-decision"
                        )
                    if pr_packet.get("merge_execution", {}).get("status") != "merged":
                        errors.append(
                            f"{scenario_name}: monitoring-report scenarios for merge-gated lanes require pr-packet merge_execution.status=merged"
                        )
                if monitoring_report["resulting_pr_artifact_version"] != pr_packet["artifact"]["version"]:
                    errors.append(
                        f"{scenario_name}: monitoring-report resulting_pr_artifact_version does not match the current pr-packet version"
                    )

                decision_status = monitoring_report["monitoring_decision"]["status"]
                if decision_status == "healthy":
                    if monitoring_report["alerts"]:
                        errors.append(
                            f"{scenario_name}: healthy monitoring-report cannot contain production alerts"
                        )
                    if has_open_monitoring_incident(pr_packet):
                        if pr_packet["merge_readiness"]["mergeable"]:
                            errors.append(
                                f"{scenario_name}: healthy follow-up monitoring cannot reopen a pr-packet while a prior production incident remains open"
                            )
                        if pr_packet["reviewer_report"]["approved"]:
                            errors.append(
                                f"{scenario_name}: healthy follow-up monitoring cannot approve a pr-packet while a prior production incident remains open"
                            )
                        if not pr_packet["reviewer_report"]["blocking_findings"]:
                            errors.append(
                                f"{scenario_name}: healthy follow-up monitoring must retain blocking findings while the prior production incident remains open"
                            )
                    else:
                        if not pr_packet["merge_readiness"]["mergeable"]:
                            errors.append(
                                f"{scenario_name}: healthy monitoring-report should leave the pr-packet mergeable"
                            )
                        if not pr_packet["reviewer_report"]["approved"]:
                            errors.append(
                                f"{scenario_name}: healthy monitoring-report should leave the reviewer_report approved"
                            )
                else:
                    if not monitoring_report["alerts"]:
                        errors.append(
                            f"{scenario_name}: incident monitoring-report must contain at least one alert"
                        )
                    if pr_packet["merge_readiness"]["mergeable"]:
                        errors.append(
                            f"{scenario_name}: incident monitoring-report cannot leave the pr-packet mergeable"
                        )
                    if pr_packet["reviewer_report"]["approved"]:
                        errors.append(
                            f"{scenario_name}: incident monitoring-report cannot leave the reviewer_report approved"
                        )

                if work_item and work_item["state"] != "PRODUCTION_MONITORING":
                    errors.append(
                        f"{scenario_name}: monitoring-report scenarios should end in PRODUCTION_MONITORING"
                    )

        if "feedback-report" in documents:
            if "monitoring-report" not in documents:
                errors.append(
                    f"{scenario_name}: feedback-report requires monitoring-report to be present"
                )
            feedback_report = documents["feedback-report"]
            pr_packet = documents.get("pr-packet")
            monitoring_report = documents.get("monitoring-report")
            promotion_decision = documents.get("promotion-decision")
            merge_decision = documents.get("merge-decision")
            eval_report = documents.get("eval-report")
            security_review = documents.get("security-review")
            latency_baseline = documents.get("latency-baseline")
            ticket_bundle = documents.get("ticket-bundle")
            eval_manifest = documents.get("eval-manifest")
            if (
                pr_packet is None
                or monitoring_report is None
                or promotion_decision is None
                or eval_report is None
                or security_review is None
                or latency_baseline is None
                or ticket_bundle is None
                or eval_manifest is None
            ):
                errors.append(
                    f"{scenario_name}: feedback-report requires pr-packet, ticket-bundle, eval-manifest, monitoring-report, promotion-decision, eval-report, security-review, and latency-baseline"
                )
            else:
                if feedback_report["spec_packet_id"] != spec_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: feedback-report spec_packet_id does not match spec-packet artifact id"
                    )
                if feedback_report["ticket_bundle_id"] != ticket_bundle["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: feedback-report ticket_bundle_id does not match ticket-bundle artifact id"
                    )
                if feedback_report["eval_manifest_id"] != eval_manifest["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: feedback-report eval_manifest_id does not match eval-manifest artifact id"
                    )
                if feedback_report["pr_packet_id"] != pr_packet["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: feedback-report pr_packet_id does not match pr-packet artifact id"
                    )
                if feedback_report["monitoring_report_id"] != monitoring_report["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: feedback-report monitoring_report_id does not match monitoring-report artifact id"
                    )
                if feedback_report["promotion_decision_id"] != promotion_decision["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: feedback-report promotion_decision_id does not match promotion-decision artifact id"
                    )
                if feedback_report["security_review_id"] != security_review["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: feedback-report security_review_id does not match security-review artifact id"
                    )
                if feedback_report["eval_report_id"] != eval_report["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: feedback-report eval_report_id does not match eval-report artifact id"
                    )
                if feedback_report["latency_baseline_id"] != latency_baseline["artifact"]["id"]:
                    errors.append(
                        f"{scenario_name}: feedback-report latency_baseline_id does not match latency-baseline artifact id"
                    )
                if "merge" in policy_decision["required_approvals"]:
                    if merge_decision is None:
                        errors.append(
                            f"{scenario_name}: feedback-report scenarios for merge-gated lanes require merge-decision to be present"
                        )
                    elif merge_decision["merge_decision"]["status"] != "merged":
                        errors.append(
                            f"{scenario_name}: feedback-report scenarios for merge-gated lanes require a merged merge-decision"
                        )
                    if pr_packet.get("merge_execution", {}).get("status") != "merged":
                        errors.append(
                            f"{scenario_name}: feedback-report scenarios for merge-gated lanes require pr-packet merge_execution.status=merged"
                        )
                if work_item and feedback_report["build_attempt"] != work_item["attempt_count"]:
                    errors.append(
                        f"{scenario_name}: feedback-report build_attempt does not match work-item attempt_count"
                    )
                if feedback_report["analyzed_pr_artifact_version"] != pr_packet["artifact"]["version"]:
                    errors.append(
                        f"{scenario_name}: feedback-report analyzed_pr_artifact_version does not match the current pr-packet version"
                    )
                mode = feedback_report["feedback_window"]["mode"]
                if mode == "incident_follow_up":
                    if not feedback_report["incident_learning_packets"]:
                        errors.append(
                            f"{scenario_name}: incident follow-up feedback-report must include incident learning packets"
                        )
                    if feedback_report["summary"]["incident_count"] < 1:
                        errors.append(
                            f"{scenario_name}: incident follow-up feedback-report must report at least one incident"
                        )
                else:
                    if feedback_report["incident_learning_packets"]:
                        errors.append(
                            f"{scenario_name}: weekly rollup feedback-report cannot include incident learning packets"
                        )
                if work_item and work_item["state"] != "PRODUCTION_MONITORING":
                    errors.append(
                        f"{scenario_name}: feedback-report scenarios should end in PRODUCTION_MONITORING"
                    )

    if work_item:
        expected_current_id = highest_artifact_id(documents)
        if expected_current_id is not None and work_item["current_artifact_id"] != expected_current_id:
            errors.append(
                f"{scenario_name}: work-item current_artifact_id does not match the latest scenario artifact"
            )

        if policy_decision and policy_decision["decision"] in {
            "backlog_candidate",
            "active_build_candidate",
        }:
            expected_lane = policy_decision["lane_assignment"]["lane"]
            if work_item["execution_lane"] != expected_lane:
                errors.append(
                    f"{scenario_name}: work-item execution_lane does not match policy lane"
                )
            if work_item["policy_decision_id"] != policy_decision["artifact"]["id"]:
                errors.append(
                    f"{scenario_name}: work-item policy_decision_id does not match policy artifact id"
                )
        if policy_decision and policy_decision["decision"] == "watchlist":
            if work_item["state"] != "WATCHLISTED":
                errors.append(f"{scenario_name}: watchlist scenario should end in WATCHLISTED")

    if (
        "pr-packet" in documents
        and "eval-report" not in documents
        and work_item
        and documents["pr-packet"]["merge_readiness"]["mergeable"]
    ):
        if work_item["state"] != "PR_MERGEABLE":
            errors.append(f"{scenario_name}: mergeable PR scenario should end in PR_MERGEABLE")

    return errors


def validate_valid_scenarios(
    validators: dict[str, Draft202012Validator],
    root: Path,
) -> list[str]:
    errors: list[str] = []
    lane_policy = load_yaml(root / "factory" / "policies" / "lanes.yaml")
    scenarios_dir = root / "fixtures" / "scenarios"
    controller = FactoryController()
    if not scenarios_dir.exists():
        return ["fixtures/scenarios is missing"]

    for scenario_dir in sorted(path for path in scenarios_dir.iterdir() if path.is_dir()):
        documents: dict[str, dict[str, Any]] = {}
        scenario_has_schema_errors = False
        for fixture_path in sorted(scenario_dir.glob("*.json")):
            schema_name = fixture_path.stem
            schema_errors = validate_schema_fixture(validators, schema_name, fixture_path)
            if schema_errors:
                scenario_has_schema_errors = True
                errors.extend(f"{fixture_path}: {message}" for message in schema_errors)
                continue
            documents[schema_name] = load_json(fixture_path)

        if documents and not scenario_has_schema_errors:
            errors.extend(
                validate_policy_alignment(scenario_dir.name, documents, lane_policy)
            )
            if "work-item" in documents:
                replayed = controller.replay_scenario(scenario_dir)
                if replayed.to_document() != documents["work-item"]:
                    errors.append(
                        f"{scenario_dir.name}: replayed work-item does not match fixture work-item.json"
                    )

    return errors


def budget_class_from_lane(lane_defaults: dict[str, Any]) -> str:
    max_budget = lane_defaults["build_limits"]["max_token_budget_usd"]
    if max_budget <= 25:
        return "small"
    if max_budget <= 75:
        return "medium"
    if max_budget <= 150:
        return "large"
    return "custom"


def validate_repository_contracts(root: Path | None = None) -> list[str]:
    resolved_root = repo_root(root)
    validators = load_validators(resolved_root)

    errors: list[str] = []
    errors.extend(validate_policy_documents(validators, resolved_root))
    errors.extend(validate_valid_scenarios(validators, resolved_root))
    errors.extend(
        validate_invalid_fixtures(validators, resolved_root / "fixtures" / "invalid")
    )
    return errors


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate factory schemas, fixtures, and scenario invariants."
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    resolved_root = repo_root(args.repo_root)
    errors = validate_repository_contracts(resolved_root)

    if errors:
        print("Contract validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print(
        "Contract validation passed for schemas, valid scenarios, and expected-invalid fixtures."
    )
    return 0
