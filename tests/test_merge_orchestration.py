from __future__ import annotations

import json
from pathlib import Path

from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.controller import ControllerState, WorkItem
from auto_mindsdb_factory.merge_orchestration import StageMergePipeline
from auto_mindsdb_factory.security_review import Stage6SecurityReviewPipeline


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_stage6_fixture(root: Path, scenario_name: str) -> dict[str, object]:
    base = root / "fixtures" / "scenarios" / scenario_name
    return {
        "spec_packet": _load_json(base / "spec-packet.json"),
        "policy_decision": _load_json(base / "policy-decision.json"),
        "ticket_bundle": _load_json(base / "ticket-bundle.json"),
        "eval_manifest": _load_json(base / "eval-manifest.json"),
        "pr_packet": _load_json(base / "pr-packet.json"),
        "prompt_contract": _load_json(base / "prompt-contract.json"),
        "tool_schema": _load_json(base / "tool-schema.json"),
        "golden_dataset": _load_json(base / "golden-dataset.json"),
        "latency_baseline": _load_json(base / "latency-baseline.json"),
        "eval_report": _load_json(base / "eval-report.json"),
        "security_review": _load_json(base / "security-review.json"),
        "work_item": WorkItem.from_document(_load_json(base / "work-item.json")),
    }


def restricted_stage6_security_approved_result(root: Path):
    pending = load_stage6_fixture(root, "stage6_security_pending_feature")
    return Stage6SecurityReviewPipeline(root).process(
        pending["spec_packet"],
        pending["policy_decision"],
        pending["ticket_bundle"],
        pending["eval_manifest"],
        pending["pr_packet"],
        pending["prompt_contract"],
        pending["tool_schema"],
        pending["golden_dataset"],
        pending["latency_baseline"],
        pending["eval_report"],
        pending["work_item"],
        approved_security_reviewers=["security-oncall"],
    )


def test_merge_stage_auto_merges_guarded_candidate() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    stage6_result = load_stage6_fixture(root, "stage6_security_approved_feature")

    result = StageMergePipeline(root).process(
        stage6_result["spec_packet"],
        stage6_result["policy_decision"],
        stage6_result["ticket_bundle"],
        stage6_result["eval_manifest"],
        stage6_result["pr_packet"],
        stage6_result["prompt_contract"],
        stage6_result["tool_schema"],
        stage6_result["golden_dataset"],
        stage6_result["latency_baseline"],
        stage6_result["eval_report"],
        stage6_result["security_review"],
        stage6_result["work_item"],
    )

    assert validation_errors_for(validators["merge-decision"], result.merge_decision) == []
    assert validation_errors_for(validators["pr-packet"], result.pr_packet) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.merge_decision["merge_decision"]["status"] == "merged"
    assert result.merge_decision["merge_decision"]["mode"] == "auto"
    assert result.pr_packet["merge_execution"]["status"] == "merged"
    assert result.pr_packet["artifact"]["next_stage"] == "release_staging"
    assert result.work_item.state is ControllerState.MERGED
    assert result.work_item.current_artifact_id == result.merge_decision["artifact"]["id"]


def test_merge_stage_can_wait_for_human_merge_approval_and_resume() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    stage6_result = restricted_stage6_security_approved_result(root)
    pipeline = StageMergePipeline(root)

    pending = pipeline.process(
        stage6_result.spec_packet,
        stage6_result.policy_decision,
        stage6_result.ticket_bundle,
        stage6_result.eval_manifest,
        stage6_result.pr_packet,
        stage6_result.prompt_contract,
        stage6_result.tool_schema,
        stage6_result.golden_dataset,
        stage6_result.latency_baseline,
        stage6_result.eval_report,
        stage6_result.security_review,
        stage6_result.work_item,
    )

    assert validation_errors_for(validators["merge-decision"], pending.merge_decision) == []
    assert pending.merge_decision["merge_decision"]["status"] == "pending_human"
    assert pending.merge_decision["merge_decision"]["pending_approvals"] == ["merge"]
    assert pending.pr_packet["merge_execution"]["status"] == "pending_human"
    assert pending.pr_packet["merge_readiness"]["mergeable"] is False
    assert pending.work_item.state is ControllerState.MERGE_REVIEWING

    merged = pipeline.process(
        pending.spec_packet,
        pending.policy_decision,
        pending.ticket_bundle,
        pending.eval_manifest,
        pending.pr_packet,
        pending.prompt_contract,
        pending.tool_schema,
        pending.golden_dataset,
        pending.latency_baseline,
        pending.eval_report,
        pending.security_review,
        pending.work_item,
        approved_merge_reviewers=["merge-oncall"],
    )

    assert validation_errors_for(validators["merge-decision"], merged.merge_decision) == []
    assert merged.merge_decision["merge_decision"]["status"] == "merged"
    assert merged.merge_decision["merge_decision"]["mode"] == "human_approved"
    assert merged.merge_decision["merge_decision"]["approvals_granted"] == ["merge"]
    assert merged.pr_packet["merge_execution"]["status"] == "merged"
    assert merged.work_item.state is ControllerState.MERGED


def test_merge_stage_can_return_to_pr_revision() -> None:
    root = Path(__file__).resolve().parents[1]
    stage6_result = load_stage6_fixture(root, "stage6_security_approved_feature")

    result = StageMergePipeline(root).process(
        stage6_result["spec_packet"],
        stage6_result["policy_decision"],
        stage6_result["ticket_bundle"],
        stage6_result["eval_manifest"],
        stage6_result["pr_packet"],
        stage6_result["prompt_contract"],
        stage6_result["tool_schema"],
        stage6_result["golden_dataset"],
        stage6_result["latency_baseline"],
        stage6_result["eval_report"],
        stage6_result["security_review"],
        stage6_result["work_item"],
        blocking_findings=["Repository policy requires a final manual squash for this branch."],
    )

    assert result.merge_decision["merge_decision"]["status"] == "blocked"
    assert result.pr_packet["merge_execution"]["status"] == "blocked"
    assert result.pr_packet["merge_readiness"]["mergeable"] is False
    assert result.work_item.state is ControllerState.PR_REVISION
