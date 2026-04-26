from __future__ import annotations

import json
from pathlib import Path

import pytest

from auto_mindsdb_factory.build_review import Stage3BuildReviewPipeline
from auto_mindsdb_factory.connectors import (
    AgentResult,
    CommandEvidence,
    EvalEvidence,
    FactoryConnectorError,
    PullRequestEvidence,
    PullRequestStatus,
)
from auto_mindsdb_factory.intake import Stage1IntakePipeline, build_manual_intake_item
from auto_mindsdb_factory.ticketing import Stage2TicketingPipeline
from auto_mindsdb_factory.vertical_slice import (
    FactoryVerticalSliceRunner,
    VerticalSliceConfig,
    build_cockpit_summary,
)


class FakeRepoConnector:
    def __init__(self, *, fail_create: bool = False) -> None:
        self.fail_create = fail_create
        self.create_calls = 0

    def create_pull_request(
        self, *, work_item_id, spec_packet, ticket_bundle, pr_packet
    ):
        self.create_calls += 1
        if self.fail_create:
            raise FactoryConnectorError("missing PR evidence")
        return PullRequestEvidence(
            repository=pr_packet["pull_request"]["repository"],
            branch_name=f"factory/test-{work_item_id[-8:]}",
            base_branch="main",
            commit_sha="abc1234",
            number=42,
            url="https://github.com/ianu82/ai-factory/pull/42",
            title=pr_packet["pull_request"]["title"],
        )

    def read_pull_request_status(self, evidence):
        return PullRequestStatus(
            repository=evidence.repository,
            number=evidence.number,
            state="OPEN",
            mergeable="MERGEABLE",
            url=evidence.url,
            checks=[
                {
                    "name": "local-contracts",
                    "status": "success",
                    "url": None,
                }
            ],
        )


class FakeEvalConnector:
    def __init__(self, *, passed: bool = True) -> None:
        self.passed = passed

    def run_required_evals(self):
        status = "passed" if self.passed else "failed"
        exit_code = 0 if self.passed else 1
        return EvalEvidence(
            status=status,
            commands=[
                CommandEvidence(
                    command=["fake-eval"],
                    exit_code=exit_code,
                    stdout=status,
                    stderr="" if self.passed else "failed",
                )
            ],
        )


class FakeAgentConnector:
    def run_task(self, task):
        if task.name == "stage2_ticket_drafting":
            return AgentResult(
                name=task.name,
                output_document={
                    "tickets": [
                        {
                            "slug": "contract",
                            "summary": "Draft contract compatibility work.",
                            "scope": [
                                "Update the contract to cover the new response-format behavior."
                            ],
                            "definition_of_done": [
                                "Contract callers remain compatible."
                            ],
                            "known_edge_cases": [
                                "Legacy callers should fail deterministically."
                            ],
                        },
                        {
                            "slug": "integration",
                            "summary": "Draft runtime integration work.",
                            "scope": [
                                "Wire the runtime path with retry-safe behavior."
                            ],
                            "definition_of_done": [
                                "Runtime wiring is deterministic and reversible."
                            ],
                            "known_edge_cases": [
                                "Tool payload mismatches should fail closed."
                            ],
                        },
                    ]
                },
                model_fingerprint="openai.responses:gpt-5.4",
                provider="openai",
                model="gpt-5.4",
                response_id="resp_stage2",
            )
        if task.name == "stage3_pr_draft":
            return AgentResult(
                name=task.name,
                output_document={
                    "what_changed": [
                        "Draft the reviewable PR around the new response-format path."
                    ],
                    "key_risks": ["Contract compatibility must remain deterministic."],
                    "changed_paths": [
                        "src/auto_mindsdb_factory/connectors.py",
                        "tests/test_connectors.py",
                    ],
                },
                model_fingerprint="openai.responses:gpt-5.4",
                provider="openai",
                model="gpt-5.4",
                response_id="resp_stage3_build",
            )
        return AgentResult(
            name=task.name,
            output_document={
                "blocking_findings": [],
                "non_blocking_findings": [
                    "Review the fallback path once pre-merge evals complete."
                ],
            },
            model_fingerprint="openai.responses:gpt-5.4",
            provider="openai",
            model="gpt-5.4",
            response_id="resp_stage3_review",
        )


class RevisingAgentConnector:
    def __init__(self) -> None:
        self.stage3_draft_inputs: list[dict] = []
        self.stage3_review_calls = 0

    def run_task(self, task):
        if task.name == "stage2_ticket_drafting":
            return FakeAgentConnector().run_task(task)
        if task.name == "stage3_pr_draft":
            self.stage3_draft_inputs.append(task.input_document)
            revision_guidance = task.input_document.get("revision_guidance") or []
            if revision_guidance:
                return AgentResult(
                    name=task.name,
                    output_document={
                        "what_changed": [
                            "Gate Anthropic response_format tool mode by explicit capability checks and surface a deterministic unsupported-capability error for unsupported models.",
                            "Persist and rehydrate tool-result state before request retries so provider retries never re-run side-effecting tools.",
                        ],
                        "key_risks": [
                            "Capability gating must remain compatibility-safe for legacy Anthropic callers."
                        ],
                        "changed_paths": [
                            "integrations/anthropic/contracts.py",
                            "integrations/anthropic/tool_runtime.py",
                            "tests/contracts/test_anthropic_contracts.py",
                            "tests/integration/test_anthropic_tool_runtime.py",
                        ],
                    },
                    model_fingerprint="openai.responses:gpt-5.4",
                    provider="openai",
                    model="gpt-5.4",
                    response_id="resp_stage3_build_revision",
                )
            return AgentResult(
                name=task.name,
                output_document={
                    "what_changed": [
                        "Add initial Anthropic response_format tool-mode support for tool-result turns."
                    ],
                    "key_risks": [
                        "Capability gating still needs explicit rollout and model coverage."
                    ],
                    "changed_paths": [
                        "integrations/anthropic/contracts.py",
                        "integrations/anthropic/tool_runtime.py",
                    ],
                },
                model_fingerprint="openai.responses:gpt-5.4",
                provider="openai",
                model="gpt-5.4",
                response_id="resp_stage3_build_initial",
            )
        if task.name == "stage3_pr_review":
            self.stage3_review_calls += 1
            if self.stage3_review_calls == 1:
                return AgentResult(
                    name=task.name,
                    output_document={
                        "blocking_findings": [
                            "The draft still does not pin down the Anthropic capability gate for response_format tool mode on unsupported models."
                        ],
                        "non_blocking_findings": [
                            "Spell out retry behavior for persisted tool-result state."
                        ],
                    },
                    model_fingerprint="openai.responses:gpt-5.4",
                    provider="openai",
                    model="gpt-5.4",
                    response_id="resp_stage3_review_blocked",
                )
            return AgentResult(
                name=task.name,
                output_document={
                    "blocking_findings": [],
                    "non_blocking_findings": [
                        "Add one more regression fixture for malformed provider payloads."
                    ],
                },
                model_fingerprint="openai.responses:gpt-5.4",
                provider="openai",
                model="gpt-5.4",
                response_id="resp_stage3_review_approved",
            )
        raise AssertionError(f"Unexpected task: {task.name}")


class UnhealthyOpsConnector:
    def ensure_default_signals(self, work_item_id: str) -> None:
        return None

    def read_rollback_signal(self, work_item_id: str):
        return {
            "work_item_id": work_item_id,
            "tested": True,
            "executed": False,
            "status": "passed",
            "evidence": "rollback probe passed",
        }

    def read_staging_signal(self, work_item_id: str):
        return {
            "work_item_id": work_item_id,
            "soak_minutes": 1440,
            "request_samples": 5000,
            "metrics": {},
        }

    def read_monitoring_signal(self, work_item_id: str):
        return {
            "work_item_id": work_item_id,
            "window_minutes": 45,
            "metrics": {"error_rate_pct": 99},
            "security_anomaly": False,
        }


def _config(tmp_path: Path) -> VerticalSliceConfig:
    root = Path(__file__).resolve().parents[1]
    return VerticalSliceConfig(
        repo_root=root,
        store_dir=tmp_path / "factory-store",
        repository="ianu82/ai-factory",
        html_file=root / "fixtures" / "intake" / "anthropic-release-notes-sample.html",
        entry_index=0,
    )


def _write_stage_result(
    store_dir: Path,
    *,
    work_item_id: str,
    stage_name: str,
    document: dict,
) -> Path:
    stage_path = store_dir / "runs" / work_item_id / f"{stage_name}-result.json"
    stage_path.parent.mkdir(parents=True, exist_ok=True)
    stage_path.write_text(json.dumps(document), encoding="utf-8")
    return stage_path


def _linear_issue_item():
    return build_manual_intake_item(
        provider="linear",
        external_id="linear-issue-sof-114",
        title="Factory cockpit should expose run isolation and concurrency health",
        url="https://linear.app/mindsdb/issue/SOF-114/factory-cockpit-isolation",
        detected_at="2026-04-26T19:18:23Z",
        published_at="2026-04-26T19:18:22Z",
        body=(
            "The factory cockpit should expose work item ownership, branch identity, PR linkage, "
            "and machine-readable run isolation evidence so operators can tell whether a Stage 3 "
            "code worker used a per-run worktree instead of editing the main checkout."
        ),
    )


def _stage2_linear_issue_result(root: Path):
    stage1_result = Stage1IntakePipeline(root).process_item(_linear_issue_item())
    return Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )


def _stage3_linear_issue_document(root: Path) -> dict:
    stage2_result = _stage2_linear_issue_result(root)
    return (
        Stage3BuildReviewPipeline(root)
        .process(
            stage2_result.spec_packet,
            stage2_result.policy_decision,
            stage2_result.ticket_bundle,
            stage2_result.eval_manifest,
            stage2_result.work_item,
            repository="ianu82/ai-factory",
        )
        .to_document()
    )


def _run_with_fakes(tmp_path: Path, **kwargs):
    return FactoryVerticalSliceRunner(
        _config(tmp_path),
        repo_connector=kwargs.get("repo_connector", FakeRepoConnector()),
        eval_connector=kwargs.get("eval_connector", FakeEvalConnector()),
        ops_connector=kwargs.get("ops_connector"),
    ).run()


def test_vertical_slice_reaches_stage9_with_pr_and_eval_evidence(tmp_path) -> None:
    result = _run_with_fakes(tmp_path)

    assert result.final_state == "PRODUCTION_MONITORING"
    assert result.pr_evidence.url == "https://github.com/ianu82/ai-factory/pull/42"
    assert result.eval_evidence.status == "passed"
    assert Path(result.stored_paths["stage9"]).exists()
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    stage3 = json.loads(Path(result.stored_paths["stage3"]).read_text(encoding="utf-8"))
    assert summary["feedback_report_id"] == result.feedback_report_id
    assert stage3["pr_packet"]["pull_request"]["number"] == 42


def test_vertical_slice_cockpit_summarizes_latest_run(tmp_path) -> None:
    result = _run_with_fakes(tmp_path)

    summary = build_cockpit_summary(
        tmp_path / "factory-store", repo_root_override=_config(tmp_path).repo_root
    )
    run = summary["runs"][0]

    assert summary["run_count"] == 1
    assert run["work_item_id"] == result.work_item_id
    assert run["latest_stage"] == "stage9"
    assert run["branch_name"] == result.pr_evidence.branch_name
    assert run["pull_request"]["url"] == result.pr_evidence.url
    assert run["source"]["provider"] == "anthropic"
    assert run["isolation"]["status"] == "warning"
    assert run["isolation"]["mode"] == "per_run_branch_only"


def test_cockpit_summary_keeps_stage2_runs_pending_for_isolation_checks(
    tmp_path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "factory-store"
    stage2_result = _stage2_linear_issue_result(root)

    _write_stage_result(
        store_dir,
        work_item_id=stage2_result.work_item.work_item_id,
        stage_name="stage2",
        document=stage2_result.to_document(),
    )

    summary = build_cockpit_summary(store_dir, repo_root_override=root)
    run = summary["runs"][0]

    assert run["state"] == "TICKETED"
    assert run["source"]["identifier"] == "SOF-114"
    assert (
        run["source"]["url"]
        == "https://linear.app/mindsdb/issue/SOF-114/factory-cockpit-isolation"
    )
    assert run["branch_name"] is None
    assert run["isolation"]["status"] == "pending"
    assert run["isolation"]["mode"] == "not_required_yet"


def test_cockpit_summary_marks_reviewable_code_worker_runs_healthy(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "factory-store"
    stage3_document = _stage3_linear_issue_document(root)
    branch_name = (
        "factory/work-item-factory-cockpit-should-expose-run-isolation-and-1454bb4de285"
    )
    pr_url = "https://github.com/ianu82/ai-factory/pull/114"
    stage3_document["pr_packet"]["branch_name"] = branch_name
    stage3_document["pr_packet"]["pull_request"]["number"] = 114
    stage3_document["pr_packet"]["pull_request"]["url"] = pr_url
    stage3_document["pr_packet"]["delivery_evidence"] = {
        "mode": "code_worker_pr",
        "repository": "ianu82/ai-factory",
        "branch_name": branch_name,
        "base_branch": "main",
        "commit_sha": "abc1234",
        "pull_request_number": 114,
        "pull_request_url": pr_url,
        "code_worker": {
            "worktree_path": "/tmp/ai-factory-worktree-8i3hlh94/repo",
            "isolated_worktree": True,
            "edited_main_checkout": False,
        },
        "created_at": "2026-04-26T19:25:16Z",
    }

    _write_stage_result(
        store_dir,
        work_item_id=stage3_document["work_item"]["work_item_id"],
        stage_name="stage3",
        document=stage3_document,
    )

    summary = build_cockpit_summary(store_dir, repo_root_override=root)
    run = summary["runs"][0]

    assert run["state"] == "PR_REVIEWABLE"
    assert run["branch_name"] == branch_name
    assert run["pull_request"]["number"] == 114
    assert run["pull_request"]["url"] == pr_url
    assert run["isolation"]["status"] == "healthy"
    assert run["isolation"]["mode"] == "per_run_branch_and_worktree"
    assert run["isolation"]["base_branch"] == "main"
    assert run["isolation"]["worktree_path"] == "/tmp/ai-factory-worktree-8i3hlh94/repo"
    assert run["isolation"]["isolated_worktree"] is True
    assert run["isolation"]["edited_main_checkout"] is False


def test_cockpit_summary_warns_when_stage3_run_lacks_worktree_evidence(
    tmp_path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "factory-store"
    stage3_document = _stage3_linear_issue_document(root)
    branch_name = "factory/factory-cockpit-should-expose-run-isolation-and"
    stage3_document["pr_packet"]["branch_name"] = branch_name

    _write_stage_result(
        store_dir,
        work_item_id=stage3_document["work_item"]["work_item_id"],
        stage_name="stage3",
        document=stage3_document,
    )

    summary = build_cockpit_summary(store_dir, repo_root_override=root)
    run = summary["runs"][0]

    assert run["state"] == "PR_REVIEWABLE"
    assert run["branch_name"] == branch_name
    assert run["isolation"]["status"] == "warning"
    assert run["isolation"]["mode"] == "per_run_branch_only"
    assert "worktree evidence" in run["isolation"]["summary"]


def test_vertical_slice_fails_when_pr_evidence_is_missing(tmp_path) -> None:
    with pytest.raises(FactoryConnectorError, match="missing PR evidence"):
        _run_with_fakes(tmp_path, repo_connector=FakeRepoConnector(fail_create=True))


def test_vertical_slice_fails_when_required_evals_fail(tmp_path) -> None:
    with pytest.raises(
        FactoryConnectorError, match="Required local eval commands failed"
    ):
        _run_with_fakes(tmp_path, eval_connector=FakeEvalConnector(passed=False))

    evidence_path = (
        tmp_path
        / "factory-store"
        / "runs"
        / "wi-anthropic-2026-04-20-april-20-2026-25307862"
        / "vertical-slice-eval-evidence.json"
    )
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))

    assert evidence["status"] == "failed"
    assert evidence["commands"][0]["exit_code"] == 1


def test_vertical_slice_records_unhealthy_monitoring_feedback(tmp_path) -> None:
    result = _run_with_fakes(tmp_path, ops_connector=UnhealthyOpsConnector())

    stage8 = json.loads(Path(result.stored_paths["stage8"]).read_text(encoding="utf-8"))
    stage9 = json.loads(Path(result.stored_paths["stage9"]).read_text(encoding="utf-8"))
    assert stage8["monitoring_report"]["monitoring_decision"]["status"] != "healthy"
    assert stage9["feedback_report"]["summary"]["incident_count"] >= 1


def test_vertical_slice_can_run_with_agent_assisted_stage2_and_stage3(tmp_path) -> None:
    result = FactoryVerticalSliceRunner(
        _config(tmp_path),
        agent_connector=FakeAgentConnector(),
        repo_connector=FakeRepoConnector(),
        eval_connector=FakeEvalConnector(),
    ).run()

    stage2 = json.loads(Path(result.stored_paths["stage2"]).read_text(encoding="utf-8"))
    stage3 = json.loads(Path(result.stored_paths["stage3"]).read_text(encoding="utf-8"))

    assert (
        stage2["ticket_bundle"]["artifact"]["model_fingerprint"]
        == "openai.responses:gpt-5.4"
    )
    assert (
        "openai.responses:gpt-5.4"
        in stage3["pr_packet"]["artifact"]["model_fingerprint"]
    )
    assert (
        "github_cli_connector.v1"
        in stage3["pr_packet"]["artifact"]["model_fingerprint"]
    )


def test_vertical_slice_revises_stage3_until_reviewable(tmp_path) -> None:
    repo_connector = FakeRepoConnector()
    agent_connector = RevisingAgentConnector()

    result = FactoryVerticalSliceRunner(
        _config(tmp_path),
        agent_connector=agent_connector,
        repo_connector=repo_connector,
        eval_connector=FakeEvalConnector(),
    ).run()

    stage3 = json.loads(Path(result.stored_paths["stage3"]).read_text(encoding="utf-8"))
    attempt1_path = (
        tmp_path
        / "factory-store"
        / "runs"
        / result.work_item_id
        / "stage3-attempt-1-result.json"
    )
    attempt2_path = (
        tmp_path
        / "factory-store"
        / "runs"
        / result.work_item_id
        / "stage3-attempt-2-result.json"
    )

    assert result.final_state == "PRODUCTION_MONITORING"
    assert repo_connector.create_calls == 1
    assert attempt1_path.exists()
    assert attempt2_path.exists()
    assert agent_connector.stage3_draft_inputs[1]["revision_guidance"] == [
        "The draft still does not pin down the Anthropic capability gate for response_format tool mode on unsupported models."
    ]
    assert stage3["work_item"]["attempt_count"] == 2
    assert stage3["pr_packet"]["reviewer_report"]["approved"] is True
