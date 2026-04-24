from __future__ import annotations

import json
from pathlib import Path

import pytest

from auto_mindsdb_factory.connectors import (
    AgentResult,
    CommandEvidence,
    EvalEvidence,
    FactoryConnectorError,
    PullRequestEvidence,
    PullRequestStatus,
)
from auto_mindsdb_factory.vertical_slice import (
    FactoryVerticalSliceRunner,
    VerticalSliceConfig,
    build_cockpit_summary,
)


class FakeRepoConnector:
    def __init__(self, *, fail_create: bool = False) -> None:
        self.fail_create = fail_create

    def create_pull_request(self, *, work_item_id, spec_packet, ticket_bundle, pr_packet):
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
                            "scope": ["Update the contract to cover the new response-format behavior."],
                            "definition_of_done": ["Contract callers remain compatible."],
                            "known_edge_cases": ["Legacy callers should fail deterministically."],
                        },
                        {
                            "slug": "integration",
                            "summary": "Draft runtime integration work.",
                            "scope": ["Wire the runtime path with retry-safe behavior."],
                            "definition_of_done": ["Runtime wiring is deterministic and reversible."],
                            "known_edge_cases": ["Tool payload mismatches should fail closed."],
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
                    "what_changed": ["Draft the reviewable PR around the new response-format path."],
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
                "non_blocking_findings": ["Review the fallback path once pre-merge evals complete."],
            },
            model_fingerprint="openai.responses:gpt-5.4",
            provider="openai",
            model="gpt-5.4",
            response_id="resp_stage3_review",
        )


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

    summary = build_cockpit_summary(tmp_path / "factory-store", repo_root_override=_config(tmp_path).repo_root)

    assert summary["run_count"] == 1
    assert summary["runs"][0]["work_item_id"] == result.work_item_id
    assert summary["runs"][0]["latest_stage"] == "stage9"
    assert summary["runs"][0]["pull_request"]["url"] == result.pr_evidence.url


def test_vertical_slice_fails_when_pr_evidence_is_missing(tmp_path) -> None:
    with pytest.raises(FactoryConnectorError, match="missing PR evidence"):
        _run_with_fakes(tmp_path, repo_connector=FakeRepoConnector(fail_create=True))


def test_vertical_slice_fails_when_required_evals_fail(tmp_path) -> None:
    with pytest.raises(FactoryConnectorError, match="Required local eval commands failed"):
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

    assert stage2["ticket_bundle"]["artifact"]["model_fingerprint"] == "openai.responses:gpt-5.4"
    assert "openai.responses:gpt-5.4" in stage3["pr_packet"]["artifact"]["model_fingerprint"]
    assert "github_cli_connector.v1" in stage3["pr_packet"]["artifact"]["model_fingerprint"]
