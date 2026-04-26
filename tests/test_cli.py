from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import auto_mindsdb_factory.__main__ as cli_main
from auto_mindsdb_factory.automation import (
    AutomationState,
    FactoryAutomationCoordinator,
    Stage1AutomationCycleResult,
)
from auto_mindsdb_factory.__main__ import _load_work_item, main
from auto_mindsdb_factory.controller import FactoryController


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _history_document(work_item) -> list[dict[str, str | None]]:
    return [
        {
            "event": record.event,
            "from_state": record.from_state,
            "to_state": record.to_state,
            "artifact_id": record.artifact_id,
            "occurred_at": record.occurred_at,
        }
        for record in work_item.history
    ]


@pytest.fixture(autouse=True)
def _disable_repo_env_files() -> None:
    tracked_keys = (
        "AI_FACTORY_SKIP_ENV_FILES",
        "OPENAI_API_KEY",
        "AI_FACTORY_AGENT_PROVIDER",
        "AI_FACTORY_OPENAI_MODEL",
        "AI_FACTORY_OPENAI_FALLBACK_MODEL",
        "AI_FACTORY_OPENAI_REASONING_EFFORT",
        "AI_FACTORY_OPENAI_MAX_OUTPUT_TOKENS",
        "AI_FACTORY_OPENAI_TIMEOUT_SECONDS",
        "OPENAI_ORGANIZATION",
        "OPENAI_PROJECT",
    )
    original_values = {key: os.environ.get(key) for key in tracked_keys}
    os.environ["AI_FACTORY_SKIP_ENV_FILES"] = "1"
    try:
        yield
    finally:
        for key, value in original_values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def load_stage8_result_document(root: Path, scenario_name: str) -> dict:
    scenario = root / "fixtures" / "scenarios" / scenario_name
    replayed = FactoryController().replay_scenario(scenario)
    return {
        "spec_packet": _load_json(scenario / "spec-packet.json"),
        "policy_decision": _load_json(scenario / "policy-decision.json"),
        "ticket_bundle": _load_json(scenario / "ticket-bundle.json"),
        "eval_manifest": _load_json(scenario / "eval-manifest.json"),
        "pr_packet": _load_json(scenario / "pr-packet.json"),
        "prompt_contract": _load_json(scenario / "prompt-contract.json"),
        "tool_schema": _load_json(scenario / "tool-schema.json"),
        "golden_dataset": _load_json(scenario / "golden-dataset.json"),
        "latency_baseline": _load_json(scenario / "latency-baseline.json"),
        "eval_report": _load_json(scenario / "eval-report.json"),
        "security_review": _load_json(scenario / "security-review.json"),
        "promotion_decision": _load_json(scenario / "promotion-decision.json"),
        "monitoring_report": _load_json(scenario / "monitoring-report.json"),
        "work_item": replayed.to_document(),
        "history": _history_document(replayed),
    }


def test_stage1_manual_intake_cli_emits_valid_bundle(capsys) -> None:
    exit_code = main(
        [
            "stage1-intake-manual",
            "--provider",
            "github",
            "--external-id",
            "github-issue-2",
            "--title",
            "Factory cockpit should surface GitHub check conclusions and eval status",
            "--body",
            (
                "The operator cockpit should surface GitHub pull request check conclusions, local eval "
                "status, and a clear health summary for each work item. This is a control-plane API and "
                "JSON schema change for the cockpit command, not a model-runtime change. Operators should "
                "not need to cross-check multiple artifacts to decide whether a run is healthy. Acceptance "
                "criteria: - update the factory cockpit tool output to include the latest GitHub check "
                "conclusions for each run - include the latest local eval status summary from vertical-slice "
                "or automation artifacts - add a single health field that resolves to ready, blocked, or "
                "warning based on PR checks, eval status, and monitoring alerts - cover the new output with "
                "CLI tests and contract-safe validation"
            ),
            "--url",
            "https://github.com/ianu82/ai-factory/issues/2",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["source_item"]["kind"] == "manual_intake"
    assert payload["spec_packet"]["summary"]["problem"].startswith("GitHub issue:")
    assert payload["spec_packet"]["summary"]["affected_surfaces"] == [
        "api_contract",
        "control_plane",
    ]
    assert payload["work_item"]["state"] == "POLICY_ASSIGNED"


def test_stage2_cli_reports_invalid_json(capsys, tmp_path) -> None:
    invalid_stage1 = tmp_path / "stage1-invalid.json"
    invalid_stage1.write_text('{"spec_packet": ', encoding="utf-8")

    exit_code = main(
        [
            "stage2-ticketing",
            "--stage1-result-file",
            str(invalid_stage1),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Stage 2 ticketing failed:" in captured.err
    assert "not valid JSON" in captured.err


def test_stage2_cli_reports_missing_openai_api_key(capsys, monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    stage1_result = tmp_path / "stage1-result.json"
    stage1_result.write_text(
        json.dumps(
            {
                "spec_packet": {
                    "artifact": {"artifact_id": "spec-1"},
                    "summary": {
                        "problem": "Test problem",
                        "proposed_capability": "Test capability",
                        "acceptance_criteria": ["Criterion 1"],
                        "non_goals": [],
                        "affected_surfaces": ["api_contract"],
                    },
                    "open_questions": [],
                    "source_item": {
                        "title": "Manual test item",
                        "kind": "manual_intake",
                    },
                },
                "policy_decision": {
                    "artifact": {"artifact_id": "policy-1"},
                    "lane": "fast",
                    "required_eval_tiers": ["unit"],
                    "risk_factors": [],
                },
                "work_item": {
                    "id": "wi-test",
                    "title": "Test item",
                    "state": "POLICY_ASSIGNED",
                    "history": [],
                    "artifacts": [],
                },
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "stage2-ticketing",
            "--agent-provider",
            "openai",
            "--stage1-result-file",
            str(stage1_result),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Stage 2 ticketing failed:" in captured.err
    assert "OPENAI_API_KEY" in captured.err


def test_stage2_cli_loads_repo_env_file_before_parser_defaults(
    capsys,
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.delenv("AI_FACTORY_SKIP_ENV_FILES", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("AI_FACTORY_AGENT_PROVIDER", raising=False)

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".env").write_text(
        "AI_FACTORY_AGENT_PROVIDER=none\nOPENAI_API_KEY=base-key\n",
        encoding="utf-8",
    )
    (repo_root / ".env.local").write_text(
        "AI_FACTORY_AGENT_PROVIDER=openai\nOPENAI_API_KEY=local-key\n",
        encoding="utf-8",
    )
    stage1_result = tmp_path / "stage1-result.json"
    stage1_result.write_text("{}", encoding="utf-8")

    captured: dict[str, str] = {}

    monkeypatch.setattr(cli_main, "_load_stage1_result", lambda path: ({}, {}, {}))
    monkeypatch.setattr(cli_main, "_load_work_item", lambda document, label: object())

    def fake_build_agent_connector(args):
        captured["agent_provider"] = args.agent_provider
        captured["openai_api_key"] = os.environ.get("OPENAI_API_KEY", "")
        return None

    class _Stage2Result:
        def to_document(self) -> dict[str, bool]:
            return {"ok": True}

    class _FakeStage2Pipeline:
        def __init__(self, root, *, agent_connector=None) -> None:
            self.root = root
            self.agent_connector = agent_connector

        def process(self, spec_packet, policy_decision, work_item):
            return _Stage2Result()

    monkeypatch.setattr(cli_main, "_build_agent_connector", fake_build_agent_connector)
    monkeypatch.setattr(cli_main, "Stage2TicketingPipeline", _FakeStage2Pipeline)

    exit_code = main(
        [
            "stage2-ticketing",
            "--repo-root",
            str(repo_root),
            "--stage1-result-file",
            str(stage1_result),
        ]
    )
    captured_io = capsys.readouterr()

    assert exit_code == 0
    assert json.loads(captured_io.out) == {"ok": True}
    assert captured["agent_provider"] == "openai"
    assert captured["openai_api_key"] == "local-key"


def test_linear_webhook_server_cli_invokes_runtime(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    def fake_serve_linear_webhooks(
        *,
        store_dir: Path,
        host: str,
        port: int,
        repo_root_override: Path | None = None,
        config=None,
    ) -> None:
        captured["store_dir"] = store_dir
        captured["host"] = host
        captured["port"] = port
        captured["repo_root_override"] = repo_root_override

    monkeypatch.setattr(cli_main, "serve_linear_webhooks", fake_serve_linear_webhooks)

    exit_code = main(
        [
            "linear-webhook-server",
            "--store-dir",
            str(tmp_path / "store"),
            "--host",
            "127.0.0.1",
            "--port",
            "8090",
            "--repo-root",
            str(tmp_path / "repo"),
        ]
    )

    assert exit_code == 0
    assert captured == {
        "store_dir": tmp_path / "store",
        "host": "127.0.0.1",
        "port": 8090,
        "repo_root_override": tmp_path / "repo",
    }


def test_linear_trigger_cycle_cli_emits_result(capsys, monkeypatch, tmp_path) -> None:
    class _FakeResult:
        failed_events: list[dict[str, object]] = []

        def to_document(self) -> dict[str, object]:
            return {
                "cycle": "linear-trigger",
                "processed_events": [{"delivery_id": "delivery-123"}],
                "skipped_events": [],
                "failed_events": [],
                "trigger_state": {
                    "version": 1,
                    "processed_delivery_ids": ["delivery-123"],
                    "processed_logical_trigger_keys": ["linear:issue-123:state-factory:2026-04-24T12:00:00Z"],
                    "updated_at": "2026-04-24T12:00:02Z",
                },
            }

        def failed_handoffs(self) -> list[dict[str, object]]:
            return []

    class _FakeWorker:
        def __init__(self, store_dir: Path, *, repo_root_override: Path | None = None) -> None:
            self.store_dir = store_dir
            self.repo_root_override = repo_root_override

        def run_cycle(self, *, repository: str, max_events: int | None = None) -> _FakeResult:
            assert repository == "ianu82/ai-factory"
            assert max_events == 2
            return _FakeResult()

    monkeypatch.setattr(cli_main, "LinearTriggerWorker", _FakeWorker)

    exit_code = main(
        [
            "automation-linear-trigger-cycle",
            "--store-dir",
            str(tmp_path / "store"),
            "--repository",
            "ianu82/ai-factory",
            "--max-events",
            "2",
            "--repo-root",
            str(tmp_path / "repo"),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["cycle"] == "linear-trigger"
    assert payload["processed_events"] == [{"delivery_id": "delivery-123"}]


def test_linear_ensure_stage_states_cli_emits_result(capsys, monkeypatch, tmp_path) -> None:
    class _FakeSync:
        def __init__(self, store_dir: Path, *, repo_root_override: Path | None = None) -> None:
            assert store_dir == tmp_path / "store"
            assert repo_root_override == tmp_path / "repo"

        def ensure_stage_states(self) -> dict[str, dict[str, object]]:
            return {
                "stage1": {"id": "state-1", "name": "Stage 1 Intake"},
                "stage9": {"id": "state-9", "name": "Stage 9 Feedback"},
            }

    monkeypatch.setattr(cli_main, "LinearWorkflowSync", _FakeSync)

    exit_code = main(
        [
            "linear-ensure-stage-states",
            "--store-dir",
            str(tmp_path / "store"),
            "--repo-root",
            str(tmp_path / "repo"),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["cycle"] == "linear-stage-setup"
    assert payload["stage_states"]["stage1"]["name"] == "Stage 1 Intake"


def test_linear_sync_cycle_cli_emits_result(capsys, monkeypatch, tmp_path) -> None:
    class _FakeResult:
        failed_runs: list[dict[str, object]] = []

        def to_document(self) -> dict[str, object]:
            return {
                "cycle": "linear-workflow-sync",
                "stage_states": {"stage1": {"id": "state-1", "name": "Stage 1 Intake"}},
                "synced_runs": [{"work_item_id": "wi-123"}],
                "skipped_runs": [],
                "failed_runs": [],
            }

    class _FakeSync:
        def __init__(self, store_dir: Path, *, repo_root_override: Path | None = None) -> None:
            assert store_dir == tmp_path / "store"
            assert repo_root_override == tmp_path / "repo"

        def sync_existing_runs(self, *, max_runs: int | None = None) -> _FakeResult:
            assert max_runs == 2
            return _FakeResult()

    monkeypatch.setattr(cli_main, "LinearWorkflowSync", _FakeSync)

    exit_code = main(
        [
            "automation-linear-sync-cycle",
            "--store-dir",
            str(tmp_path / "store"),
            "--max-runs",
            "2",
            "--repo-root",
            str(tmp_path / "repo"),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["cycle"] == "linear-workflow-sync"
    assert payload["synced_runs"] == [{"work_item_id": "wi-123"}]


def test_validate_contracts_cli_reports_malformed_env_file(
    capsys,
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.delenv("AI_FACTORY_SKIP_ENV_FILES", raising=False)

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".env").write_text("NOT VALID", encoding="utf-8")

    exit_code = main(
        [
            "validate-contracts",
            "--repo-root",
            str(repo_root),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Environment setup failed:" in captured.err
    assert "KEY=VALUE" in captured.err


def test_stage3_cli_reports_missing_stage2_fields(capsys, tmp_path) -> None:
    incomplete_stage2 = tmp_path / "stage2-incomplete.json"
    incomplete_stage2.write_text(
        json.dumps(
            {
                "spec_packet": {},
                "policy_decision": {},
                "ticket_bundle": {},
                "work_item": {},
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "stage3-build-review",
            "--stage2-result-file",
            str(incomplete_stage2),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Stage 3 build/review failed:" in captured.err
    assert "missing the required object field 'eval_manifest'" in captured.err


def test_stage4_cli_reports_missing_stage3_fields(capsys, tmp_path) -> None:
    incomplete_stage3 = tmp_path / "stage3-incomplete.json"
    incomplete_stage3.write_text(
        json.dumps(
            {
                "spec_packet": {},
                "policy_decision": {},
                "ticket_bundle": {},
                "eval_manifest": {},
                "work_item": {},
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "stage4-integration",
            "--stage3-result-file",
            str(incomplete_stage3),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Stage 4 integration failed:" in captured.err
    assert "missing the required object field 'pr_packet'" in captured.err


def test_stage5_cli_reports_missing_stage4_fields(capsys, tmp_path) -> None:
    incomplete_stage4 = tmp_path / "stage4-incomplete.json"
    incomplete_stage4.write_text(
        json.dumps(
            {
                "spec_packet": {},
                "policy_decision": {},
                "ticket_bundle": {},
                "eval_manifest": {},
                "pr_packet": {},
                "tool_schema": {},
                "golden_dataset": {},
                "latency_baseline": {},
                "work_item": {},
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "stage5-eval",
            "--stage4-result-file",
            str(incomplete_stage4),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Stage 5 eval failed:" in captured.err
    assert "missing the required object field 'prompt_contract'" in captured.err


def test_stage6_cli_reports_missing_stage5_fields(capsys, tmp_path) -> None:
    incomplete_stage5 = tmp_path / "stage5-incomplete.json"
    incomplete_stage5.write_text(
        json.dumps(
            {
                "spec_packet": {},
                "policy_decision": {},
                "ticket_bundle": {},
                "eval_manifest": {},
                "pr_packet": {},
                "prompt_contract": {},
                "tool_schema": {},
                "golden_dataset": {},
                "latency_baseline": {},
                "work_item": {},
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "stage6-security-review",
            "--stage5-result-file",
            str(incomplete_stage5),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Stage 6 security review failed:" in captured.err
    assert "missing the required object field 'eval_report'" in captured.err


def test_stage_merge_cli_reports_missing_stage6_fields(capsys, tmp_path) -> None:
    incomplete_stage6 = tmp_path / "stage6-incomplete.json"
    incomplete_stage6.write_text(
        json.dumps(
            {
                "spec_packet": {},
                "policy_decision": {},
                "ticket_bundle": {},
                "eval_manifest": {},
                "pr_packet": {},
                "prompt_contract": {},
                "tool_schema": {},
                "golden_dataset": {},
                "latency_baseline": {},
                "eval_report": {},
                "work_item": {},
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "stage-merge",
            "--stage6-result-file",
            str(incomplete_stage6),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Merge stage failed:" in captured.err
    assert "missing the required object field 'security_review'" in captured.err


def test_stage7_cli_reports_missing_stage6_fields(capsys, tmp_path) -> None:
    incomplete_stage6 = tmp_path / "stage6-incomplete.json"
    incomplete_stage6.write_text(
        json.dumps(
            {
                "spec_packet": {},
                "policy_decision": {},
                "ticket_bundle": {},
                "eval_manifest": {},
                "pr_packet": {},
                "prompt_contract": {},
                "tool_schema": {},
                "golden_dataset": {},
                "latency_baseline": {},
                "eval_report": {},
                "work_item": {},
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "stage7-release-staging",
            "--stage6-result-file",
            str(incomplete_stage6),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Stage 7 release staging failed:" in captured.err
    assert "missing the required object field 'security_review'" in captured.err


def test_stage_merge_cli_merges_guarded_fixture(capsys) -> None:
    root = Path(__file__).resolve().parents[1]
    base = root / "fixtures" / "scenarios" / "stage6_security_approved_feature"

    exit_code = main(
        [
            "stage-merge",
            "--spec-packet-file",
            str(base / "spec-packet.json"),
            "--policy-decision-file",
            str(base / "policy-decision.json"),
            "--ticket-bundle-file",
            str(base / "ticket-bundle.json"),
            "--eval-manifest-file",
            str(base / "eval-manifest.json"),
            "--pr-packet-file",
            str(base / "pr-packet.json"),
            "--prompt-contract-file",
            str(base / "prompt-contract.json"),
            "--tool-schema-file",
            str(base / "tool-schema.json"),
            "--golden-dataset-file",
            str(base / "golden-dataset.json"),
            "--latency-baseline-file",
            str(base / "latency-baseline.json"),
            "--eval-report-file",
            str(base / "eval-report.json"),
            "--security-review-file",
            str(base / "security-review.json"),
            "--work-item-file",
            str(base / "work-item.json"),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["merge_decision"]["merge_decision"]["status"] == "merged"
    assert payload["pr_packet"]["merge_execution"]["status"] == "merged"
    assert payload["work_item"]["state"] == "MERGED"


def test_stage7_cli_rejects_restricted_security_approved_bundle_without_merge(capsys, tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    base = root / "fixtures" / "scenarios" / "stage6_security_pending_feature"

    stage6_exit = main(
        [
            "stage6-security-review",
            "--spec-packet-file",
            str(base / "spec-packet.json"),
            "--policy-decision-file",
            str(base / "policy-decision.json"),
            "--ticket-bundle-file",
            str(base / "ticket-bundle.json"),
            "--eval-manifest-file",
            str(base / "eval-manifest.json"),
            "--pr-packet-file",
            str(base / "pr-packet.json"),
            "--prompt-contract-file",
            str(base / "prompt-contract.json"),
            "--tool-schema-file",
            str(base / "tool-schema.json"),
            "--golden-dataset-file",
            str(base / "golden-dataset.json"),
            "--latency-baseline-file",
            str(base / "latency-baseline.json"),
            "--eval-report-file",
            str(base / "eval-report.json"),
            "--work-item-file",
            str(base / "work-item.json"),
            "--approved-security-reviewer",
            "security-oncall",
        ]
    )
    stage6_output = capsys.readouterr()

    assert stage6_exit == 0
    approved_stage6 = tmp_path / "stage6-approved.json"
    approved_stage6.write_text(stage6_output.out, encoding="utf-8")

    exit_code = main(
        [
            "stage7-release-staging",
            "--stage6-result-file",
            str(approved_stage6),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Stage 7 release staging failed:" in captured.err
    assert "requires merge approval; run merge orchestration first" in captured.err


def test_stage8_cli_reports_missing_stage7_fields(capsys, tmp_path) -> None:
    incomplete_stage7 = tmp_path / "stage7-incomplete.json"
    incomplete_stage7.write_text(
        json.dumps(
            {
                "spec_packet": {},
                "policy_decision": {},
                "ticket_bundle": {},
                "eval_manifest": {},
                "pr_packet": {},
                "prompt_contract": {},
                "tool_schema": {},
                "golden_dataset": {},
                "latency_baseline": {},
                "eval_report": {},
                "security_review": {},
                "work_item": {},
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "stage8-production-monitoring",
            "--stage7-result-file",
            str(incomplete_stage7),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Stage 8 production monitoring failed:" in captured.err
    assert "missing the required object field 'promotion_decision'" in captured.err


def test_stage9_cli_reports_missing_stage8_fields(capsys, tmp_path) -> None:
    incomplete_stage8 = tmp_path / "stage8-incomplete.json"
    incomplete_stage8.write_text(
        json.dumps(
            {
                "spec_packet": {},
                "policy_decision": {},
                "ticket_bundle": {},
                "eval_manifest": {},
                "pr_packet": {},
                "prompt_contract": {},
                "tool_schema": {},
                "golden_dataset": {},
                "latency_baseline": {},
                "eval_report": {},
                "security_review": {},
                "promotion_decision": {},
                "work_item": {},
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "stage9-feedback-synthesis",
            "--stage8-result-file",
            str(incomplete_stage8),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Stage 9 feedback synthesis failed:" in captured.err
    assert "missing the required object field 'monitoring_report'" in captured.err


def test_load_work_item_accepts_stage_result_documents_with_history() -> None:
    work_item = _load_work_item(
        {
            "work_item": {
                "work_item_id": "wi-test-001",
                "source_provider": "anthropic",
                "source_external_id": "anthropic-test-001",
                "title": "Test work item",
                "state": "PRODUCTION_MONITORING",
                "risk_score": 35,
                "execution_lane": "guarded",
                "policy_decision_id": "policy-test-001",
                "current_artifact_id": "feedback-report-test-001",
                "attempt_count": 1,
                "dead_letter_reason": None,
                "created_at": "2026-04-22T12:00:00Z",
                "updated_at": "2026-04-22T12:30:00Z",
            },
            "history": [
                {
                    "event": "feedback_synthesized",
                    "from_state": "PRODUCTION_MONITORING",
                    "to_state": "PRODUCTION_MONITORING",
                    "artifact_id": "feedback-report-test-001",
                    "occurred_at": "2026-04-22T12:30:00Z",
                }
            ],
        },
        "Stage 9 result",
    )

    assert work_item.current_artifact_id == "feedback-report-test-001"
    assert len(work_item.history) == 1
    assert work_item.history[0].event == "feedback_synthesized"


def test_automation_stage1_cli_runs_cycle(capsys, tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html_file = root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"

    exit_code = main(
        [
            "automation-stage1-cycle",
            "--store-dir",
            str(tmp_path / "automation-store"),
            "--html-file",
            str(html_file),
            "--repo-root",
            str(root),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["cycle"] == "stage1"
    assert payload["created_results"]


def test_automation_stage1_cli_can_advance_immediately(capsys, tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html_file = root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"

    exit_code = main(
        [
            "automation-stage1-cycle",
            "--store-dir",
            str(tmp_path / "automation-store"),
            "--html-file",
            str(html_file),
            "--repo-root",
            str(root),
            "--max-new-items",
            "1",
            "--advance-immediately",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["advance_immediately"] is True
    assert payload["handoff_results"][0]["status"] == "progressed"
    assert payload["handoff_results"][0]["final_stage"] == "stage8"


def test_automation_stage1_cli_returns_error_on_failed_immediate_handoff(
    capsys,
    monkeypatch,
    tmp_path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    html_file = root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"

    def _fake_run_stage1_cycle(self, **kwargs):
        return Stage1AutomationCycleResult(
            detected_count=1,
            created_results=[
                {
                    "work_item_id": "wi-test-001",
                    "source_external_id": "anthropic-test-001",
                    "state": "POLICY_ASSIGNED",
                    "stored_path": str(tmp_path / "stage1-result.json"),
                }
            ],
            skipped_known_external_ids=[],
            deferred_external_ids=[],
            state=AutomationState(),
            advance_immediately=True,
            handoff_results=[
                {
                    "work_item_id": "wi-test-001",
                    "source_stage": "stage1",
                    "source_state": "POLICY_ASSIGNED",
                    "status": "failed",
                    "final_stage": None,
                    "final_state": None,
                    "stages_completed": [],
                    "stored_paths": {},
                    "reason": "synthetic handoff failure",
                }
            ],
        )

    monkeypatch.setattr(
        FactoryAutomationCoordinator,
        "run_stage1_cycle",
        _fake_run_stage1_cycle,
    )

    exit_code = main(
        [
            "automation-stage1-cycle",
            "--store-dir",
            str(tmp_path / "automation-store"),
            "--html-file",
            str(html_file),
            "--repo-root",
            str(root),
            "--advance-immediately",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    payload = json.loads(captured.out)
    assert payload["handoff_results"][0]["status"] == "failed"
    assert "Automation immediate handoff failed:" in captured.err
    assert "synthetic handoff failure" in captured.err


def test_automation_register_bundle_cli_rejects_malformed_stage8_result(capsys, tmp_path) -> None:
    invalid_stage8 = tmp_path / "stage8-invalid.json"
    invalid_stage8.write_text(
        json.dumps(
            {
                "work_item": {
                    "work_item_id": "wi-test-001",
                    "updated_at": "2026-04-22T12:30:00Z",
                }
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "automation-register-bundle",
            "--stage",
            "stage8",
            "--result-file",
            str(invalid_stage8),
            "--store-dir",
            str(tmp_path / "automation-store"),
            "--repo-root",
            str(Path(__file__).resolve().parents[1]),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Automation bundle registration failed:" in captured.err
    assert "missing required object fields" in captured.err


def test_automation_register_bundle_cli_can_advance_immediately(capsys, tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    source_coordinator = FactoryAutomationCoordinator(
        tmp_path / "source-store",
        repo_root_override=root,
    )
    stage1_result = source_coordinator.run_stage1_cycle(html=html, max_new_items=1)
    stage1_document = json.loads(
        Path(stage1_result.created_results[0]["stored_path"]).read_text(encoding="utf-8")
    )
    stage1_file = tmp_path / "stage1-result.json"
    stage1_file.write_text(json.dumps(stage1_document), encoding="utf-8")

    exit_code = main(
        [
            "automation-register-bundle",
            "--stage",
            "stage1",
            "--result-file",
            str(stage1_file),
            "--store-dir",
            str(tmp_path / "automation-store"),
            "--repo-root",
            str(root),
            "--advance-immediately",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["advance_immediately"] is True
    assert payload["handoff"]["status"] == "progressed"
    assert payload["handoff"]["final_stage"] == "stage8"


def test_automation_register_bundle_cli_advances_stage8_incident_into_stage9(
    capsys,
    tmp_path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    stage8_document = load_stage8_result_document(root, "stage8_auto_mitigated_feature")
    stage8_file = tmp_path / "stage8-result.json"
    stage8_file.write_text(json.dumps(stage8_document), encoding="utf-8")

    exit_code = main(
        [
            "automation-register-bundle",
            "--stage",
            "stage8",
            "--result-file",
            str(stage8_file),
            "--store-dir",
            str(tmp_path / "automation-store"),
            "--repo-root",
            str(root),
            "--advance-immediately",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["handoff"]["status"] == "progressed"
    assert payload["handoff"]["final_stage"] == "stage9"


def test_automation_register_bundle_cli_returns_error_on_failed_immediate_handoff(
    capsys,
    tmp_path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    scenario = root / "fixtures" / "scenarios" / "stage4_reviewable_feature"
    stage4_document = {
        "spec_packet": json.loads((scenario / "spec-packet.json").read_text(encoding="utf-8")),
        "policy_decision": json.loads(
            (scenario / "policy-decision.json").read_text(encoding="utf-8")
        ),
        "ticket_bundle": json.loads((scenario / "ticket-bundle.json").read_text(encoding="utf-8")),
        "eval_manifest": json.loads((scenario / "eval-manifest.json").read_text(encoding="utf-8")),
        "pr_packet": json.loads((scenario / "pr-packet.json").read_text(encoding="utf-8")),
        "prompt_contract": json.loads(
            (scenario / "prompt-contract.json").read_text(encoding="utf-8")
        ),
        "tool_schema": json.loads((scenario / "tool-schema.json").read_text(encoding="utf-8")),
        "golden_dataset": json.loads(
            (scenario / "golden-dataset.json").read_text(encoding="utf-8")
        ),
        "latency_baseline": json.loads(
            (scenario / "latency-baseline.json").read_text(encoding="utf-8")
        ),
        "work_item": json.loads((scenario / "work-item.json").read_text(encoding="utf-8")),
    }
    stage4_document["work_item"]["state"] = "PR_MERGEABLE"
    stage4_file = tmp_path / "stage4-result.json"
    stage4_file.write_text(json.dumps(stage4_document), encoding="utf-8")

    exit_code = main(
        [
            "automation-register-bundle",
            "--stage",
            "stage4",
            "--result-file",
            str(stage4_file),
            "--store-dir",
            str(tmp_path / "automation-store"),
            "--repo-root",
            str(root),
            "--advance-immediately",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    payload = json.loads(captured.out)
    assert payload["handoff"]["status"] == "failed"
    assert "missing required object fields: eval_report" in payload["handoff"]["reason"]
    assert "Automation immediate handoff failed:" in captured.err


def test_automation_advance_runs_cli_progresses_active_build_items(capsys, tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html_file = root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"
    store_dir = tmp_path / "automation-store"

    stage1_exit_code = main(
        [
            "automation-stage1-cycle",
            "--store-dir",
            str(store_dir),
            "--html-file",
            str(html_file),
            "--repo-root",
            str(root),
            "--max-new-items",
            "1",
        ]
    )
    stage1_captured = capsys.readouterr()

    assert stage1_exit_code == 0
    assert json.loads(stage1_captured.out)["created_results"]

    advance_exit_code = main(
        [
            "automation-advance-runs",
            "--store-dir",
            str(store_dir),
            "--repo-root",
            str(root),
        ]
    )
    advance_captured = capsys.readouterr()

    assert advance_exit_code == 0
    payload = json.loads(advance_captured.out)
    assert payload["cycle"] == "stage2-through-stage8-progression"
    assert payload["processed_runs"][0]["final_stage"] == "stage8"


def test_factory_doctor_cli_emits_runtime_checks(capsys, monkeypatch, tmp_path) -> None:
    class _FakeDoctor:
        def __init__(self, config) -> None:
            assert config.repository == "ianu82/ai-factory"
            assert config.autonomy_mode.value == "pr_ready"

        def run(self) -> dict[str, object]:
            return {
                "cycle": "factory-doctor",
                "status": "passed",
                "checks": [{"name": "env:OPENAI_API_KEY", "status": "passed"}],
            }

    monkeypatch.setattr(cli_main, "FactoryDoctor", _FakeDoctor)

    exit_code = main(
        [
            "factory-doctor",
            "--store-dir",
            str(tmp_path / "store"),
            "--repo-root",
            str(tmp_path / "repo"),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["cycle"] == "factory-doctor"
    assert payload["status"] == "passed"


def test_factory_worker_cli_runs_once(capsys, monkeypatch, tmp_path) -> None:
    class _FakeWorker:
        def __init__(self, config) -> None:
            assert config.repository == "ianu82/ai-factory"
            assert config.autonomy_mode.value == "pr_ready"
            assert config.max_events_per_cycle == 3

        def run(self, *, once: bool = False, max_cycles: int | None = None) -> dict[str, object]:
            assert once is True
            assert max_cycles is None
            return {
                "cycle": "factory-worker",
                "status": "completed",
                "cycles": [{"cycle": "factory-worker-cycle"}],
            }

    monkeypatch.setattr(cli_main, "FactoryWorker", _FakeWorker)

    exit_code = main(
        [
            "factory-worker",
            "--store-dir",
            str(tmp_path / "store"),
            "--repo-root",
            str(tmp_path / "repo"),
            "--max-events",
            "3",
            "--once",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["cycle"] == "factory-worker"
    assert payload["cycles"] == [{"cycle": "factory-worker-cycle"}]


def test_factory_cockpit_cli_marks_stale_heartbeats_with_custom_threshold(
    capsys,
    monkeypatch,
    tmp_path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    work_item_id = "wi-stage5-stale-cli"
    run_dir = tmp_path / "store" / "runs" / work_item_id
    run_dir.mkdir(parents=True)
    monkeypatch.setattr(
        "auto_mindsdb_factory.vertical_slice.load_active_code_worker_operations",
        lambda: {},
    )

    stage4_result = {
        "spec_packet": {},
        "policy_decision": {},
        "ticket_bundle": {},
        "eval_manifest": {},
        "pr_packet": {
            "pull_request": {
                "repository": "ianu82/ai-factory",
                "url": "https://github.com/ianu82/ai-factory/pull/42",
            }
        },
        "prompt_contract": {},
        "tool_schema": {},
        "golden_dataset": {},
        "latency_baseline": {},
        "work_item": {
            "work_item_id": work_item_id,
            "state": "PR_REVIEWABLE",
            "title": "CLI stale heartbeat test",
            "updated_at": "2026-04-26T20:00:00Z",
        },
    }
    (run_dir / "stage4-result.json").write_text(
        json.dumps(stage4_result),
        encoding="utf-8",
    )
    now = datetime.now(timezone.utc).replace(microsecond=0)
    refreshed_at = (now - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")
    acquired_at = (now - timedelta(minutes=12)).isoformat().replace("+00:00", "Z")
    expires_at = (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
    (run_dir / ".automation.lock").write_text(
        json.dumps(
            {
                "scope": "run",
                "resource_id": work_item_id,
                "lease_id": "lease-123",
                "acquired_at": acquired_at,
                "refreshed_at": refreshed_at,
                "expires_at": expires_at,
                "pid": 999,
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "factory-cockpit",
            "--store-dir",
            str(tmp_path / "store"),
            "--repo-root",
            str(root),
            "--stale-heartbeat-seconds",
            "5",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["runs"][0]["active_operation"]["stage"] == "stage5"
    assert payload["runs"][0]["active_operation"]["heartbeat_status"] == "possibly_stuck"


def test_automation_supervisor_cycle_cli_runs_full_pass(capsys, tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html_file = root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"

    exit_code = main(
        [
            "automation-supervisor-cycle",
            "--store-dir",
            str(tmp_path / "automation-store"),
            "--html-file",
            str(html_file),
            "--repo-root",
            str(root),
            "--max-new-items",
            "1",
            "--run-weekly-feedback",
            "--window-label",
            "2026-W17",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["cycle"] == "automation-supervisor-cycle"
    assert payload["stage1_result"]["created_results"]
    assert payload["progression_result"]["processed_runs"][0]["final_stage"] == "stage8"
    assert payload["weekly_feedback_result"]["processed_results"]
    assert payload["post_progression_handoff_results"] == []
