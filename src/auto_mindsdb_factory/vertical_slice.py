from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .automation import FactoryRunStore
from .build_review import Stage3BuildReviewPipeline
from .connectors import (
    AgentConnector,
    EvalConnector,
    EvalEvidence,
    FileBackedOpsConnector,
    GitHubCLIRepoConnector,
    OpsSignalConnector,
    PullRequestEvidence,
    PullRequestStatus,
    RepoConnector,
)
from .controller import ControllerState, WorkItem
from .eval_execution import Stage5EvalPipeline
from .feedback_synthesis import Stage9FeedbackSynthesisPipeline
from .integration import Stage4IntegrationPipeline
from .intake import AnthropicScout, Stage1IntakePipeline, repo_root, utc_now
from .merge_orchestration import StageMergePipeline
from .release_staging import Stage7ReleaseStagingPipeline
from .production_monitoring import Stage8ProductionMonitoringPipeline
from .security_review import Stage6SecurityReviewPipeline
from .ticketing import Stage2TicketingPipeline


class VerticalSliceError(RuntimeError):
    """Raised when the end-to-end factory slice cannot safely continue."""


@dataclass(slots=True)
class VerticalSliceConfig:
    repo_root: Path | None
    store_dir: Path
    repository: str = "ianu82/ai-factory"
    html_file: Path | None = None
    source_url: str | None = None
    entry_index: int = 0
    base_branch: str = "main"
    seed_missing_ops_signals: bool = True
    feedback_window_days: int = 7


@dataclass(slots=True)
class VerticalSliceResult:
    work_item_id: str
    final_state: str
    stored_paths: dict[str, str]
    summary_path: str
    pr_evidence: PullRequestEvidence
    pr_status: PullRequestStatus
    eval_evidence: EvalEvidence
    staging_signal: dict[str, Any]
    rollback_signal: dict[str, Any]
    monitoring_signal: dict[str, Any]
    feedback_report_id: str
    completed_at: str = field(default_factory=utc_now)

    def to_document(self) -> dict[str, Any]:
        return {
            "work_item_id": self.work_item_id,
            "final_state": self.final_state,
            "stored_paths": dict(self.stored_paths),
            "summary_path": self.summary_path,
            "pr_evidence": self.pr_evidence.to_document(),
            "pr_status": self.pr_status.to_document(),
            "eval_evidence": self.eval_evidence.to_document(),
            "staging_signal": dict(self.staging_signal),
            "rollback_signal": dict(self.rollback_signal),
            "monitoring_signal": dict(self.monitoring_signal),
            "feedback_report_id": self.feedback_report_id,
            "completed_at": self.completed_at,
        }


class FactoryVerticalSliceRunner:
    """Run one release-note-to-feedback slice through real delivery seams."""

    def __init__(
        self,
        config: VerticalSliceConfig,
        *,
        agent_connector: AgentConnector | None = None,
        repo_connector: RepoConnector | None = None,
        eval_connector: EvalConnector | None = None,
        ops_connector: OpsSignalConnector | None = None,
    ) -> None:
        self.config = config
        self.root = repo_root(config.repo_root)
        self.agent_connector = agent_connector
        self.store = FactoryRunStore(config.store_dir, repo_root_override=self.root)
        self.repo_connector = repo_connector or GitHubCLIRepoConnector(
            self.root,
            repository=config.repository,
            base_branch=config.base_branch,
        )
        self.eval_connector = eval_connector
        self.ops_connector = ops_connector or FileBackedOpsConnector(
            config.store_dir,
            seed_missing_signals=config.seed_missing_ops_signals,
        )
        self.stage1 = Stage1IntakePipeline(self.root)
        self.stage2 = Stage2TicketingPipeline(self.root, agent_connector=agent_connector)
        self.stage3 = Stage3BuildReviewPipeline(self.root, agent_connector=agent_connector)
        self.stage4 = Stage4IntegrationPipeline(self.root)
        self.stage5 = Stage5EvalPipeline(self.root)
        self.stage6 = Stage6SecurityReviewPipeline(self.root)
        self.merge = StageMergePipeline(self.root)
        self.stage7 = Stage7ReleaseStagingPipeline(self.root)
        self.stage8 = Stage8ProductionMonitoringPipeline(self.root)
        self.stage9 = Stage9FeedbackSynthesisPipeline(self.root)

    def run(self) -> VerticalSliceResult:
        source_item = self._select_source_item()
        stage1_result = self.stage1.process_item(source_item)
        stored_paths = {
            "stage1": str(self._save_stage("stage1", stage1_result.to_document())),
        }
        work_item = stage1_result.work_item

        stage2_result = self.stage2.process(
            stage1_result.spec_packet,
            stage1_result.policy_decision,
            work_item,
        )
        stored_paths["stage2"] = str(self._save_stage("stage2", stage2_result.to_document()))
        stage3_result = self._run_stage3_until_reviewable(stage2_result, stored_paths)
        pr_evidence = self.repo_connector.create_pull_request(
            work_item_id=stage3_result.work_item.work_item_id,
            spec_packet=stage3_result.spec_packet,
            ticket_bundle=stage3_result.ticket_bundle,
            pr_packet=stage3_result.pr_packet,
        )
        pr_packet = self._attach_pr_evidence(stage3_result.pr_packet, pr_evidence)
        stage3_document = stage3_result.to_document()
        stage3_document["pr_packet"] = pr_packet
        stored_paths["stage3"] = str(self._save_stage("stage3", stage3_document))
        work_item = stage3_result.work_item

        stage4_result = self.stage4.process(
            stage3_result.spec_packet,
            stage3_result.policy_decision,
            stage3_result.ticket_bundle,
            stage3_result.eval_manifest,
            pr_packet,
            work_item,
        )
        stored_paths["stage4"] = str(self._save_stage("stage4", stage4_result.to_document()))
        work_item = stage4_result.work_item

        eval_evidence = self._run_eval_connector()
        self._write_run_document(
            work_item.work_item_id,
            "vertical-slice-eval-evidence.json",
            eval_evidence.to_document(),
        )
        eval_evidence.assert_passed()
        pr_status = self.repo_connector.read_pull_request_status(pr_evidence)
        stage5_result = self.stage5.process(
            stage4_result.spec_packet,
            stage4_result.policy_decision,
            stage4_result.ticket_bundle,
            stage4_result.eval_manifest,
            stage4_result.pr_packet,
            stage4_result.prompt_contract,
            stage4_result.tool_schema,
            stage4_result.golden_dataset,
            stage4_result.latency_baseline,
            work_item,
        )
        stored_paths["stage5"] = str(self._save_stage("stage5", stage5_result.to_document()))
        work_item = stage5_result.work_item

        stage6_result = self.stage6.process(
            stage5_result.spec_packet,
            stage5_result.policy_decision,
            stage5_result.ticket_bundle,
            stage5_result.eval_manifest,
            stage5_result.pr_packet,
            stage5_result.prompt_contract,
            stage5_result.tool_schema,
            stage5_result.golden_dataset,
            stage5_result.latency_baseline,
            stage5_result.eval_report,
            work_item,
        )
        stored_paths["stage6"] = str(self._save_stage("stage6", stage6_result.to_document()))
        work_item = stage6_result.work_item

        merge_result = self.merge.process(
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
            work_item,
        )
        stored_paths["merge"] = str(self._save_stage("merge", merge_result.to_document()))
        work_item = merge_result.work_item

        work_item_id = work_item.work_item_id
        self.ops_connector.ensure_default_signals(work_item_id)
        rollback_signal = self.ops_connector.read_rollback_signal(work_item_id)
        staging_signal = self.ops_connector.read_staging_signal(work_item_id)
        stage7_result = self.stage7.process(
            merge_result.spec_packet,
            merge_result.policy_decision,
            merge_result.ticket_bundle,
            merge_result.eval_manifest,
            merge_result.pr_packet,
            merge_result.prompt_contract,
            merge_result.tool_schema,
            merge_result.golden_dataset,
            merge_result.latency_baseline,
            merge_result.eval_report,
            merge_result.security_review,
            work_item,
            merge_decision=merge_result.merge_decision,
            observed_soak_minutes=staging_signal["soak_minutes"],
            observed_request_samples=staging_signal["request_samples"],
            metric_overrides=staging_signal["metrics"],
            rollback_tested=bool(rollback_signal["tested"]),
        )
        stored_paths["stage7"] = str(self._save_stage("stage7", stage7_result.to_document()))
        work_item = stage7_result.work_item

        monitoring_signal = self.ops_connector.read_monitoring_signal(work_item_id)
        stage8_result = self.stage8.process(
            stage7_result.spec_packet,
            stage7_result.policy_decision,
            stage7_result.ticket_bundle,
            stage7_result.eval_manifest,
            stage7_result.pr_packet,
            stage7_result.prompt_contract,
            stage7_result.tool_schema,
            stage7_result.golden_dataset,
            stage7_result.latency_baseline,
            stage7_result.eval_report,
            stage7_result.security_review,
            stage7_result.promotion_decision,
            work_item,
            merge_decision=stage7_result.merge_decision,
            observed_window_minutes=monitoring_signal["window_minutes"],
            metric_overrides=monitoring_signal["metrics"],
            security_anomaly=monitoring_signal["security_anomaly"],
        )
        stored_paths["stage8"] = str(self._save_stage("stage8", stage8_result.to_document()))
        work_item = stage8_result.work_item

        stage9_result = self.stage9.process(
            stage8_result.spec_packet,
            stage8_result.policy_decision,
            stage8_result.ticket_bundle,
            stage8_result.eval_manifest,
            stage8_result.pr_packet,
            stage8_result.prompt_contract,
            stage8_result.tool_schema,
            stage8_result.golden_dataset,
            stage8_result.latency_baseline,
            stage8_result.eval_report,
            stage8_result.security_review,
            stage8_result.promotion_decision,
            stage8_result.monitoring_report,
            work_item,
            merge_decision=stage8_result.merge_decision,
            feedback_window_days=self.config.feedback_window_days,
            positive_surprises=[
                "The factory produced real GitHub PR evidence while keeping ops signals file-backed."
            ],
        )
        stored_paths["stage9"] = str(self._save_stage("stage9", stage9_result.to_document()))

        result = VerticalSliceResult(
            work_item_id=stage9_result.work_item.work_item_id,
            final_state=stage9_result.work_item.state.value,
            stored_paths=stored_paths,
            summary_path="",
            pr_evidence=pr_evidence,
            pr_status=pr_status,
            eval_evidence=eval_evidence,
            staging_signal=staging_signal,
            rollback_signal=rollback_signal,
            monitoring_signal=monitoring_signal,
            feedback_report_id=stage9_result.feedback_report["artifact"]["id"],
        )
        summary_path = self._write_run_document(
            result.work_item_id,
            "vertical-slice-summary.json",
            {**result.to_document(), "summary_path": None},
        )
        result.summary_path = str(summary_path)
        summary_path.write_text(
            f"{json.dumps(result.to_document(), indent=2)}\n",
            encoding="utf-8",
        )
        return result

    def _run_stage3_until_reviewable(
        self,
        stage2_result,
        stored_paths: dict[str, str],
    ) -> Stage3BuildReviewResult:
        work_item = stage2_result.work_item
        revision_guidance: list[str] | None = None
        previous_pr_packet: dict[str, Any] | None = None
        max_cycles = int(stage2_result.policy_decision["budget_policy"]["max_pr_review_cycles"])

        while True:
            stage3_result = self.stage3.process(
                stage2_result.spec_packet,
                stage2_result.policy_decision,
                stage2_result.ticket_bundle,
                stage2_result.eval_manifest,
                work_item,
                repository=self.config.repository,
                revision_guidance=revision_guidance,
                previous_pr_packet=previous_pr_packet,
            )
            attempt_number = stage3_result.work_item.attempt_count
            stage3_document = stage3_result.to_document()
            self._write_run_document(
                stage3_result.work_item.work_item_id,
                f"stage3-attempt-{attempt_number}-result.json",
                stage3_document,
            )
            stored_paths["stage3"] = str(self._save_stage("stage3", stage3_document))

            if stage3_result.work_item.state is ControllerState.PR_REVIEWABLE:
                return stage3_result
            if stage3_result.work_item.state is not ControllerState.PR_REVISION:
                raise VerticalSliceError(
                    "Stage 3 revision loop ended in an unexpected state: "
                    f"{stage3_result.work_item.state.value}."
                )

            revision_guidance = list(stage3_result.pr_packet["reviewer_report"]["blocking_findings"])
            if not revision_guidance:
                raise VerticalSliceError(
                    "Stage 3 returned PR_REVISION without blocking findings to address."
                )
            if self.agent_connector is None:
                raise VerticalSliceError(
                    "Stage 3 produced blocking findings but no live agent connector is configured "
                    "to revise the draft: "
                    + "; ".join(revision_guidance)
                )
            if stage3_result.work_item.attempt_count >= max_cycles:
                raise VerticalSliceError(
                    "Stage 3 revision loop exhausted the build retry budget. "
                    "Last blocking findings: "
                    + "; ".join(revision_guidance)
                )

            previous_pr_packet = stage3_result.pr_packet
            work_item = stage3_result.work_item

    def _select_source_item(self):
        html = None
        if self.config.html_file is not None:
            html = self.config.html_file.read_text(encoding="utf-8")
        elif self.config.source_url is None:
            default_fixture = self.root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"
            html = default_fixture.read_text(encoding="utf-8")

        scout = AnthropicScout(source_url=self.config.source_url)
        items = scout.list_items(html=html)
        if self.config.entry_index < 0 or self.config.entry_index >= len(items):
            raise VerticalSliceError(
                f"entry_index must be between 0 and {len(items) - 1}; "
                f"got {self.config.entry_index}."
            )
        return items[self.config.entry_index]

    def _run_eval_connector(self) -> EvalEvidence:
        if self.eval_connector is not None:
            return self.eval_connector.run_required_evals()
        from .connectors import LocalEvalConnector

        return LocalEvalConnector(self.root).run_required_evals()

    def _save_stage(self, stage_name: str, document: dict[str, Any]) -> Path:
        with self.store.state_transaction() as state:
            stored_path = self.store.save_stage_result(stage_name, document)
            self.store.apply_stage_result_to_state(state, stage_name, document)
        return stored_path

    def _write_run_document(
        self,
        work_item_id: str,
        file_name: str,
        document: dict[str, Any],
    ) -> Path:
        path = self.store.runs_dir / work_item_id / file_name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{json.dumps(document, indent=2)}\n", encoding="utf-8")
        return path

    @staticmethod
    def _attach_pr_evidence(
        pr_packet: dict[str, Any],
        evidence: PullRequestEvidence,
    ) -> dict[str, Any]:
        updated = deepcopy(pr_packet)
        prior_fingerprint = updated["artifact"].get("model_fingerprint")
        updated["artifact"]["version"] = int(updated["artifact"]["version"]) + 1
        updated["artifact"]["owner_agent"] = "GitHub Connector"
        updated["artifact"]["model_fingerprint"] = (
            f"{prior_fingerprint} -> github_cli_connector.v1"
            if prior_fingerprint
            else "github_cli_connector.v1"
        )
        updated["artifact"]["updated_at"] = evidence.created_at
        updated["branch_name"] = evidence.branch_name
        updated["pull_request"] = {
            **updated["pull_request"],
            "repository": evidence.repository,
            "number": evidence.number,
            "url": evidence.url,
            "title": evidence.title,
        }
        return updated


def build_cockpit_summary(
    store_dir: Path,
    *,
    repo_root_override: Path | None = None,
) -> dict[str, Any]:
    store = FactoryRunStore(store_dir, repo_root_override=repo_root_override)
    runs: list[dict[str, Any]] = []
    for run_dir in store.iter_run_directories():
        candidate = store.load_latest_candidate(run_dir, tuple(reversed(tuple(store_stage_order()))))
        if candidate is None:
            continue
        work_item = store.extract_work_item_document(candidate.document)
        pr_packet = candidate.document.get("pr_packet")
        monitoring_report = candidate.document.get("monitoring_report")
        feedback_report = candidate.document.get("feedback_report")
        runs.append(
            {
                "work_item_id": candidate.work_item_id,
                "latest_stage": candidate.stage_name,
                "state": work_item["state"],
                "title": work_item["title"],
                "updated_at": work_item["updated_at"],
                "pull_request": (
                    None
                    if not isinstance(pr_packet, dict)
                    else pr_packet.get("pull_request")
                ),
                "monitoring_status": (
                    None
                    if not isinstance(monitoring_report, dict)
                    else monitoring_report["monitoring_decision"]["status"]
                ),
                "feedback_report_id": (
                    None
                    if not isinstance(feedback_report, dict)
                    else feedback_report["artifact"]["id"]
                ),
            }
        )
    return {
        "store_dir": str(store.root),
        "run_count": len(runs),
        "runs": runs,
        "generated_at": utc_now(),
    }


def store_stage_order() -> list[str]:
    return [
        "stage1",
        "stage2",
        "stage3",
        "stage4",
        "stage5",
        "stage6",
        "merge",
        "stage7",
        "stage8",
        "stage9",
    ]
