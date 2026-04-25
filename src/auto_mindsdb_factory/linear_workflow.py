from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

from .controller import ControllerState
from .intake import normalize_whitespace, repo_root, utc_now
from .linear_trigger import (
    DEFAULT_LINEAR_GRAPHQL_URL,
    LinearGraphQLClient,
    LinearGraphQLClientError,
    LinearIssueSnapshot,
)


class LinearWorkflowError(RuntimeError):
    """Base class for Linear workflow-sync failures."""


class LinearWorkflowConfigurationError(LinearWorkflowError):
    """Raised when Linear workflow sync is configured incorrectly."""


class LinearWorkflowStoreError(LinearWorkflowError):
    """Raised when Linear workflow-sync artifacts cannot be persisted safely."""


def _bool_from_env(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise LinearWorkflowConfigurationError(
        f"{name} must be one of true/false, yes/no, on/off, or 1/0."
    )


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


@dataclass(frozen=True, slots=True)
class LinearFactoryStageDefinition:
    key: str
    name: str
    state_type: str
    color: str
    description: str


LINEAR_FACTORY_STAGES: tuple[LinearFactoryStageDefinition, ...] = (
    LinearFactoryStageDefinition(
        key="stage1",
        name="Stage 1 Intake",
        state_type="started",
        color="#4C6FFF",
        description="Factory intake, clarification, and acceptance criteria shaping.",
    ),
    LinearFactoryStageDefinition(
        key="stage2",
        name="Stage 2 Ticketing",
        state_type="started",
        color="#2E90FA",
        description="Factory ticket shaping, scoping, and definition of done.",
    ),
    LinearFactoryStageDefinition(
        key="stage3",
        name="Stage 3 Build",
        state_type="started",
        color="#12B76A",
        description="Builder and reviewer draft the first implementation pass.",
    ),
    LinearFactoryStageDefinition(
        key="stage4",
        name="Stage 4 Integration",
        state_type="started",
        color="#22C55E",
        description="Prompt, tools, context, and integration artifacts are wired up.",
    ),
    LinearFactoryStageDefinition(
        key="stage5",
        name="Stage 5 Evals",
        state_type="started",
        color="#F79009",
        description="Required evals, regression checks, and latency gates run.",
    ),
    LinearFactoryStageDefinition(
        key="stage6",
        name="Stage 6 Security Review",
        state_type="started",
        color="#F04438",
        description="Security review, sign-off, and threat-surface validation.",
    ),
    LinearFactoryStageDefinition(
        key="stage7",
        name="Stage 7 Staging",
        state_type="started",
        color="#D92D20",
        description="Merge, soak, rollback readiness, and promotion decisions.",
    ),
    LinearFactoryStageDefinition(
        key="stage8",
        name="Stage 8 Production Monitoring",
        state_type="started",
        color="#7A5AF8",
        description="Production monitoring, incidents, and steady-state health.",
    ),
    LinearFactoryStageDefinition(
        key="stage9",
        name="Stage 9 Feedback",
        state_type="completed",
        color="#344054",
        description="Weekly or incident-driven feedback synthesis and learning capture.",
    ),
)

LINEAR_FACTORY_STAGE_BY_KEY = {
    definition.key: definition for definition in LINEAR_FACTORY_STAGES
}
AUTOMATION_STAGE_TO_LINEAR_STAGE = {
    "stage1": "stage1",
    "stage2": "stage2",
    "stage3": "stage3",
    "stage4": "stage4",
    "stage5": "stage5",
    "stage6": "stage6",
    "merge": "stage7",
    "stage7": "stage7",
    "stage8": "stage8",
    "stage9": "stage9",
}


@dataclass(slots=True)
class LinearWorkflowConfig:
    api_key: str
    team_id: str
    graphql_url: str = DEFAULT_LINEAR_GRAPHQL_URL
    trigger_base_url: str | None = None
    trigger_state_id: str | None = None
    create_missing_states: bool = True

    @classmethod
    def maybe_from_env(cls) -> "LinearWorkflowConfig | None":
        if _bool_from_env("LINEAR_FACTORY_SYNC_DISABLED", default=False):
            return None
        if os.environ.get("PYTEST_CURRENT_TEST") and not _bool_from_env(
            "AI_FACTORY_ALLOW_LIVE_LINEAR_IN_TESTS",
            default=False,
        ):
            return None
        api_key = os.environ.get("LINEAR_API_KEY", "").strip()
        team_id = os.environ.get("LINEAR_TARGET_TEAM_ID", "").strip()
        if not api_key or not team_id:
            return None
        graphql_url = os.environ.get("LINEAR_GRAPHQL_URL", "").strip() or DEFAULT_LINEAR_GRAPHQL_URL
        trigger_base_url = os.environ.get("FACTORY_TRIGGER_BASE_URL", "").strip() or None
        trigger_state_id = os.environ.get("LINEAR_TARGET_STATE_ID", "").strip() or None
        return cls(
            api_key=api_key,
            team_id=team_id,
            graphql_url=graphql_url,
            trigger_base_url=trigger_base_url,
            trigger_state_id=trigger_state_id,
            create_missing_states=_bool_from_env("LINEAR_FACTORY_CREATE_STATES", default=True),
        )


@dataclass(slots=True)
class LinearWorkflowBinding:
    issue_id: str
    issue_identifier: str | None = None
    issue_url: str | None = None
    created_by_factory: bool = False
    last_synced_stage_key: str | None = None
    last_synced_state_id: str | None = None
    last_comment_key: str | None = None
    last_comment_id: str | None = None
    updated_at: str = field(default_factory=utc_now)
    version: int = 1

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> "LinearWorkflowBinding":
        return cls(
            issue_id=str(document["issue_id"]),
            issue_identifier=_optional_str(document.get("issue_identifier")),
            issue_url=_optional_str(document.get("issue_url")),
            created_by_factory=bool(document.get("created_by_factory", False)),
            last_synced_stage_key=_optional_str(document.get("last_synced_stage_key")),
            last_synced_state_id=_optional_str(document.get("last_synced_state_id")),
            last_comment_key=_optional_str(document.get("last_comment_key")),
            last_comment_id=_optional_str(document.get("last_comment_id")),
            updated_at=str(document["updated_at"]),
            version=int(document.get("version", 1)),
        )

    def to_document(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "issue_id": self.issue_id,
            "issue_identifier": self.issue_identifier,
            "issue_url": self.issue_url,
            "created_by_factory": self.created_by_factory,
            "last_synced_stage_key": self.last_synced_stage_key,
            "last_synced_state_id": self.last_synced_state_id,
            "last_comment_key": self.last_comment_key,
            "last_comment_id": self.last_comment_id,
            "updated_at": self.updated_at,
        }


@dataclass(slots=True)
class LinearWorkflowSyncCycleResult:
    stage_states: dict[str, dict[str, Any]]
    synced_runs: list[dict[str, Any]]
    skipped_runs: list[dict[str, str]]
    failed_runs: list[dict[str, str]]

    def to_document(self) -> dict[str, Any]:
        return {
            "cycle": "linear-workflow-sync",
            "stage_states": dict(self.stage_states),
            "synced_runs": list(self.synced_runs),
            "skipped_runs": list(self.skipped_runs),
            "failed_runs": list(self.failed_runs),
        }


class LinearWorkflowStore:
    def __init__(self, root: Path, *, repo_root_override: Path | None = None) -> None:
        self.root = root.resolve()
        self.repo_root = repo_root(repo_root_override)
        self.runs_dir = self.root / "runs"

    def binding_path(self, work_item_id: str) -> Path:
        return self.runs_dir / work_item_id / "linear-workflow-binding.json"

    def load_binding(self, work_item_id: str) -> LinearWorkflowBinding | None:
        path = self.binding_path(work_item_id)
        if not path.exists():
            return None
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LinearWorkflowStoreError(
                f"Could not load Linear workflow binding at {path}: {exc}"
            ) from exc
        return LinearWorkflowBinding.from_document(document)

    def save_binding(self, work_item_id: str, binding: LinearWorkflowBinding) -> Path:
        path = self.binding_path(work_item_id)
        self._write_json_atomic(path, binding.to_document())
        return path

    @staticmethod
    def _write_json_atomic(path: Path, document: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            tmp_path.write_text(f"{json.dumps(document, indent=2)}\n", encoding="utf-8")
            os.replace(tmp_path, path)
        finally:
            tmp_path.unlink(missing_ok=True)


class LinearWorkflowSync:
    def __init__(
        self,
        store_dir: Path,
        *,
        repo_root_override: Path | None = None,
        config: LinearWorkflowConfig | None = None,
        linear_client: LinearGraphQLClient | None = None,
    ) -> None:
        resolved_config = config or LinearWorkflowConfig.maybe_from_env()
        if resolved_config is None:
            raise LinearWorkflowConfigurationError(
                "LINEAR_API_KEY and LINEAR_TARGET_TEAM_ID are required for workflow sync."
            )
        self.repo_root = repo_root(repo_root_override)
        self.config = resolved_config
        self.store = LinearWorkflowStore(store_dir, repo_root_override=self.repo_root)
        self.linear_client = linear_client or LinearGraphQLClient(self.config)
        self._stage_states_cache: dict[str, dict[str, Any]] | None = None

    @classmethod
    def maybe_create(
        cls,
        store_dir: Path,
        *,
        repo_root_override: Path | None = None,
    ) -> "LinearWorkflowSync | None":
        config = LinearWorkflowConfig.maybe_from_env()
        if config is None:
            return None
        return cls(
            store_dir,
            repo_root_override=repo_root_override,
            config=config,
        )

    def ensure_stage_states(self, *, force_refresh: bool = False) -> dict[str, dict[str, Any]]:
        try:
            if self._stage_states_cache is not None and not force_refresh:
                return dict(self._stage_states_cache)

            existing_states = {
                state["name"]: dict(state)
                for state in self.linear_client.fetch_team_states(self.config.team_id)
            }
            max_position = 0.0
            for state in existing_states.values():
                position = state.get("position")
                if isinstance(position, (int, float)):
                    max_position = max(max_position, float(position))

            synced: dict[str, dict[str, Any]] = {}
            for offset, definition in enumerate(LINEAR_FACTORY_STAGES, start=1):
                existing = existing_states.get(definition.name)
                if existing is not None:
                    self._validate_existing_stage_state(definition, existing)
                    synced[definition.key] = existing
                    continue
                if not self.config.create_missing_states:
                    raise LinearWorkflowError(
                        f"Linear workflow state '{definition.name}' is missing."
                    )
                created = self.linear_client.create_workflow_state(
                    team_id=self.config.team_id,
                    name=definition.name,
                    state_type=definition.state_type,
                    color=definition.color,
                    description=definition.description,
                    position=max_position + (offset * 100.0),
                )
                synced[definition.key] = created
                existing_states[definition.name] = created

            self._stage_states_cache = dict(synced)
            return dict(synced)
        except LinearGraphQLClientError as exc:
            raise LinearWorkflowError(f"Linear stage-state sync failed: {exc}") from exc

    def sync_existing_runs(
        self,
        *,
        max_runs: int | None = None,
    ) -> LinearWorkflowSyncCycleResult:
        if max_runs is not None and max_runs < 1:
            raise LinearWorkflowError("max_runs must be >= 1 when provided.")

        from .automation import FactoryRunStore, PROGRESSION_SCAN_STAGES, RunLeaseBusyError

        store = FactoryRunStore(self.store.root, repo_root_override=self.repo_root)
        stage_states = self.ensure_stage_states()
        synced_runs: list[dict[str, Any]] = []
        skipped_runs: list[dict[str, str]] = []
        failed_runs: list[dict[str, str]] = []

        for run_dir in store.iter_run_directories():
            if max_runs is not None and len(synced_runs) + len(failed_runs) >= max_runs:
                break
            try:
                with store.run_lease(run_dir.name):
                    candidate = store.load_latest_candidate(run_dir, PROGRESSION_SCAN_STAGES)
            except RunLeaseBusyError:
                skipped_runs.append(
                    {
                        "work_item_id": run_dir.name,
                        "reason": "run_locked",
                    }
                )
                continue
            except Exception as exc:
                failed_runs.append(
                    {
                        "work_item_id": run_dir.name,
                        "reason": str(exc),
                    }
                )
                continue
            if candidate is None:
                skipped_runs.append(
                    {
                        "work_item_id": run_dir.name,
                        "reason": "no_persisted_run_found",
                    }
                )
                continue
            try:
                synced_runs.append(self.sync_stage_result(candidate.stage_name, candidate.document))
            except LinearWorkflowError as exc:
                failed_runs.append(
                    {
                        "work_item_id": candidate.work_item_id,
                        "reason": str(exc),
                    }
                )

        return LinearWorkflowSyncCycleResult(
            stage_states=stage_states,
            synced_runs=synced_runs,
            skipped_runs=skipped_runs,
            failed_runs=failed_runs,
        )

    def sync_stage_result(
        self,
        stage_name: str,
        document: dict[str, Any],
        *,
        stall_reason: str | None = None,
    ) -> dict[str, Any]:
        try:
            linear_stage_key = self._linear_stage_key(stage_name)
            stage_definition = LINEAR_FACTORY_STAGE_BY_KEY[linear_stage_key]
            stage_states = self.ensure_stage_states()
            target_state = stage_states[linear_stage_key]
            work_item = self._require_work_item(document)
            work_item_id = work_item["work_item_id"]

            binding = self.store.load_binding(work_item_id)
            created_new_binding = False
            if binding is None:
                binding = self._bind_issue_for_run(
                    work_item=work_item,
                    stage_name=stage_name,
                    linear_stage_key=linear_stage_key,
                    state_id=str(target_state["id"]),
                    document=document,
                )
                if binding is None:
                    return {
                        "status": "skipped",
                        "work_item_id": work_item_id,
                        "stage_name": stage_name,
                        "linear_stage": stage_definition.name,
                        "reason": "issue_not_created_for_non_actionable_stage1",
                    }
                created_new_binding = True

            previous_stage_key = binding.last_synced_stage_key
            if previous_stage_key != linear_stage_key:
                binding.last_comment_key = None
                binding.last_comment_id = None

            state_update = "unchanged"
            if binding.last_synced_state_id != str(target_state["id"]):
                issue = self.linear_client.update_issue_state(
                    binding.issue_id,
                    str(target_state["id"]),
                )
                binding.issue_identifier = _optional_str(issue.get("identifier")) or binding.issue_identifier
                binding.issue_url = _optional_str(issue.get("url")) or binding.issue_url
                binding.last_synced_state_id = str(target_state["id"])
                state_update = "moved"

            comment = self._maybe_post_stall_comment(
                binding,
                stage_name=stage_name,
                linear_stage_key=linear_stage_key,
                document=document,
                stall_reason=stall_reason,
            )

            binding.last_synced_stage_key = linear_stage_key
            if binding.last_synced_state_id is None:
                binding.last_synced_state_id = str(target_state["id"])
            binding.updated_at = utc_now()
            self.store.save_binding(work_item_id, binding)

            return {
                "status": "synced",
                "work_item_id": work_item_id,
                "issue_id": binding.issue_id,
                "issue_identifier": binding.issue_identifier,
                "issue_url": binding.issue_url,
                "stage_name": stage_name,
                "linear_stage": stage_definition.name,
                "state_id": str(target_state["id"]),
                "state_update": (
                    "created" if created_new_binding and binding.created_by_factory and state_update == "unchanged"
                    else state_update
                ),
                "comment": comment,
                "created_by_factory": binding.created_by_factory,
            }
        except LinearGraphQLClientError as exc:
            raise LinearWorkflowError(f"Linear issue sync failed: {exc}") from exc

    def _bind_issue_for_run(
        self,
        *,
        work_item: dict[str, Any],
        stage_name: str,
        linear_stage_key: str,
        state_id: str,
        document: dict[str, Any],
    ) -> LinearWorkflowBinding | None:
        existing_issue_id = self._source_linear_issue_id(work_item)
        if existing_issue_id is not None:
            snapshot = self.linear_client.fetch_issue_snapshot(existing_issue_id)
            return LinearWorkflowBinding(
                issue_id=snapshot.id,
                issue_identifier=snapshot.identifier,
                issue_url=snapshot.url,
                created_by_factory=False,
            )

        existing_factory_issue = self._existing_factory_issue_binding(work_item)
        if existing_factory_issue is not None:
            return existing_factory_issue

        if not self._should_create_issue(stage_name, document):
            return None

        issue = self.linear_client.create_issue(
            team_id=self.config.team_id,
            title=self._issue_title(work_item),
            description=self._issue_description(
                stage_name=stage_name,
                linear_stage_key=linear_stage_key,
                document=document,
            ),
            state_id=state_id,
        )
        return LinearWorkflowBinding(
            issue_id=str(issue["id"]),
            issue_identifier=_optional_str(issue.get("identifier")),
            issue_url=_optional_str(issue.get("url")),
            created_by_factory=True,
            last_synced_state_id=state_id,
        )

    def _existing_factory_issue_binding(
        self,
        work_item: dict[str, Any],
    ) -> LinearWorkflowBinding | None:
        work_item_id = work_item.get("work_item_id")
        if not isinstance(work_item_id, str) or not work_item_id:
            return None
        existing_issue = self.linear_client.find_factory_issue_by_work_item(
            team_id=self.config.team_id,
            work_item_id=work_item_id,
        )
        if existing_issue is None:
            return None
        return LinearWorkflowBinding(
            issue_id=str(existing_issue["id"]),
            issue_identifier=_optional_str(existing_issue.get("identifier")),
            issue_url=_optional_str(existing_issue.get("url")),
            created_by_factory=True,
        )

    def _maybe_post_stall_comment(
        self,
        binding: LinearWorkflowBinding,
        *,
        stage_name: str,
        linear_stage_key: str,
        document: dict[str, Any],
        stall_reason: str | None,
    ) -> dict[str, Any]:
        comment_payload = self._stall_comment_payload(
            stage_name=stage_name,
            linear_stage_key=linear_stage_key,
            document=document,
            stall_reason=stall_reason,
        )
        if comment_payload is None:
            return {"status": "skipped", "reason": "no_human_attention_needed"}
        comment_key, body = comment_payload
        if comment_key == binding.last_comment_key:
            return {"status": "skipped", "reason": "duplicate_comment"}
        comment_id = self.linear_client.create_comment(binding.issue_id, body)
        binding.last_comment_key = comment_key
        binding.last_comment_id = comment_id
        return {"status": "posted", "comment_id": comment_id}

    def _stall_comment_payload(
        self,
        *,
        stage_name: str,
        linear_stage_key: str,
        document: dict[str, Any],
        stall_reason: str | None,
    ) -> tuple[str, str] | None:
        stage_definition = LINEAR_FACTORY_STAGE_BY_KEY[linear_stage_key]
        work_item = self._require_work_item(document)
        state = ControllerState(work_item["state"])
        summary = self._stage_stall_summary(stage_name, state, document, stall_reason=stall_reason)
        if summary is None:
            return None
        reason, findings = summary
        normalized_reason = normalize_whitespace(reason)
        normalized_findings = [normalize_whitespace(item) for item in findings if item]
        fingerprint_seed = json.dumps(
            {
                "stage": linear_stage_key,
                "reason": normalized_reason,
                "findings": normalized_findings,
            },
            sort_keys=True,
        )
        comment_key = hashlib.sha1(fingerprint_seed.encode("utf-8")).hexdigest()

        lines = [
            f"AI Factory is waiting in `{stage_definition.name}`.",
            "",
            normalized_reason,
        ]
        if normalized_findings:
            lines.append("")
            lines.append("What needs attention:")
            lines.extend(f"- {item}" for item in normalized_findings[:5])
        lines.extend(
            [
                "",
                f"- Work item: `{work_item['work_item_id']}`",
                f"- Controller state: `{state.value}`",
            ]
        )
        if self.config.trigger_base_url:
            lines.append(
                f"- Cockpit: {self.config.trigger_base_url.rstrip('/')} (filter for `{work_item['work_item_id']}`)"
            )
        return comment_key, "\n".join(lines)

    def _stage_stall_summary(
        self,
        stage_name: str,
        state: ControllerState,
        document: dict[str, Any],
        *,
        stall_reason: str | None,
    ) -> tuple[str, list[str]] | None:
        if stall_reason:
            reason = self._humanize_stall_reason(stall_reason)
            findings = self._fallback_findings(stage_name, document)
            return reason, findings

        if stage_name == "stage1":
            decision = self._stage1_decision(document)
            if decision == "active_build_candidate":
                return None
            rationale = self._stage1_rationale(document)
            return (
                f"Stage 1 classified this item as `{decision}` instead of an active build candidate.",
                [rationale] if rationale else [],
            )

        if stage_name == "stage3" and state is ControllerState.PR_REVISION:
            return (
                "Stage 3 review found blocking issues that need another builder pass.",
                self._findings(document.get("pr_packet"), "reviewer_report", "blocking_findings"),
            )

        if stage_name == "stage5" and state is ControllerState.PR_REVISION:
            findings = self._findings(document.get("pr_packet"), "reviewer_report", "blocking_findings")
            if not findings:
                findings = self._findings(document.get("eval_report"), "summary", "failing_merge_gate_tiers")
            return (
                "Stage 5 eval gates did not pass cleanly.",
                findings,
            )

        if stage_name == "stage6":
            if state is ControllerState.SECURITY_REVIEWING:
                findings = self._findings(document.get("security_review"), "summary", "watch_findings")
                return (
                    "Security review is waiting on explicit human sign-off.",
                    findings or ["Security sign-off is still pending."],
                )
            if state is ControllerState.PR_REVISION:
                return (
                    "Security review found blocking issues that must be fixed before release.",
                    self._findings(document.get("security_review"), "summary", "blocking_findings"),
                )

        if stage_name == "merge":
            if state is ControllerState.MERGE_REVIEWING:
                findings = self._findings(document.get("merge_decision"), "summary", "watch_findings")
                return (
                    "Merge is waiting on explicit human approval.",
                    findings or ["Merge approval is still pending."],
                )
            if state is ControllerState.PR_REVISION:
                return (
                    "Merge checks blocked promotion back to the release path.",
                    self._findings(document.get("merge_decision"), "summary", "blocking_findings"),
                )

        if stage_name == "stage7":
            if state is ControllerState.STAGING_SOAK:
                decision = self._nested_value(document.get("promotion_decision"), "promotion_decision", "status")
                if decision == "pending_human":
                    findings = self._findings(document.get("promotion_decision"), "summary", "watch_items")
                    return (
                        "Stage 7 completed the soak checks and is waiting on release approval.",
                        findings or ["Release approval is still pending."],
                    )
                soak_minutes = self._nested_value(
                    document.get("promotion_decision"),
                    "staging_report",
                    "soak_minutes_observed",
                )
                minimum_soak = self._nested_value(
                    document.get("promotion_decision"),
                    "staging_report",
                    "minimum_soak_minutes",
                )
                request_samples = self._nested_value(
                    document.get("promotion_decision"),
                    "staging_report",
                    "request_samples_observed",
                )
                minimum_samples = self._nested_value(
                    document.get("promotion_decision"),
                    "staging_report",
                    "minimum_request_samples",
                )
                details: list[str] = []
                if soak_minutes is not None and minimum_soak is not None:
                    details.append(
                        f"Soak observed {soak_minutes} / {minimum_soak} required minutes."
                    )
                if request_samples is not None and minimum_samples is not None:
                    details.append(
                        f"Request samples observed {request_samples} / {minimum_samples} required."
                    )
                return (
                    "Stage 7 staging soak is still in progress.",
                    details,
                )
            if state is ControllerState.PR_REVISION:
                return (
                    "Stage 7 found rollout or soak blockers and sent the change back for revision.",
                    self._findings(document.get("promotion_decision"), "summary", "threshold_breaches"),
                )

        if stage_name == "stage8":
            decision = self._nested_value(document.get("monitoring_report"), "monitoring_decision", "status")
            if decision in {"auto_mitigated", "human_escalated"}:
                return (
                    "Production monitoring detected a regression that needs human visibility.",
                    self._findings(document.get("monitoring_report"), "summary", "regressions")
                    or self._findings(document.get("pr_packet"), "reviewer_report", "blocking_findings"),
                )

        return None

    def _fallback_findings(self, stage_name: str, document: dict[str, Any]) -> list[str]:
        if stage_name == "stage3":
            return self._findings(document.get("pr_packet"), "reviewer_report", "blocking_findings")
        if stage_name == "stage5":
            return self._findings(document.get("pr_packet"), "reviewer_report", "blocking_findings")
        if stage_name == "stage6":
            return self._findings(document.get("security_review"), "summary", "blocking_findings")
        if stage_name == "merge":
            return self._findings(document.get("merge_decision"), "summary", "blocking_findings")
        if stage_name == "stage7":
            return self._findings(document.get("promotion_decision"), "summary", "threshold_breaches")
        if stage_name == "stage8":
            return self._findings(document.get("monitoring_report"), "summary", "regressions")
        return []

    @staticmethod
    def _findings(document: Any, section_key: str, findings_key: str) -> list[str]:
        if not isinstance(document, dict):
            return []
        section = document.get(section_key)
        if not isinstance(section, dict):
            return []
        findings = section.get(findings_key)
        if not isinstance(findings, list):
            return []
        return [normalize_whitespace(str(item)) for item in findings if item]

    @staticmethod
    def _nested_value(document: Any, *path: str) -> Any:
        current = document
        for part in path:
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current

    @staticmethod
    def _linear_stage_key(stage_name: str) -> str:
        try:
            return AUTOMATION_STAGE_TO_LINEAR_STAGE[stage_name]
        except KeyError as exc:
            raise LinearWorkflowError(
                f"Unsupported automation stage '{stage_name}' for Linear sync."
            ) from exc

    @staticmethod
    def _require_work_item(document: dict[str, Any]) -> dict[str, str]:
        work_item = document.get("work_item")
        if not isinstance(work_item, dict):
            raise LinearWorkflowError("Stage result is missing work_item for Linear sync.")
        if not isinstance(work_item.get("work_item_id"), str):
            raise LinearWorkflowError("Stage result is missing work_item.work_item_id for Linear sync.")
        if not isinstance(work_item.get("state"), str):
            raise LinearWorkflowError("Stage result is missing work_item.state for Linear sync.")
        return work_item

    @staticmethod
    def _source_linear_issue_id(work_item: dict[str, Any]) -> str | None:
        if work_item.get("source_provider") != "linear":
            return None
        external_id = work_item.get("source_external_id")
        if not isinstance(external_id, str):
            return None
        if external_id.startswith("linear:"):
            parts = external_id.split(":")
            if len(parts) >= 2 and parts[1]:
                return parts[1]
        return None

    @staticmethod
    def _stage1_decision(document: dict[str, Any]) -> str:
        if isinstance(document.get("policy_decision"), dict):
            decision = document["policy_decision"].get("decision")
            if isinstance(decision, str):
                return decision
        relevance = document.get("spec_packet", {}).get("relevance")
        if isinstance(relevance, dict) and isinstance(relevance.get("decision"), str):
            return relevance["decision"]
        raise LinearWorkflowError("Stage 1 result is missing an intake decision.")

    @staticmethod
    def _stage1_rationale(document: dict[str, Any]) -> str | None:
        relevance = document.get("spec_packet", {}).get("relevance")
        if isinstance(relevance, dict) and isinstance(relevance.get("rationale"), str):
            return normalize_whitespace(relevance["rationale"])
        return None

    def _should_create_issue(self, stage_name: str, document: dict[str, Any]) -> bool:
        if stage_name != "stage1":
            return True
        return self._stage1_decision(document) == "active_build_candidate"

    @staticmethod
    def _issue_title(work_item: dict[str, Any]) -> str:
        title = normalize_whitespace(str(work_item.get("title") or "Untitled factory work item"))
        if title.lower().startswith("ai factory:"):
            return title
        return f"AI Factory: {title}"

    def _issue_description(
        self,
        *,
        stage_name: str,
        linear_stage_key: str,
        document: dict[str, Any],
    ) -> str:
        work_item = self._require_work_item(document)
        stage_definition = LINEAR_FACTORY_STAGE_BY_KEY[linear_stage_key]
        source_provider = str(work_item.get("source_provider") or "unknown")
        source_external_id = str(work_item.get("source_external_id") or "unknown")
        source_url = self._source_url(document)
        problem = self._problem_summary(document)
        acceptance = self._acceptance_criteria(document)

        lines = [
            "This issue is synchronized automatically by the AI Factory.",
            "",
            f"- Work item: `{work_item['work_item_id']}`",
            f"- Source provider: `{source_provider}`",
            f"- Source reference: `{source_external_id}`",
            f"- Current factory stage: `{stage_definition.name}`",
        ]
        if source_url:
            lines.append(f"- Source URL: {source_url}")
        if self.config.trigger_base_url:
            lines.append(
                f"- Cockpit: {self.config.trigger_base_url.rstrip('/')} (filter for `{work_item['work_item_id']}`)"
            )
        lines.extend(["", "Problem summary:", problem or "No summary was available."])
        if acceptance:
            lines.extend(["", "Acceptance criteria:"])
            lines.extend(f"- {criterion}" for criterion in acceptance[:8])
        if stage_name == "stage1":
            rationale = self._stage1_rationale(document)
            if rationale:
                lines.extend(["", "Stage 1 rationale:", rationale])
        return "\n".join(lines)

    @staticmethod
    def _problem_summary(document: dict[str, Any]) -> str:
        spec_packet = document.get("spec_packet")
        if isinstance(spec_packet, dict):
            summary = spec_packet.get("summary")
            if isinstance(summary, dict) and isinstance(summary.get("problem"), str):
                return normalize_whitespace(summary["problem"])
        source_item = document.get("source_item")
        if isinstance(source_item, dict) and isinstance(source_item.get("body"), str):
            return normalize_whitespace(source_item["body"])
        return ""

    @staticmethod
    def _acceptance_criteria(document: dict[str, Any]) -> list[str]:
        spec_packet = document.get("spec_packet")
        if not isinstance(spec_packet, dict):
            return []
        summary = spec_packet.get("summary")
        if not isinstance(summary, dict):
            return []
        criteria = summary.get("acceptance_criteria")
        if not isinstance(criteria, list):
            return []
        return [normalize_whitespace(str(item)) for item in criteria if item]

    @staticmethod
    def _source_url(document: dict[str, Any]) -> str | None:
        source_item = document.get("source_item")
        if isinstance(source_item, dict) and isinstance(source_item.get("url"), str):
            return source_item["url"]
        source = document.get("spec_packet", {}).get("source")
        if isinstance(source, dict) and isinstance(source.get("url"), str):
            return source["url"]
        return None

    @staticmethod
    def _humanize_stall_reason(reason: str) -> str:
        mapping = {
            "non_actionable_state": "The run is in a non-actionable state and needs a human decision on whether to continue.",
            "dead_letter_state": "The run moved to dead letter and needs human investigation before retrying.",
            "awaiting_security_signoff": "The factory is waiting on explicit security sign-off.",
            "awaiting_merge_signoff": "The factory is waiting on explicit merge approval.",
            "awaiting_release_signoff": "The factory is waiting on release approval after staging.",
            "already_in_production_monitoring": "The run is already in production monitoring and no further automatic move is pending.",
            "non_model_touching_progression_not_supported": "This non-model change still needs the next non-model automation path implemented.",
            "pr_ready_for_human_merge_deploy": "The PR is ready for human merge and deploy; production-mode automation stops before merge, staging, and monitoring.",
            "awaiting_builder_follow_up": "The builder needs to make another revision before the run can continue.",
            "run_locked": "Another worker currently owns this run lease.",
            "no_persisted_run_found": "No persisted run bundle could be found for this work item.",
            "no_autonomous_progress_available": "The factory does not have an automatic next step for the current state yet.",
        }
        if reason in mapping:
            return mapping[reason]
        return normalize_whitespace(reason.replace("_", " ").rstrip(".")).capitalize() + "."

    @staticmethod
    def _validate_existing_stage_state(
        definition: LinearFactoryStageDefinition,
        existing: dict[str, Any],
    ) -> None:
        existing_type = _optional_str(existing.get("type"))
        if existing_type != definition.state_type:
            raise LinearWorkflowError(
                "Linear workflow state "
                f"'{definition.name}' already exists with type '{existing_type}', "
                f"expected '{definition.state_type}'."
            )
