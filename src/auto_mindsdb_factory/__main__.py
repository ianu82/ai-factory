from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from .automation import AutomationError, FactoryAutomationCoordinator, FactoryRunStore
from .build_review import BuildReviewError, Stage3BuildReviewPipeline
from .connectors import (
    FactoryConnectorError,
    OpenAIResponsesAgentConfig,
    OpenAIResponsesAgentConnector,
)
from .contracts import main as validate_contracts_main
from .controller import FactoryController, WorkItem
from .eval_execution import EvalExecutionError, Stage5EvalPipeline
from .feedback_synthesis import (
    FeedbackSynthesisError,
    Stage9FeedbackSynthesisPipeline,
)
from .integration import IntegrationError, Stage4IntegrationPipeline
from .intake import AnthropicScout, IntakeError, Stage1IntakePipeline, build_manual_intake_item
from .linear_trigger import (
    LinearTriggerError,
    LinearTriggerWorker,
    serve_linear_webhooks,
)
from .linear_workflow import (
    LinearWorkflowConfig,
    LinearWorkflowError,
    LinearWorkflowSync,
)
from .merge_orchestration import MergeError, StageMergePipeline
from .policy import PolicyEngine
from .production_monitoring import (
    ProductionMonitoringError,
    Stage8ProductionMonitoringPipeline,
)
from .production_runtime import (
    FactoryDoctor,
    FactoryWorker,
    ProductionRuntimeConfig,
    intake_paused,
)
from .release_staging import ReleaseStagingError, Stage7ReleaseStagingPipeline
from .security_review import SecurityReviewError, Stage6SecurityReviewPipeline
from .ticketing import Stage2TicketingPipeline, TicketingError
from .vertical_slice import (
    FactoryVerticalSliceRunner,
    VerticalSliceConfig,
    VerticalSliceError,
    build_cockpit_summary,
)

_ENV_FILE_NAMES = (".env", ".env.local")
_ENV_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class EnvironmentSetupError(RuntimeError):
    """Raised when repo-local environment files cannot be parsed safely."""


def _argv_repo_root(argv: list[str] | None) -> Path | None:
    if not argv:
        return None
    for index, argument in enumerate(argv):
        if argument == "--repo-root":
            if index + 1 >= len(argv):
                return None
            return Path(argv[index + 1]).expanduser().resolve()
        if argument.startswith("--repo-root="):
            return Path(argument.split("=", 1)[1]).expanduser().resolve()
    return None


def _env_search_roots(argv: list[str] | None) -> list[Path]:
    roots = [Path.cwd().resolve()]
    repo_root_override = _argv_repo_root(argv)
    if repo_root_override is not None and repo_root_override not in roots:
        roots.append(repo_root_override)
    return roots


def _decode_env_value(raw_value: str, *, path: Path, line_number: int) -> str:
    if not raw_value:
        return ""
    if raw_value[0] not in {"'", '"'}:
        return raw_value
    quote = raw_value[0]
    if len(raw_value) < 2 or raw_value[-1] != quote:
        raise EnvironmentSetupError(
            f"{path}:{line_number} has an unterminated quoted value."
        )
    inner = raw_value[1:-1]
    if quote == "'":
        return inner
    try:
        return bytes(inner, "utf-8").decode("unicode_escape")
    except UnicodeDecodeError as exc:
        raise EnvironmentSetupError(
            f"{path}:{line_number} contains an invalid escape sequence."
        ) from exc


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise EnvironmentSetupError(f"Could not read environment file {path}: {exc}") from exc

    for line_number, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export ") :].lstrip()
        if "=" not in stripped:
            raise EnvironmentSetupError(
                f"{path}:{line_number} must use KEY=VALUE format."
            )
        name, raw_value = stripped.split("=", 1)
        name = name.strip()
        if not _ENV_NAME_PATTERN.fullmatch(name):
            raise EnvironmentSetupError(
                f"{path}:{line_number} has an invalid variable name '{name}'."
            )
        values[name] = _decode_env_value(raw_value.strip(), path=path, line_number=line_number)
    return values


def _load_local_env_files(argv: list[str] | None = None) -> list[Path]:
    if os.environ.get("AI_FACTORY_SKIP_ENV_FILES") == "1":
        return []

    protected = set(os.environ)
    loaded_paths: list[Path] = []
    for root in _env_search_roots(argv):
        for filename in _ENV_FILE_NAMES:
            path = root / filename
            if not path.is_file():
                continue
            for name, value in _parse_env_file(path).items():
                if name in protected:
                    continue
                os.environ[name] = value
            loaded_paths.append(path)
    return loaded_paths


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="auto-mindsdb-factory",
        description="Factory validation and controller utilities.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser(
        "validate-contracts",
        help="Validate schemas, fixtures, and scenario invariants.",
    )
    validate_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    policy_parser = subparsers.add_parser(
        "evaluate-policy",
        help="Evaluate lane and policy output for a spec candidate.",
    )
    policy_parser.add_argument("--spec-packet-id", required=True)
    policy_parser.add_argument(
        "--decision",
        required=True,
        choices=[
            "ignore",
            "watchlist",
            "backlog_candidate",
            "active_build_candidate",
        ],
    )
    policy_parser.add_argument(
        "--flag",
        action="append",
        default=[],
        help="Risk flag to include. Can be passed multiple times.",
    )
    policy_parser.add_argument(
        "--reasoning",
        action="append",
        default=[],
        help="Reasoning line to include. Can be passed multiple times.",
    )
    policy_parser.add_argument(
        "--artifact-id",
        default="policy-cli-001",
        help="Artifact id to assign to the generated policy decision.",
    )
    policy_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    demo_parser = subparsers.add_parser(
        "demo-controller",
        help="Replay a fixture scenario through the controller state machine.",
    )
    demo_parser.add_argument(
        "scenario",
        type=Path,
        help="Scenario directory under fixtures/scenarios.",
    )

    scout_parser = subparsers.add_parser(
        "scout-anthropic",
        help="Fetch and normalize Anthropic release-note items.",
    )
    scout_parser.add_argument(
        "--html-file",
        type=Path,
        default=None,
        help="Use a local HTML file instead of fetching the live page.",
    )
    scout_parser.add_argument(
        "--source-url",
        default=None,
        help="Override the Anthropic release-notes URL.",
    )
    scout_parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of normalized items to print.",
    )
    scout_parser.add_argument(
        "--detected-at",
        default=None,
        help="Override the detection timestamp.",
    )

    stage1_parser = subparsers.add_parser(
        "stage1-intake",
        help="Run the Stage 1 intake flow for one Anthropic release-note item.",
    )
    stage1_parser.add_argument(
        "--html-file",
        type=Path,
        default=None,
        help="Use a local HTML file instead of fetching the live page.",
    )
    stage1_parser.add_argument(
        "--source-url",
        default=None,
        help="Override the Anthropic release-notes URL.",
    )
    stage1_parser.add_argument(
        "--entry-index",
        type=int,
        default=0,
        help="Zero-based index into the normalized release-note item list.",
    )
    stage1_parser.add_argument(
        "--detected-at",
        default=None,
        help="Override the detection timestamp.",
    )
    stage1_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    stage1_manual_parser = subparsers.add_parser(
        "stage1-intake-manual",
        help="Run the Stage 1 intake flow for a manually supplied issue or change request.",
    )
    stage1_manual_parser.add_argument(
        "--title",
        required=True,
        help="Short title for the manual intake item.",
    )
    stage1_manual_parser.add_argument(
        "--body",
        required=True,
        help="Detailed body for the manual intake item.",
    )
    stage1_manual_parser.add_argument(
        "--url",
        required=True,
        help="Canonical URL for the manual intake item.",
    )
    stage1_manual_parser.add_argument(
        "--provider",
        default="manual",
        help="Provider label recorded on the manual intake item.",
    )
    stage1_manual_parser.add_argument(
        "--external-id",
        default=None,
        help="Stable external id for the manual intake item.",
    )
    stage1_manual_parser.add_argument(
        "--published-at",
        default=None,
        help="Optional published timestamp or ISO date for the source item.",
    )
    stage1_manual_parser.add_argument(
        "--detected-at",
        default=None,
        help="Override the detection timestamp.",
    )
    stage1_manual_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    stage2_parser = subparsers.add_parser(
        "stage2-ticketing",
        help="Run the Stage 2 ticket/eval planning flow for an active-build item.",
    )
    stage2_parser.add_argument(
        "--stage1-result-file",
        type=Path,
        default=None,
        help="Path to a JSON document emitted by stage1-intake.",
    )
    stage2_parser.add_argument(
        "--spec-packet-file",
        type=Path,
        default=None,
        help="Path to spec-packet JSON.",
    )
    stage2_parser.add_argument(
        "--policy-decision-file",
        type=Path,
        default=None,
        help="Path to policy-decision JSON.",
    )
    stage2_parser.add_argument(
        "--work-item-file",
        type=Path,
        default=None,
        help="Path to work-item JSON.",
    )
    stage2_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )
    _add_agent_args(stage2_parser)

    stage3_parser = subparsers.add_parser(
        "stage3-build-review",
        help="Run the Stage 3 builder/reviewer flow for a ticketed work item.",
    )
    stage3_parser.add_argument(
        "--stage2-result-file",
        type=Path,
        default=None,
        help="Path to a JSON document emitted by stage2-ticketing.",
    )
    stage3_parser.add_argument(
        "--spec-packet-file",
        type=Path,
        default=None,
        help="Path to spec-packet JSON.",
    )
    stage3_parser.add_argument(
        "--policy-decision-file",
        type=Path,
        default=None,
        help="Path to policy-decision JSON.",
    )
    stage3_parser.add_argument(
        "--ticket-bundle-file",
        type=Path,
        default=None,
        help="Path to ticket-bundle JSON.",
    )
    stage3_parser.add_argument(
        "--eval-manifest-file",
        type=Path,
        default=None,
        help="Path to eval-manifest JSON.",
    )
    stage3_parser.add_argument(
        "--work-item-file",
        type=Path,
        default=None,
        help="Path to work-item JSON.",
    )
    stage3_parser.add_argument(
        "--repository",
        default="mindsdb/platform",
        help="Repository name to embed in the draft PR packet.",
    )
    stage3_parser.add_argument(
        "--blocking-finding",
        action="append",
        default=[],
        help="Inject a reviewer blocking finding. Can be passed multiple times.",
    )
    stage3_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )
    _add_agent_args(stage3_parser)

    stage4_parser = subparsers.add_parser(
        "stage4-integration",
        help="Run the Stage 4 integration-design flow for a reviewable PR.",
    )
    stage4_parser.add_argument(
        "--stage3-result-file",
        type=Path,
        default=None,
        help="Path to a JSON document emitted by stage3-build-review.",
    )
    stage4_parser.add_argument(
        "--spec-packet-file",
        type=Path,
        default=None,
        help="Path to spec-packet JSON.",
    )
    stage4_parser.add_argument(
        "--policy-decision-file",
        type=Path,
        default=None,
        help="Path to policy-decision JSON.",
    )
    stage4_parser.add_argument(
        "--ticket-bundle-file",
        type=Path,
        default=None,
        help="Path to ticket-bundle JSON.",
    )
    stage4_parser.add_argument(
        "--eval-manifest-file",
        type=Path,
        default=None,
        help="Path to eval-manifest JSON.",
    )
    stage4_parser.add_argument(
        "--pr-packet-file",
        type=Path,
        default=None,
        help="Path to pr-packet JSON.",
    )
    stage4_parser.add_argument(
        "--work-item-file",
        type=Path,
        default=None,
        help="Path to work-item JSON.",
    )
    stage4_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    stage5_parser = subparsers.add_parser(
        "stage5-eval",
        help="Run the Stage 5 eval execution flow for a reviewable PR.",
    )
    stage5_parser.add_argument(
        "--stage4-result-file",
        type=Path,
        default=None,
        help="Path to a JSON document emitted by stage4-integration.",
    )
    stage5_parser.add_argument(
        "--spec-packet-file",
        type=Path,
        default=None,
        help="Path to spec-packet JSON.",
    )
    stage5_parser.add_argument(
        "--policy-decision-file",
        type=Path,
        default=None,
        help="Path to policy-decision JSON.",
    )
    stage5_parser.add_argument(
        "--ticket-bundle-file",
        type=Path,
        default=None,
        help="Path to ticket-bundle JSON.",
    )
    stage5_parser.add_argument(
        "--eval-manifest-file",
        type=Path,
        default=None,
        help="Path to eval-manifest JSON.",
    )
    stage5_parser.add_argument(
        "--pr-packet-file",
        type=Path,
        default=None,
        help="Path to pr-packet JSON.",
    )
    stage5_parser.add_argument(
        "--prompt-contract-file",
        type=Path,
        default=None,
        help="Path to prompt-contract JSON.",
    )
    stage5_parser.add_argument(
        "--tool-schema-file",
        type=Path,
        default=None,
        help="Path to tool-schema JSON.",
    )
    stage5_parser.add_argument(
        "--golden-dataset-file",
        type=Path,
        default=None,
        help="Path to golden-dataset JSON.",
    )
    stage5_parser.add_argument(
        "--latency-baseline-file",
        type=Path,
        default=None,
        help="Path to latency-baseline JSON.",
    )
    stage5_parser.add_argument(
        "--work-item-file",
        type=Path,
        default=None,
        help="Path to work-item JSON.",
    )
    stage5_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    stage6_parser = subparsers.add_parser(
        "stage6-security-review",
        help="Run the Stage 6 security-review flow for an eval-approved PR.",
    )
    stage6_parser.add_argument(
        "--stage5-result-file",
        type=Path,
        default=None,
        help="Path to a JSON document emitted by stage5-eval.",
    )
    stage6_parser.add_argument(
        "--spec-packet-file",
        type=Path,
        default=None,
        help="Path to spec-packet JSON.",
    )
    stage6_parser.add_argument(
        "--policy-decision-file",
        type=Path,
        default=None,
        help="Path to policy-decision JSON.",
    )
    stage6_parser.add_argument(
        "--ticket-bundle-file",
        type=Path,
        default=None,
        help="Path to ticket-bundle JSON.",
    )
    stage6_parser.add_argument(
        "--eval-manifest-file",
        type=Path,
        default=None,
        help="Path to eval-manifest JSON.",
    )
    stage6_parser.add_argument(
        "--pr-packet-file",
        type=Path,
        default=None,
        help="Path to pr-packet JSON.",
    )
    stage6_parser.add_argument(
        "--prompt-contract-file",
        type=Path,
        default=None,
        help="Path to prompt-contract JSON.",
    )
    stage6_parser.add_argument(
        "--tool-schema-file",
        type=Path,
        default=None,
        help="Path to tool-schema JSON.",
    )
    stage6_parser.add_argument(
        "--golden-dataset-file",
        type=Path,
        default=None,
        help="Path to golden-dataset JSON.",
    )
    stage6_parser.add_argument(
        "--latency-baseline-file",
        type=Path,
        default=None,
        help="Path to latency-baseline JSON.",
    )
    stage6_parser.add_argument(
        "--eval-report-file",
        type=Path,
        default=None,
        help="Path to eval-report JSON.",
    )
    stage6_parser.add_argument(
        "--work-item-file",
        type=Path,
        default=None,
        help="Path to work-item JSON.",
    )
    stage6_parser.add_argument(
        "--approved-security-reviewer",
        action="append",
        default=[],
        help="Record a human security reviewer approval. Can be passed multiple times.",
    )
    stage6_parser.add_argument(
        "--security-blocking-finding",
        action="append",
        default=[],
        help="Inject a blocking security finding. Can be passed multiple times.",
    )
    stage6_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    merge_parser = subparsers.add_parser(
        "stage-merge",
        help="Run merge orchestration for a security-approved PR candidate.",
    )
    merge_parser.add_argument(
        "--stage6-result-file",
        type=Path,
        default=None,
        help="Path to a JSON document emitted by stage6-security-review.",
    )
    merge_parser.add_argument(
        "--spec-packet-file",
        type=Path,
        default=None,
        help="Path to spec-packet JSON.",
    )
    merge_parser.add_argument(
        "--policy-decision-file",
        type=Path,
        default=None,
        help="Path to policy-decision JSON.",
    )
    merge_parser.add_argument(
        "--ticket-bundle-file",
        type=Path,
        default=None,
        help="Path to ticket-bundle JSON.",
    )
    merge_parser.add_argument(
        "--eval-manifest-file",
        type=Path,
        default=None,
        help="Path to eval-manifest JSON.",
    )
    merge_parser.add_argument(
        "--pr-packet-file",
        type=Path,
        default=None,
        help="Path to pr-packet JSON.",
    )
    merge_parser.add_argument(
        "--prompt-contract-file",
        type=Path,
        default=None,
        help="Path to prompt-contract JSON.",
    )
    merge_parser.add_argument(
        "--tool-schema-file",
        type=Path,
        default=None,
        help="Path to tool-schema JSON.",
    )
    merge_parser.add_argument(
        "--golden-dataset-file",
        type=Path,
        default=None,
        help="Path to golden-dataset JSON.",
    )
    merge_parser.add_argument(
        "--latency-baseline-file",
        type=Path,
        default=None,
        help="Path to latency-baseline JSON.",
    )
    merge_parser.add_argument(
        "--eval-report-file",
        type=Path,
        default=None,
        help="Path to eval-report JSON.",
    )
    merge_parser.add_argument(
        "--security-review-file",
        type=Path,
        default=None,
        help="Path to security-review JSON.",
    )
    merge_parser.add_argument(
        "--work-item-file",
        type=Path,
        default=None,
        help="Path to work-item JSON.",
    )
    merge_parser.add_argument(
        "--approved-merge-reviewer",
        action="append",
        default=[],
        help="Record a human merge approval. Can be passed multiple times.",
    )
    merge_parser.add_argument(
        "--merge-blocking-finding",
        action="append",
        default=[],
        help="Inject a blocking merge finding. Can be passed multiple times.",
    )
    merge_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    stage7_parser = subparsers.add_parser(
        "stage7-release-staging",
        help="Run the Stage 7 staging-soak and promotion flow for a merge-ready PR.",
    )
    stage7_parser.add_argument(
        "--stage6-result-file",
        type=Path,
        default=None,
        help="Path to a JSON document emitted by stage6-security-review.",
    )
    stage7_parser.add_argument(
        "--merge-result-file",
        type=Path,
        default=None,
        help="Path to a JSON document emitted by stage-merge.",
    )
    stage7_parser.add_argument(
        "--spec-packet-file",
        type=Path,
        default=None,
        help="Path to spec-packet JSON.",
    )
    stage7_parser.add_argument(
        "--policy-decision-file",
        type=Path,
        default=None,
        help="Path to policy-decision JSON.",
    )
    stage7_parser.add_argument(
        "--ticket-bundle-file",
        type=Path,
        default=None,
        help="Path to ticket-bundle JSON.",
    )
    stage7_parser.add_argument(
        "--eval-manifest-file",
        type=Path,
        default=None,
        help="Path to eval-manifest JSON.",
    )
    stage7_parser.add_argument(
        "--pr-packet-file",
        type=Path,
        default=None,
        help="Path to pr-packet JSON.",
    )
    stage7_parser.add_argument(
        "--prompt-contract-file",
        type=Path,
        default=None,
        help="Path to prompt-contract JSON.",
    )
    stage7_parser.add_argument(
        "--tool-schema-file",
        type=Path,
        default=None,
        help="Path to tool-schema JSON.",
    )
    stage7_parser.add_argument(
        "--golden-dataset-file",
        type=Path,
        default=None,
        help="Path to golden-dataset JSON.",
    )
    stage7_parser.add_argument(
        "--latency-baseline-file",
        type=Path,
        default=None,
        help="Path to latency-baseline JSON.",
    )
    stage7_parser.add_argument(
        "--eval-report-file",
        type=Path,
        default=None,
        help="Path to eval-report JSON.",
    )
    stage7_parser.add_argument(
        "--security-review-file",
        type=Path,
        default=None,
        help="Path to security-review JSON.",
    )
    stage7_parser.add_argument(
        "--merge-decision-file",
        type=Path,
        default=None,
        help="Path to merge-decision JSON.",
    )
    stage7_parser.add_argument(
        "--work-item-file",
        type=Path,
        default=None,
        help="Path to work-item JSON.",
    )
    stage7_parser.add_argument(
        "--approved-release-reviewer",
        action="append",
        default=[],
        help="Record a human release approval. Can be passed multiple times.",
    )
    stage7_parser.add_argument(
        "--observed-soak-minutes",
        type=int,
        default=None,
        help="Override the observed staging soak duration in minutes.",
    )
    stage7_parser.add_argument(
        "--observed-request-samples",
        type=int,
        default=None,
        help="Override the observed staging request sample count.",
    )
    stage7_parser.add_argument(
        "--metric-override",
        action="append",
        default=[],
        help="Override a staging metric as key=value. Can be passed multiple times.",
    )
    stage7_parser.add_argument(
        "--skip-rollback-test",
        action="store_true",
        help="Mark the rollback path as untested to force a staging gate failure.",
    )
    stage7_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    stage8_parser = subparsers.add_parser(
        "stage8-production-monitoring",
        help="Run the Stage 8 production monitoring flow for a promoted release.",
    )
    stage8_parser.add_argument(
        "--stage7-result-file",
        type=Path,
        default=None,
        help="Path to a JSON document emitted by stage7-release-staging.",
    )
    stage8_parser.add_argument(
        "--spec-packet-file",
        type=Path,
        default=None,
        help="Path to spec-packet JSON.",
    )
    stage8_parser.add_argument(
        "--policy-decision-file",
        type=Path,
        default=None,
        help="Path to policy-decision JSON.",
    )
    stage8_parser.add_argument(
        "--ticket-bundle-file",
        type=Path,
        default=None,
        help="Path to ticket-bundle JSON.",
    )
    stage8_parser.add_argument(
        "--eval-manifest-file",
        type=Path,
        default=None,
        help="Path to eval-manifest JSON.",
    )
    stage8_parser.add_argument(
        "--pr-packet-file",
        type=Path,
        default=None,
        help="Path to pr-packet JSON.",
    )
    stage8_parser.add_argument(
        "--prompt-contract-file",
        type=Path,
        default=None,
        help="Path to prompt-contract JSON.",
    )
    stage8_parser.add_argument(
        "--tool-schema-file",
        type=Path,
        default=None,
        help="Path to tool-schema JSON.",
    )
    stage8_parser.add_argument(
        "--golden-dataset-file",
        type=Path,
        default=None,
        help="Path to golden-dataset JSON.",
    )
    stage8_parser.add_argument(
        "--latency-baseline-file",
        type=Path,
        default=None,
        help="Path to latency-baseline JSON.",
    )
    stage8_parser.add_argument(
        "--eval-report-file",
        type=Path,
        default=None,
        help="Path to eval-report JSON.",
    )
    stage8_parser.add_argument(
        "--security-review-file",
        type=Path,
        default=None,
        help="Path to security-review JSON.",
    )
    stage8_parser.add_argument(
        "--merge-decision-file",
        type=Path,
        default=None,
        help="Path to merge-decision JSON.",
    )
    stage8_parser.add_argument(
        "--promotion-decision-file",
        type=Path,
        default=None,
        help="Path to promotion-decision JSON.",
    )
    stage8_parser.add_argument(
        "--work-item-file",
        type=Path,
        default=None,
        help="Path to work-item JSON.",
    )
    stage8_parser.add_argument(
        "--observed-window-minutes",
        type=int,
        default=None,
        help="Override the observed production monitoring window in minutes.",
    )
    stage8_parser.add_argument(
        "--metric-override",
        action="append",
        default=[],
        help="Override a production metric as key=value. Can be passed multiple times.",
    )
    stage8_parser.add_argument(
        "--security-anomaly",
        action="store_true",
        help="Simulate a critical production safety or security anomaly.",
    )
    stage8_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    stage9_parser = subparsers.add_parser(
        "stage9-feedback-synthesis",
        help="Run the Stage 9 feedback synthesis flow for a monitored production release.",
    )
    stage9_parser.add_argument(
        "--stage8-result-file",
        type=Path,
        default=None,
        help="Path to a JSON document emitted by stage8-production-monitoring.",
    )
    stage9_parser.add_argument(
        "--spec-packet-file",
        type=Path,
        default=None,
        help="Path to spec-packet JSON.",
    )
    stage9_parser.add_argument(
        "--policy-decision-file",
        type=Path,
        default=None,
        help="Path to policy-decision JSON.",
    )
    stage9_parser.add_argument(
        "--ticket-bundle-file",
        type=Path,
        default=None,
        help="Path to ticket-bundle JSON.",
    )
    stage9_parser.add_argument(
        "--eval-manifest-file",
        type=Path,
        default=None,
        help="Path to eval-manifest JSON.",
    )
    stage9_parser.add_argument(
        "--pr-packet-file",
        type=Path,
        default=None,
        help="Path to pr-packet JSON.",
    )
    stage9_parser.add_argument(
        "--prompt-contract-file",
        type=Path,
        default=None,
        help="Path to prompt-contract JSON.",
    )
    stage9_parser.add_argument(
        "--tool-schema-file",
        type=Path,
        default=None,
        help="Path to tool-schema JSON.",
    )
    stage9_parser.add_argument(
        "--golden-dataset-file",
        type=Path,
        default=None,
        help="Path to golden-dataset JSON.",
    )
    stage9_parser.add_argument(
        "--latency-baseline-file",
        type=Path,
        default=None,
        help="Path to latency-baseline JSON.",
    )
    stage9_parser.add_argument(
        "--eval-report-file",
        type=Path,
        default=None,
        help="Path to eval-report JSON.",
    )
    stage9_parser.add_argument(
        "--security-review-file",
        type=Path,
        default=None,
        help="Path to security-review JSON.",
    )
    stage9_parser.add_argument(
        "--merge-decision-file",
        type=Path,
        default=None,
        help="Path to merge-decision JSON.",
    )
    stage9_parser.add_argument(
        "--promotion-decision-file",
        type=Path,
        default=None,
        help="Path to promotion-decision JSON.",
    )
    stage9_parser.add_argument(
        "--monitoring-report-file",
        type=Path,
        default=None,
        help="Path to monitoring-report JSON.",
    )
    stage9_parser.add_argument(
        "--work-item-file",
        type=Path,
        default=None,
        help="Path to work-item JSON.",
    )
    stage9_parser.add_argument(
        "--feedback-window-days",
        type=int,
        default=7,
        help="Override the Stage 9 synthesis lookback window in days.",
    )
    stage9_parser.add_argument(
        "--unexpected-user-behavior",
        action="append",
        default=[],
        help="Record an unexpected production user behavior. Can be passed multiple times.",
    )
    stage9_parser.add_argument(
        "--positive-surprise",
        action="append",
        default=[],
        help="Record a positive production surprise. Can be passed multiple times.",
    )
    stage9_parser.add_argument(
        "--spec-mismatch",
        action="append",
        default=[],
        help="Record a manual spec mismatch. Can be passed multiple times.",
    )
    stage9_parser.add_argument(
        "--eval-miss",
        action="append",
        default=[],
        help="Record a manual eval miss. Can be passed multiple times.",
    )
    stage9_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    register_parser = subparsers.add_parser(
        "automation-register-bundle",
        help="Persist a Stage 1, Stage 8, or Stage 9 result bundle for recurring automation.",
    )
    register_parser.add_argument(
        "--stage",
        required=True,
        choices=[
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
        ],
        help="Stage result type to persist.",
    )
    register_parser.add_argument(
        "--result-file",
        type=Path,
        required=True,
        help="Path to the stage result JSON document to register.",
    )
    register_parser.add_argument(
        "--store-dir",
        type=Path,
        required=True,
        help="Directory where automation state and run bundles are persisted.",
    )
    register_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )
    register_parser.add_argument(
        "--advance-immediately",
        action="store_true",
        help="Trigger an immediate per-work-item handoff after persisting the bundle.",
    )
    register_parser.add_argument(
        "--repository",
        default="mindsdb/platform",
        help="Repository name to embed in generated PR packets when immediate handoff runs.",
    )

    linear_webhook_parser = subparsers.add_parser(
        "linear-webhook-server",
        help="Serve the Linear issue webhook intake endpoint.",
    )
    linear_webhook_parser.add_argument(
        "--store-dir",
        type=Path,
        default=Path(".factory-automation"),
        help="Directory where Linear trigger inbox and state are persisted.",
    )
    linear_webhook_parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host interface to bind the Linear webhook server.",
    )
    linear_webhook_parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port to bind the Linear webhook server.",
    )
    linear_webhook_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    linear_cycle_parser = subparsers.add_parser(
        "automation-linear-trigger-cycle",
        help="Drain persisted Linear trigger events into Stage 1 manual intake and immediate handoff.",
    )
    linear_cycle_parser.add_argument(
        "--store-dir",
        type=Path,
        required=True,
        help="Directory where Linear trigger state and automation runs are persisted.",
    )
    linear_cycle_parser.add_argument(
        "--repository",
        default="mindsdb/platform",
        help="Repository name to embed in generated PR packets when automation advances work.",
    )
    linear_cycle_parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Limit how many queued Linear trigger events are processed in one cycle.",
    )
    linear_cycle_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    linear_stage_setup_parser = subparsers.add_parser(
        "linear-ensure-stage-states",
        aliases=["linear-stage-setup"],
        help="Create or reuse the Linear workflow states that mirror Factory stages 1-9.",
    )
    linear_stage_setup_parser.add_argument(
        "--store-dir",
        type=Path,
        default=Path(".factory-automation"),
        help="Directory where automation runs and Linear workflow bindings are persisted.",
    )
    linear_stage_setup_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )
    linear_stage_setup_parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Verify that stage states exist instead of creating missing Linear workflow states.",
    )

    linear_sync_parser = subparsers.add_parser(
        "automation-linear-sync-cycle",
        help="Backfill or resync persisted factory runs into Linear workflow issues and stage states.",
    )
    linear_sync_parser.add_argument(
        "--store-dir",
        type=Path,
        required=True,
        help="Directory where automation runs and Linear workflow bindings are persisted.",
    )
    linear_sync_parser.add_argument(
        "--max-runs",
        type=int,
        default=None,
        help="Limit how many persisted runs are synced in one pass.",
    )
    linear_sync_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    automation_stage1_parser = subparsers.add_parser(
        "automation-stage1-cycle",
        help="Run the recurring Stage 1 scout/intake cycle and persist new work items.",
    )
    automation_stage1_parser.add_argument(
        "--store-dir",
        type=Path,
        required=True,
        help="Directory where automation state and run bundles are persisted.",
    )
    automation_stage1_parser.add_argument(
        "--html-file",
        type=Path,
        default=None,
        help="Use a local HTML file instead of fetching the live page.",
    )
    automation_stage1_parser.add_argument(
        "--source-url",
        default=None,
        help="Override the Anthropic release-notes URL.",
    )
    automation_stage1_parser.add_argument(
        "--detected-at",
        default=None,
        help="Override the detection timestamp for the scout cycle.",
    )
    automation_stage1_parser.add_argument(
        "--max-new-items",
        type=int,
        default=None,
        help="Limit how many previously unseen upstream items are admitted in one cycle.",
    )
    automation_stage1_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )
    automation_stage1_parser.add_argument(
        "--advance-immediately",
        action="store_true",
        help="Immediately hand off each newly created work item into the Stage 2-8 progression lane.",
    )
    automation_stage1_parser.add_argument(
        "--repository",
        default="mindsdb/platform",
        help="Repository name to embed in generated PR packets when immediate handoff runs.",
    )

    automation_progress_parser = subparsers.add_parser(
        "automation-advance-runs",
        help="Advance stored runs through autonomous Stages 2 through 8 until they hit a gate.",
    )
    automation_progress_parser.add_argument(
        "--store-dir",
        type=Path,
        required=True,
        help="Directory where automation state and run bundles are persisted.",
    )
    automation_progress_parser.add_argument(
        "--repository",
        default="mindsdb/platform",
        help="Repository name to embed in generated PR packets.",
    )
    automation_progress_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )
    automation_progress_parser.add_argument(
        "--autonomy-mode",
        choices=["simulation_full", "pr_ready"],
        default=os.environ.get("AI_FACTORY_AUTONOMY_MODE", "simulation_full"),
        help="How far autonomous progression is allowed to go.",
    )

    factory_worker_parser = subparsers.add_parser(
        "factory-worker",
        help="Run the production worker loop: drain Linear triggers, advance runs, and sync stages.",
    )
    factory_worker_parser.add_argument(
        "--store-dir",
        type=Path,
        required=True,
        help="Directory where automation state and run bundles are persisted.",
    )
    factory_worker_parser.add_argument(
        "--repository",
        default="ianu82/ai-factory",
        help="GitHub repository that receives factory implementation PRs.",
    )
    factory_worker_parser.add_argument(
        "--base-branch",
        default="main",
        help="Base branch for factory implementation PRs.",
    )
    factory_worker_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )
    factory_worker_parser.add_argument(
        "--interval-seconds",
        type=float,
        default=30.0,
        help="Delay between worker cycles when not using --once.",
    )
    factory_worker_parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Limit how many queued Linear trigger events are processed in one cycle.",
    )
    factory_worker_parser.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one worker cycle and exit.",
    )
    factory_worker_parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Run at most this many cycles and exit.",
    )
    factory_worker_parser.add_argument(
        "--autonomy-mode",
        choices=["simulation_full", "pr_ready"],
        default=os.environ.get("AI_FACTORY_AUTONOMY_MODE", "pr_ready"),
        help="Production defaults to pr_ready, which stops before merge/deploy.",
    )

    factory_doctor_parser = subparsers.add_parser(
        "factory-doctor",
        help="Validate production runtime configuration and local dependencies.",
    )
    factory_doctor_parser.add_argument(
        "--store-dir",
        type=Path,
        default=Path(".factory-automation"),
        help="Directory where automation state and run bundles are persisted.",
    )
    factory_doctor_parser.add_argument(
        "--repository",
        default="ianu82/ai-factory",
        help="GitHub repository used by the production factory.",
    )
    factory_doctor_parser.add_argument(
        "--base-branch",
        default="main",
        help="Base branch for factory implementation PRs.",
    )
    factory_doctor_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )
    factory_doctor_parser.add_argument(
        "--autonomy-mode",
        choices=["simulation_full", "pr_ready"],
        default=os.environ.get("AI_FACTORY_AUTONOMY_MODE", "pr_ready"),
        help="Runtime autonomy mode to validate.",
    )

    automation_stage9_parser = subparsers.add_parser(
        "automation-weekly-feedback",
        help="Run the recurring weekly Stage 9 synthesis pass over stored production runs.",
    )
    automation_stage9_parser.add_argument(
        "--store-dir",
        type=Path,
        required=True,
        help="Directory where automation state and run bundles are persisted.",
    )
    automation_stage9_parser.add_argument(
        "--window-label",
        default=None,
        help="Override the ISO week window key in YYYY-Www form.",
    )
    automation_stage9_parser.add_argument(
        "--feedback-window-days",
        type=int,
        default=7,
        help="Lookback window recorded on the generated Stage 9 feedback reports.",
    )
    automation_stage9_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    automation_supervisor_parser = subparsers.add_parser(
        "automation-supervisor-cycle",
        help="Run a full automation supervisor pass across Stage 1 intake, progression, and optional weekly feedback.",
    )
    automation_supervisor_parser.add_argument(
        "--store-dir",
        type=Path,
        required=True,
        help="Directory where automation state and run bundles are persisted.",
    )
    automation_supervisor_parser.add_argument(
        "--html-file",
        type=Path,
        default=None,
        help="Use a local HTML file instead of fetching the live page for Stage 1.",
    )
    automation_supervisor_parser.add_argument(
        "--source-url",
        default=None,
        help="Override the Anthropic release-notes URL for Stage 1 intake.",
    )
    automation_supervisor_parser.add_argument(
        "--detected-at",
        default=None,
        help="Override the detection timestamp for the Stage 1 scout cycle.",
    )
    automation_supervisor_parser.add_argument(
        "--max-new-items",
        type=int,
        default=None,
        help="Limit how many previously unseen upstream items are admitted in one cycle.",
    )
    automation_supervisor_parser.add_argument(
        "--advance-immediately",
        action="store_true",
        help="Immediately hand off each newly created work item before the progression pass.",
    )
    automation_supervisor_parser.add_argument(
        "--repository",
        default="mindsdb/platform",
        help="Repository name to embed in generated PR packets when automation advances work.",
    )
    automation_supervisor_parser.add_argument(
        "--run-weekly-feedback",
        action="store_true",
        help="Also run the weekly Stage 9 synthesis pass after Stage 1 and progression complete.",
    )
    automation_supervisor_parser.add_argument(
        "--window-label",
        default=None,
        help="Override the ISO week window key in YYYY-Www form when weekly feedback runs.",
    )
    automation_supervisor_parser.add_argument(
        "--feedback-window-days",
        type=int,
        default=7,
        help="Lookback window recorded on generated Stage 9 feedback reports when weekly feedback runs.",
    )
    automation_supervisor_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )

    vertical_slice_parser = subparsers.add_parser(
        "factory-vertical-slice",
        help="Run one release-note-to-feedback slice with real GitHub PR evidence.",
    )
    vertical_slice_parser.add_argument(
        "--store-dir",
        type=Path,
        default=Path(".factory-automation"),
        help="Directory where vertical-slice run bundles and ops signals are persisted.",
    )
    vertical_slice_parser.add_argument(
        "--repository",
        default="ianu82/ai-factory",
        help="GitHub repository that receives the factory-generated PR.",
    )
    vertical_slice_parser.add_argument(
        "--base-branch",
        default="main",
        help="Base branch for the factory-generated PR.",
    )
    vertical_slice_parser.add_argument(
        "--html-file",
        type=Path,
        default=None,
        help="Use a local Anthropic release-note HTML file instead of the default fixture.",
    )
    vertical_slice_parser.add_argument(
        "--source-url",
        default=None,
        help="Fetch Anthropic release notes from this URL instead of using a local fixture.",
    )
    vertical_slice_parser.add_argument(
        "--entry-index",
        type=int,
        default=0,
        help="Release-note item index to use for the slice.",
    )
    vertical_slice_parser.add_argument(
        "--require-existing-ops-signals",
        action="store_true",
        help="Require staging, monitoring, and rollback signal files to exist before running.",
    )
    vertical_slice_parser.add_argument(
        "--feedback-window-days",
        type=int,
        default=7,
        help="Lookback window recorded on the generated Stage 9 feedback report.",
    )
    vertical_slice_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )
    _add_agent_args(vertical_slice_parser)

    cockpit_parser = subparsers.add_parser(
        "factory-cockpit",
        help="Summarize persisted factory runs, gates, PRs, monitoring, and feedback.",
    )
    cockpit_parser.add_argument(
        "--store-dir",
        type=Path,
        default=Path(".factory-automation"),
        help="Directory where automation and vertical-slice bundles are persisted.",
    )
    cockpit_parser.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Override the repository root.",
    )
    cockpit_parser.add_argument(
        "--stale-heartbeat-seconds",
        type=int,
        default=300,
        help="Mark active work as possibly stuck when its heartbeat is older than this many seconds.",
    )

    return parser


class CommandInputError(RuntimeError):
    """Raised when a CLI artifact or input file cannot be loaded safely."""


def _add_agent_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--agent-provider",
        choices=["none", "openai"],
        default=os.environ.get("AI_FACTORY_AGENT_PROVIDER", "none"),
        help="Optional live agent provider for judgment-heavy drafting steps.",
    )
    parser.add_argument(
        "--agent-model",
        default=None,
        help="Override the OpenAI model id. Defaults to AI_FACTORY_OPENAI_MODEL or gpt-5.4.",
    )
    parser.add_argument(
        "--agent-fallback-model",
        default=None,
        help="Optional fallback OpenAI model id if the primary model request fails.",
    )
    parser.add_argument(
        "--agent-reasoning-effort",
        choices=["none", "minimal", "low", "medium", "high", "xhigh"],
        default=None,
        help="Override OpenAI reasoning effort. Defaults to AI_FACTORY_OPENAI_REASONING_EFFORT or medium.",
    )
    parser.add_argument(
        "--agent-max-output-tokens",
        type=int,
        default=None,
        help="Override OpenAI max_output_tokens for agent tasks.",
    )
    parser.add_argument(
        "--agent-timeout-seconds",
        type=int,
        default=None,
        help="Override the OpenAI request timeout in seconds.",
    )
    parser.add_argument(
        "--agent-base-url",
        default=None,
        help="Override the OpenAI Responses API base URL.",
    )


def _build_agent_connector(args: argparse.Namespace):
    provider = getattr(args, "agent_provider", "none")
    if provider == "none":
        return None
    if provider != "openai":
        raise CommandInputError(f"Unsupported agent provider: {provider}")
    try:
        config = OpenAIResponsesAgentConfig.from_env(
            model=args.agent_model,
            fallback_model=args.agent_fallback_model,
            reasoning_effort=args.agent_reasoning_effort,
            max_output_tokens=args.agent_max_output_tokens,
            timeout_seconds=args.agent_timeout_seconds,
            base_url=args.agent_base_url,
        )
        return OpenAIResponsesAgentConnector(config)
    except FactoryConnectorError as exc:
        raise CommandInputError(f"Could not initialize the OpenAI agent connector: {exc}") from exc


def _read_text_file(path: Path, label: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise CommandInputError(f"Could not read {label} at {path}: {exc}") from exc


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    payload = _read_text_file(path, label)
    try:
        document = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise CommandInputError(
            f"{label} at {path} is not valid JSON: {exc.msg} (line {exc.lineno}, column {exc.colno})."
        ) from exc
    if not isinstance(document, dict):
        raise CommandInputError(f"{label} at {path} must contain a top-level JSON object.")
    return document


def _required_mapping(document: dict[str, Any], key: str, label: str) -> dict[str, Any]:
    value = document.get(key)
    if not isinstance(value, dict):
        raise CommandInputError(f"{label} is missing the required object field '{key}'.")
    return value


def _optional_mapping(document: dict[str, Any], key: str, label: str) -> dict[str, Any] | None:
    value = document.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise CommandInputError(f"{label} has a non-object '{key}' field.")
    return value


def _load_work_item(document: dict[str, Any], label: str) -> WorkItem:
    work_item_document = document
    if "work_item" in document:
        nested_work_item = document.get("work_item")
        if not isinstance(nested_work_item, dict):
            raise CommandInputError(f"{label} has a non-object 'work_item' field.")
        work_item_document = dict(nested_work_item)
        history = document.get("history")
        if isinstance(history, list) and "history" not in work_item_document:
            work_item_document["history"] = history
    try:
        return WorkItem.from_document(work_item_document)
    except (KeyError, TypeError, ValueError) as exc:
        raise CommandInputError(f"{label} is not a valid work-item document: {exc}") from exc


def _load_stage1_result(path: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    document = _read_json_object(path, "Stage 1 result")
    return (
        _required_mapping(document, "spec_packet", "Stage 1 result"),
        _required_mapping(document, "policy_decision", "Stage 1 result"),
        _required_mapping(document, "work_item", "Stage 1 result"),
    )


def _load_stage2_result(
    path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    document = _read_json_object(path, "Stage 2 result")
    return (
        _required_mapping(document, "spec_packet", "Stage 2 result"),
        _required_mapping(document, "policy_decision", "Stage 2 result"),
        _required_mapping(document, "ticket_bundle", "Stage 2 result"),
        _required_mapping(document, "eval_manifest", "Stage 2 result"),
        _required_mapping(document, "work_item", "Stage 2 result"),
    )


def _load_stage3_result(
    path: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    document = _read_json_object(path, "Stage 3 result")
    return (
        _required_mapping(document, "spec_packet", "Stage 3 result"),
        _required_mapping(document, "policy_decision", "Stage 3 result"),
        _required_mapping(document, "ticket_bundle", "Stage 3 result"),
        _required_mapping(document, "eval_manifest", "Stage 3 result"),
        _required_mapping(document, "pr_packet", "Stage 3 result"),
        _required_mapping(document, "work_item", "Stage 3 result"),
    )


def _load_stage4_result(
    path: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    document = _read_json_object(path, "Stage 4 result")
    return (
        _required_mapping(document, "spec_packet", "Stage 4 result"),
        _required_mapping(document, "policy_decision", "Stage 4 result"),
        _required_mapping(document, "ticket_bundle", "Stage 4 result"),
        _required_mapping(document, "eval_manifest", "Stage 4 result"),
        _required_mapping(document, "pr_packet", "Stage 4 result"),
        _required_mapping(document, "prompt_contract", "Stage 4 result"),
        _required_mapping(document, "tool_schema", "Stage 4 result"),
        _required_mapping(document, "golden_dataset", "Stage 4 result"),
        _required_mapping(document, "latency_baseline", "Stage 4 result"),
        _required_mapping(document, "work_item", "Stage 4 result"),
    )


def _load_stage5_result(
    path: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    document = _read_json_object(path, "Stage 5 result")
    return (
        _required_mapping(document, "spec_packet", "Stage 5 result"),
        _required_mapping(document, "policy_decision", "Stage 5 result"),
        _required_mapping(document, "ticket_bundle", "Stage 5 result"),
        _required_mapping(document, "eval_manifest", "Stage 5 result"),
        _required_mapping(document, "pr_packet", "Stage 5 result"),
        _required_mapping(document, "prompt_contract", "Stage 5 result"),
        _required_mapping(document, "tool_schema", "Stage 5 result"),
        _required_mapping(document, "golden_dataset", "Stage 5 result"),
        _required_mapping(document, "latency_baseline", "Stage 5 result"),
        _required_mapping(document, "eval_report", "Stage 5 result"),
        _required_mapping(document, "work_item", "Stage 5 result"),
    )


def _load_stage6_result(
    path: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    document = _read_json_object(path, "Stage 6 result")
    return (
        _required_mapping(document, "spec_packet", "Stage 6 result"),
        _required_mapping(document, "policy_decision", "Stage 6 result"),
        _required_mapping(document, "ticket_bundle", "Stage 6 result"),
        _required_mapping(document, "eval_manifest", "Stage 6 result"),
        _required_mapping(document, "pr_packet", "Stage 6 result"),
        _required_mapping(document, "prompt_contract", "Stage 6 result"),
        _required_mapping(document, "tool_schema", "Stage 6 result"),
        _required_mapping(document, "golden_dataset", "Stage 6 result"),
        _required_mapping(document, "latency_baseline", "Stage 6 result"),
        _required_mapping(document, "eval_report", "Stage 6 result"),
        _required_mapping(document, "security_review", "Stage 6 result"),
        _required_mapping(document, "work_item", "Stage 6 result"),
    )


def _load_merge_result(
    path: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    document = _read_json_object(path, "Merge result")
    return (
        _required_mapping(document, "spec_packet", "Merge result"),
        _required_mapping(document, "policy_decision", "Merge result"),
        _required_mapping(document, "ticket_bundle", "Merge result"),
        _required_mapping(document, "eval_manifest", "Merge result"),
        _required_mapping(document, "pr_packet", "Merge result"),
        _required_mapping(document, "prompt_contract", "Merge result"),
        _required_mapping(document, "tool_schema", "Merge result"),
        _required_mapping(document, "golden_dataset", "Merge result"),
        _required_mapping(document, "latency_baseline", "Merge result"),
        _required_mapping(document, "eval_report", "Merge result"),
        _required_mapping(document, "security_review", "Merge result"),
        _required_mapping(document, "merge_decision", "Merge result"),
        _required_mapping(document, "work_item", "Merge result"),
    )


def _load_stage7_result(
    path: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any] | None,
    dict[str, Any],
    dict[str, Any],
]:
    document = _read_json_object(path, "Stage 7 result")
    return (
        _required_mapping(document, "spec_packet", "Stage 7 result"),
        _required_mapping(document, "policy_decision", "Stage 7 result"),
        _required_mapping(document, "ticket_bundle", "Stage 7 result"),
        _required_mapping(document, "eval_manifest", "Stage 7 result"),
        _required_mapping(document, "pr_packet", "Stage 7 result"),
        _required_mapping(document, "prompt_contract", "Stage 7 result"),
        _required_mapping(document, "tool_schema", "Stage 7 result"),
        _required_mapping(document, "golden_dataset", "Stage 7 result"),
        _required_mapping(document, "latency_baseline", "Stage 7 result"),
        _required_mapping(document, "eval_report", "Stage 7 result"),
        _required_mapping(document, "security_review", "Stage 7 result"),
        _optional_mapping(document, "merge_decision", "Stage 7 result"),
        _required_mapping(document, "promotion_decision", "Stage 7 result"),
        _required_mapping(document, "work_item", "Stage 7 result"),
    )


def _load_stage8_result(
    path: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any] | None,
    dict[str, Any],
    dict[str, Any],
]:
    document = _read_json_object(path, "Stage 8 result")
    return (
        _required_mapping(document, "spec_packet", "Stage 8 result"),
        _required_mapping(document, "policy_decision", "Stage 8 result"),
        _required_mapping(document, "ticket_bundle", "Stage 8 result"),
        _required_mapping(document, "eval_manifest", "Stage 8 result"),
        _required_mapping(document, "pr_packet", "Stage 8 result"),
        _required_mapping(document, "prompt_contract", "Stage 8 result"),
        _required_mapping(document, "tool_schema", "Stage 8 result"),
        _required_mapping(document, "golden_dataset", "Stage 8 result"),
        _required_mapping(document, "latency_baseline", "Stage 8 result"),
        _required_mapping(document, "eval_report", "Stage 8 result"),
        _required_mapping(document, "security_review", "Stage 8 result"),
        _optional_mapping(document, "merge_decision", "Stage 8 result"),
        _required_mapping(document, "promotion_decision", "Stage 8 result"),
        _required_mapping(document, "monitoring_report", "Stage 8 result"),
        _required_mapping(document, "work_item", "Stage 8 result"),
    )


def _parse_metric_overrides(values: list[str]) -> dict[str, float | int]:
    overrides: dict[str, float | int] = {}
    for raw_value in values:
        if "=" not in raw_value:
            raise CommandInputError(
                f"Metric override '{raw_value}' must be in key=value format."
            )
        key, value = raw_value.split("=", 1)
        key = key.strip()
        if not key:
            raise CommandInputError(
                f"Metric override '{raw_value}' must include a metric name."
            )
        try:
            parsed: float | int
            if "." in value:
                parsed = float(value)
            else:
                parsed = int(value)
        except ValueError as exc:
            raise CommandInputError(
                f"Metric override '{raw_value}' has a non-numeric value."
            ) from exc
        overrides[key] = parsed
    return overrides


def main(argv: list[str] | None = None) -> int:
    try:
        _load_local_env_files(argv)
    except EnvironmentSetupError as exc:
        print(f"Environment setup failed: {exc}", file=sys.stderr)
        return 1

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "validate-contracts":
        command_argv: list[str] = []
        if args.repo_root is not None:
            command_argv.extend(["--repo-root", str(args.repo_root)])
        return validate_contracts_main(command_argv)

    if args.command == "evaluate-policy":
        engine = PolicyEngine(args.repo_root)
        reasoning = args.reasoning or ["CLI evaluation requested."]
        decision = engine.evaluate_change(
            spec_packet_id=args.spec_packet_id,
            decision=args.decision,
            flags=args.flag,
            reasoning=reasoning,
            artifact_id=args.artifact_id,
        )
        print(json.dumps(decision, indent=2))
        return 0

    if args.command == "demo-controller":
        controller = FactoryController()
        replayed = controller.replay_scenario(args.scenario)
        print(json.dumps(replayed.to_document(), indent=2))
        return 0

    if args.command == "scout-anthropic":
        if args.limit < 1:
            parser.error("--limit must be >= 1")
        try:
            html = _read_text_file(args.html_file, "HTML source") if args.html_file is not None else None
        except CommandInputError as exc:
            print(f"Stage 1 scout failed: {exc}", file=sys.stderr)
            return 1
        scout = AnthropicScout(source_url=args.source_url)
        try:
            items = scout.list_items(html=html, detected_at=args.detected_at)
        except IntakeError as exc:
            print(f"Stage 1 scout failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps([item.to_document() for item in items[: args.limit]], indent=2))
        return 0

    if args.command == "automation-register-bundle":
        try:
            document = _read_json_object(args.result_file, "Automation stage result")
            coordinator = FactoryAutomationCoordinator(
                args.store_dir,
                repo_root_override=args.repo_root,
            )
            stored_path, state = coordinator.register_bundle(args.stage, document)
        except (AutomationError, LinearWorkflowError, CommandInputError) as exc:
            print(f"Automation bundle registration failed: {exc}", file=sys.stderr)
            return 1
        handoff = None
        if args.advance_immediately:
            handoff = coordinator.run_immediate_handoff(
                FactoryRunStore.extract_work_item_id(document),
                raise_on_failure=False,
                repository=args.repository,
            )
        print(
            json.dumps(
                {
                    "stage": args.stage,
                    "stored_path": str(stored_path),
                    "work_item_id": FactoryRunStore.extract_work_item_id(document),
                    "advance_immediately": args.advance_immediately,
                    "handoff": None if handoff is None else handoff.to_document(),
                    "automation_state": state.to_document(),
                },
                indent=2,
            )
        )
        if handoff is not None and handoff.status == "failed":
            print(
                "Automation immediate handoff failed: "
                f"{handoff.reason or 'unknown handoff error'}",
                file=sys.stderr,
            )
            return 1
        return 0

    if args.command == "linear-webhook-server":
        try:
            serve_linear_webhooks(
                store_dir=args.store_dir,
                host=args.host,
                port=args.port,
                repo_root_override=args.repo_root,
            )
        except (LinearTriggerError, OSError) as exc:
            print(f"Linear webhook server failed: {exc}", file=sys.stderr)
            return 1
        return 0

    if args.command == "automation-linear-trigger-cycle":
        if intake_paused():
            print(
                json.dumps(
                    {
                        "cycle": "linear-trigger",
                        "status": "skipped",
                        "reason": "intake_paused",
                        "processed_events": [],
                        "skipped_events": [],
                        "failed_events": [],
                    },
                    indent=2,
                )
            )
            return 0
        try:
            worker = LinearTriggerWorker(
                args.store_dir,
                repo_root_override=args.repo_root,
            )
            result = worker.run_cycle(
                repository=args.repository,
                max_events=args.max_events,
            )
        except (LinearTriggerError, LinearWorkflowError, AutomationError, IntakeError) as exc:
            print(f"Linear trigger cycle failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        if result.failed_events:
            print(
                "Linear trigger cycle recorded failures: "
                f"{result.failed_events[0].get('reason', 'unknown error')}",
                file=sys.stderr,
            )
            return 1
        failed_handoffs = result.failed_handoffs()
        if failed_handoffs:
            print(
                "Linear trigger handoff failed: "
                f"{failed_handoffs[0]['handoff'].get('reason', 'unknown handoff error')}",
                file=sys.stderr,
            )
            return 1
        return 0

    if args.command in {"linear-ensure-stage-states", "linear-stage-setup"}:
        try:
            config = None
            if args.verify_only:
                config = LinearWorkflowConfig.maybe_from_env()
                if config is None:
                    raise LinearWorkflowError(
                        "LINEAR_API_KEY and LINEAR_TARGET_TEAM_ID are required for workflow sync."
                    )
                config.create_missing_states = False
            if config is None:
                sync = LinearWorkflowSync(
                    args.store_dir,
                    repo_root_override=args.repo_root,
                )
            else:
                sync = LinearWorkflowSync(
                    args.store_dir,
                    repo_root_override=args.repo_root,
                    config=config,
                )
            stage_states = sync.ensure_stage_states()
        except LinearWorkflowError as exc:
            print(f"Linear stage setup failed: {exc}", file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "cycle": "linear-stage-setup",
                    "stage_states": stage_states,
                },
                indent=2,
            )
        )
        return 0

    if args.command == "automation-linear-sync-cycle":
        try:
            sync = LinearWorkflowSync(
                args.store_dir,
                repo_root_override=args.repo_root,
            )
            result = sync.sync_existing_runs(max_runs=args.max_runs)
        except LinearWorkflowError as exc:
            print(f"Linear sync cycle failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        if result.failed_runs:
            print(
                "Linear sync cycle recorded failures: "
                f"{result.failed_runs[0].get('reason', 'unknown error')}",
                file=sys.stderr,
            )
            return 1
        return 0

    if args.command == "automation-stage1-cycle":
        try:
            html = _read_text_file(args.html_file, "HTML source") if args.html_file is not None else None
            coordinator = FactoryAutomationCoordinator(
                args.store_dir,
                repo_root_override=args.repo_root,
            )
            result = coordinator.run_stage1_cycle(
                html=html,
                source_url=args.source_url,
                detected_at=args.detected_at,
                max_new_items=args.max_new_items,
                advance_immediately=args.advance_immediately,
                raise_on_failed_handoff=False,
                repository=args.repository,
            )
        except (AutomationError, LinearWorkflowError, IntakeError, CommandInputError) as exc:
            print(f"Automation Stage 1 cycle failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        failed_handoffs = result.failed_handoffs()
        if failed_handoffs:
            first_failure = failed_handoffs[0]
            print(
                "Automation immediate handoff failed: "
                f"{first_failure.get('reason', 'unknown handoff error')}",
                file=sys.stderr,
            )
            return 1
        return 0

    if args.command == "automation-advance-runs":
        try:
            coordinator = FactoryAutomationCoordinator(
                args.store_dir,
                repo_root_override=args.repo_root,
                autonomy_mode=args.autonomy_mode,
            )
            result = coordinator.run_progression_cycle(
                repository=args.repository,
            )
        except (
            AutomationError,
            TicketingError,
            BuildReviewError,
            IntegrationError,
            EvalExecutionError,
            SecurityReviewError,
            ReleaseStagingError,
            ProductionMonitoringError,
            LinearWorkflowError,
        ) as exc:
            print(f"Automation progression failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "factory-worker":
        if args.interval_seconds <= 0:
            parser.error("--interval-seconds must be > 0")
        if args.max_cycles is not None and args.max_cycles < 1:
            parser.error("--max-cycles must be >= 1")
        if args.max_events is not None and args.max_events < 1:
            parser.error("--max-events must be >= 1")
        try:
            config = ProductionRuntimeConfig.from_env(
                store_dir=args.store_dir,
                repo_root=args.repo_root or Path.cwd(),
                repository=args.repository,
                base_branch=args.base_branch,
                interval_seconds=args.interval_seconds,
                max_events_per_cycle=args.max_events,
                autonomy_mode=args.autonomy_mode,
            )
            result = FactoryWorker(config).run(once=args.once, max_cycles=args.max_cycles)
        except (
            AutomationError,
            BuildReviewError,
            EvalExecutionError,
            FactoryConnectorError,
            IntakeError,
            LinearTriggerError,
            LinearWorkflowError,
            TicketingError,
            OSError,
        ) as exc:
            print(f"Factory worker failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "factory-doctor":
        try:
            config = ProductionRuntimeConfig.from_env(
                store_dir=args.store_dir,
                repo_root=args.repo_root or Path.cwd(),
                repository=args.repository,
                base_branch=args.base_branch,
                autonomy_mode=args.autonomy_mode,
            )
            result = FactoryDoctor(config).run()
        except (AutomationError, OSError) as exc:
            print(f"Factory doctor failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2))
        return 0 if result["status"] == "passed" else 1

    if args.command == "automation-supervisor-cycle":
        if args.feedback_window_days < 1:
            parser.error("--feedback-window-days must be >= 1")
        try:
            html = _read_text_file(args.html_file, "HTML source") if args.html_file is not None else None
            coordinator = FactoryAutomationCoordinator(
                args.store_dir,
                repo_root_override=args.repo_root,
            )
            result = coordinator.run_supervisor_cycle(
                html=html,
                source_url=args.source_url,
                detected_at=args.detected_at,
                max_new_items=args.max_new_items,
                advance_immediately=args.advance_immediately,
                raise_on_failed_handoff=False,
                repository=args.repository,
                run_weekly_feedback=args.run_weekly_feedback,
                window_label=args.window_label,
                feedback_window_days=args.feedback_window_days,
            )
        except (
            AutomationError,
            IntakeError,
            TicketingError,
            BuildReviewError,
            IntegrationError,
            EvalExecutionError,
            SecurityReviewError,
            ReleaseStagingError,
            ProductionMonitoringError,
            LinearWorkflowError,
            CommandInputError,
        ) as exc:
            print(f"Automation supervisor cycle failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        failed_handoffs = result.stage1_result.failed_handoffs()
        if failed_handoffs:
            first_failure = failed_handoffs[0]
            print(
                "Automation immediate handoff failed: "
                f"{first_failure.get('reason', 'unknown handoff error')}",
                file=sys.stderr,
            )
            return 1
        return 0

    if args.command == "automation-weekly-feedback":
        if args.feedback_window_days < 1:
            parser.error("--feedback-window-days must be >= 1")
        try:
            coordinator = FactoryAutomationCoordinator(
                args.store_dir,
                repo_root_override=args.repo_root,
            )
            result = coordinator.run_weekly_feedback_cycle(
                window_label=args.window_label,
                feedback_window_days=args.feedback_window_days,
            )
        except (AutomationError, FeedbackSynthesisError, LinearWorkflowError) as exc:
            print(f"Automation weekly feedback failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "factory-vertical-slice":
        if args.feedback_window_days < 1:
            parser.error("--feedback-window-days must be >= 1")
        config = VerticalSliceConfig(
            repo_root=args.repo_root,
            store_dir=args.store_dir,
            repository=args.repository,
            html_file=args.html_file,
            source_url=args.source_url,
            entry_index=args.entry_index,
            base_branch=args.base_branch,
            seed_missing_ops_signals=not args.require_existing_ops_signals,
            feedback_window_days=args.feedback_window_days,
        )
        try:
            agent_connector = _build_agent_connector(args)
            result = FactoryVerticalSliceRunner(
                config,
                agent_connector=agent_connector,
            ).run()
        except (
            BuildReviewError,
            CommandInputError,
            EvalExecutionError,
            FactoryConnectorError,
            FeedbackSynthesisError,
            IntegrationError,
            IntakeError,
            MergeError,
            ProductionMonitoringError,
            ReleaseStagingError,
            SecurityReviewError,
            TicketingError,
            VerticalSliceError,
            OSError,
        ) as exc:
            print(f"Factory vertical slice failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "factory-cockpit":
        try:
            summary = build_cockpit_summary(
                args.store_dir,
                repo_root_override=args.repo_root,
                stale_heartbeat_seconds=args.stale_heartbeat_seconds,
            )
        except AutomationError as exc:
            print(f"Factory cockpit failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(summary, indent=2))
        return 0

    if args.command == "stage1-intake":
        try:
            html = _read_text_file(args.html_file, "HTML source") if args.html_file is not None else None
        except CommandInputError as exc:
            print(f"Stage 1 intake failed before clarification: {exc}", file=sys.stderr)
            return 1
        scout = AnthropicScout(source_url=args.source_url)
        try:
            items = scout.list_items(html=html, detected_at=args.detected_at)
        except IntakeError as exc:
            print(f"Stage 1 intake failed before clarification: {exc}", file=sys.stderr)
            return 1
        if args.entry_index < 0 or args.entry_index >= len(items):
            parser.error(
                f"--entry-index must be between 0 and {len(items) - 1} for the selected source"
            )
        pipeline = Stage1IntakePipeline(args.repo_root)
        try:
            result = pipeline.process_item(items[args.entry_index])
        except IntakeError as exc:
            print(f"Stage 1 intake failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "stage1-intake-manual":
        item = build_manual_intake_item(
            title=args.title,
            body=args.body,
            url=args.url,
            provider=args.provider,
            external_id=args.external_id,
            detected_at=args.detected_at,
            published_at=args.published_at,
        )
        pipeline = Stage1IntakePipeline(args.repo_root)
        try:
            result = pipeline.process_item(item)
        except IntakeError as exc:
            print(f"Stage 1 manual intake failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "stage2-ticketing":
        try:
            if args.stage1_result_file is not None:
                spec_packet, policy_decision, work_item_document = _load_stage1_result(
                    args.stage1_result_file
                )
            else:
                missing = [
                    name
                    for name, path in (
                        ("--spec-packet-file", args.spec_packet_file),
                        ("--policy-decision-file", args.policy_decision_file),
                        ("--work-item-file", args.work_item_file),
                    )
                    if path is None
                ]
                if missing:
                    parser.error(
                        "stage2-ticketing requires either --stage1-result-file or all of "
                        "--spec-packet-file, --policy-decision-file, and --work-item-file"
                    )
                spec_packet = _read_json_object(args.spec_packet_file, "spec-packet")
                policy_decision = _read_json_object(args.policy_decision_file, "policy-decision")
                work_item_document = _read_json_object(args.work_item_file, "work-item")

            agent_connector = _build_agent_connector(args)
            pipeline = Stage2TicketingPipeline(
                args.repo_root,
                agent_connector=agent_connector,
            )
            result = pipeline.process(
                spec_packet,
                policy_decision,
                _load_work_item(work_item_document, "work-item"),
            )
        except (CommandInputError, TicketingError) as exc:
            print(f"Stage 2 ticketing failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "stage3-build-review":
        try:
            if args.stage2_result_file is not None:
                (
                    spec_packet,
                    policy_decision,
                    ticket_bundle,
                    eval_manifest,
                    work_item_document,
                ) = _load_stage2_result(args.stage2_result_file)
            else:
                missing = [
                    name
                    for name, path in (
                        ("--spec-packet-file", args.spec_packet_file),
                        ("--policy-decision-file", args.policy_decision_file),
                        ("--ticket-bundle-file", args.ticket_bundle_file),
                        ("--eval-manifest-file", args.eval_manifest_file),
                        ("--work-item-file", args.work_item_file),
                    )
                    if path is None
                ]
                if missing:
                    parser.error(
                        "stage3-build-review requires either --stage2-result-file or all of "
                        "--spec-packet-file, --policy-decision-file, --ticket-bundle-file, "
                        "--eval-manifest-file, and --work-item-file"
                    )
                spec_packet = _read_json_object(args.spec_packet_file, "spec-packet")
                policy_decision = _read_json_object(args.policy_decision_file, "policy-decision")
                ticket_bundle = _read_json_object(args.ticket_bundle_file, "ticket-bundle")
                eval_manifest = _read_json_object(args.eval_manifest_file, "eval-manifest")
                work_item_document = _read_json_object(args.work_item_file, "work-item")

            agent_connector = _build_agent_connector(args)
            pipeline = Stage3BuildReviewPipeline(
                args.repo_root,
                agent_connector=agent_connector,
            )
            result = pipeline.process(
                spec_packet,
                policy_decision,
                ticket_bundle,
                eval_manifest,
                _load_work_item(work_item_document, "work-item"),
                repository=args.repository,
                blocking_findings=args.blocking_finding,
            )
        except (BuildReviewError, CommandInputError) as exc:
            print(f"Stage 3 build/review failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "stage4-integration":
        try:
            if args.stage3_result_file is not None:
                (
                    spec_packet,
                    policy_decision,
                    ticket_bundle,
                    eval_manifest,
                    pr_packet,
                    work_item_document,
                ) = _load_stage3_result(args.stage3_result_file)
            else:
                missing = [
                    name
                    for name, path in (
                        ("--spec-packet-file", args.spec_packet_file),
                        ("--policy-decision-file", args.policy_decision_file),
                        ("--ticket-bundle-file", args.ticket_bundle_file),
                        ("--eval-manifest-file", args.eval_manifest_file),
                        ("--pr-packet-file", args.pr_packet_file),
                        ("--work-item-file", args.work_item_file),
                    )
                    if path is None
                ]
                if missing:
                    parser.error(
                        "stage4-integration requires either --stage3-result-file or all of "
                        "--spec-packet-file, --policy-decision-file, --ticket-bundle-file, "
                        "--eval-manifest-file, --pr-packet-file, and --work-item-file"
                    )
                spec_packet = _read_json_object(args.spec_packet_file, "spec-packet")
                policy_decision = _read_json_object(args.policy_decision_file, "policy-decision")
                ticket_bundle = _read_json_object(args.ticket_bundle_file, "ticket-bundle")
                eval_manifest = _read_json_object(args.eval_manifest_file, "eval-manifest")
                pr_packet = _read_json_object(args.pr_packet_file, "pr-packet")
                work_item_document = _read_json_object(args.work_item_file, "work-item")

            pipeline = Stage4IntegrationPipeline(args.repo_root)
            result = pipeline.process(
                spec_packet,
                policy_decision,
                ticket_bundle,
                eval_manifest,
                pr_packet,
                _load_work_item(work_item_document, "work-item"),
            )
        except (IntegrationError, CommandInputError) as exc:
            print(f"Stage 4 integration failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "stage5-eval":
        try:
            if args.stage4_result_file is not None:
                (
                    spec_packet,
                    policy_decision,
                    ticket_bundle,
                    eval_manifest,
                    pr_packet,
                    prompt_contract,
                    tool_schema,
                    golden_dataset,
                    latency_baseline,
                    work_item_document,
                ) = _load_stage4_result(args.stage4_result_file)
            else:
                missing = [
                    name
                    for name, path in (
                        ("--spec-packet-file", args.spec_packet_file),
                        ("--policy-decision-file", args.policy_decision_file),
                        ("--ticket-bundle-file", args.ticket_bundle_file),
                        ("--eval-manifest-file", args.eval_manifest_file),
                        ("--pr-packet-file", args.pr_packet_file),
                        ("--prompt-contract-file", args.prompt_contract_file),
                        ("--tool-schema-file", args.tool_schema_file),
                        ("--golden-dataset-file", args.golden_dataset_file),
                        ("--latency-baseline-file", args.latency_baseline_file),
                        ("--work-item-file", args.work_item_file),
                    )
                    if path is None
                ]
                if missing:
                    parser.error(
                        "stage5-eval requires either --stage4-result-file or all of "
                        "--spec-packet-file, --policy-decision-file, --ticket-bundle-file, "
                        "--eval-manifest-file, --pr-packet-file, --prompt-contract-file, "
                        "--tool-schema-file, --golden-dataset-file, --latency-baseline-file, "
                        "and --work-item-file"
                    )
                spec_packet = _read_json_object(args.spec_packet_file, "spec-packet")
                policy_decision = _read_json_object(args.policy_decision_file, "policy-decision")
                ticket_bundle = _read_json_object(args.ticket_bundle_file, "ticket-bundle")
                eval_manifest = _read_json_object(args.eval_manifest_file, "eval-manifest")
                pr_packet = _read_json_object(args.pr_packet_file, "pr-packet")
                prompt_contract = _read_json_object(args.prompt_contract_file, "prompt-contract")
                tool_schema = _read_json_object(args.tool_schema_file, "tool-schema")
                golden_dataset = _read_json_object(args.golden_dataset_file, "golden-dataset")
                latency_baseline = _read_json_object(
                    args.latency_baseline_file,
                    "latency-baseline",
                )
                work_item_document = _read_json_object(args.work_item_file, "work-item")

            pipeline = Stage5EvalPipeline(args.repo_root)
            result = pipeline.process(
                spec_packet,
                policy_decision,
                ticket_bundle,
                eval_manifest,
                pr_packet,
                prompt_contract,
                tool_schema,
                golden_dataset,
                latency_baseline,
                _load_work_item(work_item_document, "work-item"),
            )
        except (EvalExecutionError, CommandInputError) as exc:
            print(f"Stage 5 eval failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "stage6-security-review":
        try:
            if args.stage5_result_file is not None:
                (
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
                    work_item_document,
                ) = _load_stage5_result(args.stage5_result_file)
            else:
                missing = [
                    name
                    for name, path in (
                        ("--spec-packet-file", args.spec_packet_file),
                        ("--policy-decision-file", args.policy_decision_file),
                        ("--ticket-bundle-file", args.ticket_bundle_file),
                        ("--eval-manifest-file", args.eval_manifest_file),
                        ("--pr-packet-file", args.pr_packet_file),
                        ("--prompt-contract-file", args.prompt_contract_file),
                        ("--tool-schema-file", args.tool_schema_file),
                        ("--golden-dataset-file", args.golden_dataset_file),
                        ("--latency-baseline-file", args.latency_baseline_file),
                        ("--eval-report-file", args.eval_report_file),
                        ("--work-item-file", args.work_item_file),
                    )
                    if path is None
                ]
                if missing:
                    parser.error(
                        "stage6-security-review requires either --stage5-result-file or all of "
                        "--spec-packet-file, --policy-decision-file, --ticket-bundle-file, "
                        "--eval-manifest-file, --pr-packet-file, --prompt-contract-file, "
                        "--tool-schema-file, --golden-dataset-file, --latency-baseline-file, "
                        "--eval-report-file, and --work-item-file"
                    )
                spec_packet = _read_json_object(args.spec_packet_file, "spec-packet")
                policy_decision = _read_json_object(args.policy_decision_file, "policy-decision")
                ticket_bundle = _read_json_object(args.ticket_bundle_file, "ticket-bundle")
                eval_manifest = _read_json_object(args.eval_manifest_file, "eval-manifest")
                pr_packet = _read_json_object(args.pr_packet_file, "pr-packet")
                prompt_contract = _read_json_object(args.prompt_contract_file, "prompt-contract")
                tool_schema = _read_json_object(args.tool_schema_file, "tool-schema")
                golden_dataset = _read_json_object(args.golden_dataset_file, "golden-dataset")
                latency_baseline = _read_json_object(
                    args.latency_baseline_file,
                    "latency-baseline",
                )
                eval_report = _read_json_object(args.eval_report_file, "eval-report")
                work_item_document = _read_json_object(args.work_item_file, "work-item")

            pipeline = Stage6SecurityReviewPipeline(args.repo_root)
            result = pipeline.process(
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
                _load_work_item(work_item_document, "work-item"),
                approved_security_reviewers=args.approved_security_reviewer,
                blocking_findings=args.security_blocking_finding,
            )
        except (CommandInputError, SecurityReviewError) as exc:
            print(f"Stage 6 security review failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "stage-merge":
        try:
            if args.stage6_result_file is not None:
                (
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
                    work_item_document,
                ) = _load_stage6_result(args.stage6_result_file)
            else:
                missing = [
                    name
                    for name, path in (
                        ("--spec-packet-file", args.spec_packet_file),
                        ("--policy-decision-file", args.policy_decision_file),
                        ("--ticket-bundle-file", args.ticket_bundle_file),
                        ("--eval-manifest-file", args.eval_manifest_file),
                        ("--pr-packet-file", args.pr_packet_file),
                        ("--prompt-contract-file", args.prompt_contract_file),
                        ("--tool-schema-file", args.tool_schema_file),
                        ("--golden-dataset-file", args.golden_dataset_file),
                        ("--latency-baseline-file", args.latency_baseline_file),
                        ("--eval-report-file", args.eval_report_file),
                        ("--security-review-file", args.security_review_file),
                        ("--work-item-file", args.work_item_file),
                    )
                    if path is None
                ]
                if missing:
                    parser.error(
                        "stage-merge requires either --stage6-result-file or all of "
                        "--spec-packet-file, --policy-decision-file, --ticket-bundle-file, "
                        "--eval-manifest-file, --pr-packet-file, --prompt-contract-file, "
                        "--tool-schema-file, --golden-dataset-file, --latency-baseline-file, "
                        "--eval-report-file, --security-review-file, and --work-item-file"
                    )
                spec_packet = _read_json_object(args.spec_packet_file, "spec-packet")
                policy_decision = _read_json_object(args.policy_decision_file, "policy-decision")
                ticket_bundle = _read_json_object(args.ticket_bundle_file, "ticket-bundle")
                eval_manifest = _read_json_object(args.eval_manifest_file, "eval-manifest")
                pr_packet = _read_json_object(args.pr_packet_file, "pr-packet")
                prompt_contract = _read_json_object(args.prompt_contract_file, "prompt-contract")
                tool_schema = _read_json_object(args.tool_schema_file, "tool-schema")
                golden_dataset = _read_json_object(args.golden_dataset_file, "golden-dataset")
                latency_baseline = _read_json_object(
                    args.latency_baseline_file,
                    "latency-baseline",
                )
                eval_report = _read_json_object(args.eval_report_file, "eval-report")
                security_review = _read_json_object(args.security_review_file, "security-review")
                work_item_document = _read_json_object(args.work_item_file, "work-item")

            pipeline = StageMergePipeline(args.repo_root)
            result = pipeline.process(
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
                _load_work_item(work_item_document, "work-item"),
                approved_merge_reviewers=args.approved_merge_reviewer,
                blocking_findings=args.merge_blocking_finding,
            )
        except (CommandInputError, MergeError) as exc:
            print(f"Merge stage failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "stage7-release-staging":
        try:
            merge_decision = None
            if args.stage6_result_file is not None and args.merge_result_file is not None:
                parser.error("stage7-release-staging accepts only one of --stage6-result-file or --merge-result-file")
            if args.merge_result_file is not None:
                (
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
                    merge_decision,
                    work_item_document,
                ) = _load_merge_result(args.merge_result_file)
            elif args.stage6_result_file is not None:
                (
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
                    work_item_document,
                ) = _load_stage6_result(args.stage6_result_file)
            else:
                missing = [
                    name
                    for name, path in (
                        ("--spec-packet-file", args.spec_packet_file),
                        ("--policy-decision-file", args.policy_decision_file),
                        ("--ticket-bundle-file", args.ticket_bundle_file),
                        ("--eval-manifest-file", args.eval_manifest_file),
                        ("--pr-packet-file", args.pr_packet_file),
                        ("--prompt-contract-file", args.prompt_contract_file),
                        ("--tool-schema-file", args.tool_schema_file),
                        ("--golden-dataset-file", args.golden_dataset_file),
                        ("--latency-baseline-file", args.latency_baseline_file),
                        ("--eval-report-file", args.eval_report_file),
                        ("--security-review-file", args.security_review_file),
                        ("--work-item-file", args.work_item_file),
                    )
                    if path is None
                ]
                if missing:
                    parser.error(
                        "stage7-release-staging requires either --stage6-result-file or all of "
                        "--spec-packet-file, --policy-decision-file, --ticket-bundle-file, "
                        "--eval-manifest-file, --pr-packet-file, --prompt-contract-file, "
                        "--tool-schema-file, --golden-dataset-file, --latency-baseline-file, "
                        "--eval-report-file, --security-review-file, and --work-item-file"
                    )
                spec_packet = _read_json_object(args.spec_packet_file, "spec-packet")
                policy_decision = _read_json_object(args.policy_decision_file, "policy-decision")
                ticket_bundle = _read_json_object(args.ticket_bundle_file, "ticket-bundle")
                eval_manifest = _read_json_object(args.eval_manifest_file, "eval-manifest")
                pr_packet = _read_json_object(args.pr_packet_file, "pr-packet")
                prompt_contract = _read_json_object(args.prompt_contract_file, "prompt-contract")
                tool_schema = _read_json_object(args.tool_schema_file, "tool-schema")
                golden_dataset = _read_json_object(args.golden_dataset_file, "golden-dataset")
                latency_baseline = _read_json_object(
                    args.latency_baseline_file,
                    "latency-baseline",
                )
                eval_report = _read_json_object(args.eval_report_file, "eval-report")
                security_review = _read_json_object(args.security_review_file, "security-review")
                merge_decision = (
                    None
                    if args.merge_decision_file is None
                    else _read_json_object(args.merge_decision_file, "merge-decision")
                )
                work_item_document = _read_json_object(args.work_item_file, "work-item")

            if args.observed_soak_minutes is not None and args.observed_soak_minutes < 0:
                parser.error("--observed-soak-minutes must be >= 0")
            if args.observed_request_samples is not None and args.observed_request_samples < 0:
                parser.error("--observed-request-samples must be >= 0")

            pipeline = Stage7ReleaseStagingPipeline(args.repo_root)
            result = pipeline.process(
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
                _load_work_item(work_item_document, "work-item"),
                merge_decision=merge_decision,
                approved_release_reviewers=args.approved_release_reviewer,
                observed_soak_minutes=args.observed_soak_minutes,
                observed_request_samples=args.observed_request_samples,
                metric_overrides=_parse_metric_overrides(args.metric_override),
                rollback_tested=not args.skip_rollback_test,
            )
        except (CommandInputError, ReleaseStagingError) as exc:
            print(f"Stage 7 release staging failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "stage8-production-monitoring":
        try:
            if args.stage7_result_file is not None:
                (
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
                    merge_decision,
                    promotion_decision,
                    work_item_document,
                ) = _load_stage7_result(args.stage7_result_file)
            else:
                merge_decision = None
                missing = [
                    name
                    for name, path in (
                        ("--spec-packet-file", args.spec_packet_file),
                        ("--policy-decision-file", args.policy_decision_file),
                        ("--ticket-bundle-file", args.ticket_bundle_file),
                        ("--eval-manifest-file", args.eval_manifest_file),
                        ("--pr-packet-file", args.pr_packet_file),
                        ("--prompt-contract-file", args.prompt_contract_file),
                        ("--tool-schema-file", args.tool_schema_file),
                        ("--golden-dataset-file", args.golden_dataset_file),
                        ("--latency-baseline-file", args.latency_baseline_file),
                        ("--eval-report-file", args.eval_report_file),
                        ("--security-review-file", args.security_review_file),
                        ("--promotion-decision-file", args.promotion_decision_file),
                        ("--work-item-file", args.work_item_file),
                    )
                    if path is None
                ]
                if missing:
                    parser.error(
                        "stage8-production-monitoring requires either --stage7-result-file or all of "
                        "--spec-packet-file, --policy-decision-file, --ticket-bundle-file, "
                        "--eval-manifest-file, --pr-packet-file, --prompt-contract-file, "
                        "--tool-schema-file, --golden-dataset-file, --latency-baseline-file, "
                        "--eval-report-file, --security-review-file, --promotion-decision-file, "
                        "and --work-item-file"
                    )
                spec_packet = _read_json_object(args.spec_packet_file, "spec-packet")
                policy_decision = _read_json_object(args.policy_decision_file, "policy-decision")
                ticket_bundle = _read_json_object(args.ticket_bundle_file, "ticket-bundle")
                eval_manifest = _read_json_object(args.eval_manifest_file, "eval-manifest")
                pr_packet = _read_json_object(args.pr_packet_file, "pr-packet")
                prompt_contract = _read_json_object(args.prompt_contract_file, "prompt-contract")
                tool_schema = _read_json_object(args.tool_schema_file, "tool-schema")
                golden_dataset = _read_json_object(args.golden_dataset_file, "golden-dataset")
                latency_baseline = _read_json_object(
                    args.latency_baseline_file,
                    "latency-baseline",
                )
                eval_report = _read_json_object(args.eval_report_file, "eval-report")
                security_review = _read_json_object(args.security_review_file, "security-review")
                merge_decision = (
                    None
                    if args.merge_decision_file is None
                    else _read_json_object(args.merge_decision_file, "merge-decision")
                )
                promotion_decision = _read_json_object(
                    args.promotion_decision_file,
                    "promotion-decision",
                )
                work_item_document = _read_json_object(args.work_item_file, "work-item")

            if args.observed_window_minutes is not None and args.observed_window_minutes < 1:
                parser.error("--observed-window-minutes must be >= 1")

            pipeline = Stage8ProductionMonitoringPipeline(args.repo_root)
            result = pipeline.process(
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
                _load_work_item(work_item_document, "work-item"),
                merge_decision=merge_decision,
                observed_window_minutes=args.observed_window_minutes,
                metric_overrides=_parse_metric_overrides(args.metric_override),
                security_anomaly=args.security_anomaly,
            )
        except (CommandInputError, ProductionMonitoringError) as exc:
            print(f"Stage 8 production monitoring failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    if args.command == "stage9-feedback-synthesis":
        try:
            if args.stage8_result_file is not None:
                (
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
                    merge_decision,
                    promotion_decision,
                    monitoring_report,
                    work_item_document,
                ) = _load_stage8_result(args.stage8_result_file)
            else:
                merge_decision = None
                missing = [
                    name
                    for name, path in (
                        ("--spec-packet-file", args.spec_packet_file),
                        ("--policy-decision-file", args.policy_decision_file),
                        ("--ticket-bundle-file", args.ticket_bundle_file),
                        ("--eval-manifest-file", args.eval_manifest_file),
                        ("--pr-packet-file", args.pr_packet_file),
                        ("--prompt-contract-file", args.prompt_contract_file),
                        ("--tool-schema-file", args.tool_schema_file),
                        ("--golden-dataset-file", args.golden_dataset_file),
                        ("--latency-baseline-file", args.latency_baseline_file),
                        ("--eval-report-file", args.eval_report_file),
                        ("--security-review-file", args.security_review_file),
                        ("--promotion-decision-file", args.promotion_decision_file),
                        ("--monitoring-report-file", args.monitoring_report_file),
                        ("--work-item-file", args.work_item_file),
                    )
                    if path is None
                ]
                if missing:
                    parser.error(
                        "stage9-feedback-synthesis requires either --stage8-result-file or all of "
                        "--spec-packet-file, --policy-decision-file, --ticket-bundle-file, "
                        "--eval-manifest-file, --pr-packet-file, --prompt-contract-file, "
                        "--tool-schema-file, --golden-dataset-file, --latency-baseline-file, "
                        "--eval-report-file, --security-review-file, --promotion-decision-file, "
                        "--monitoring-report-file, and --work-item-file"
                    )
                spec_packet = _read_json_object(args.spec_packet_file, "spec-packet")
                policy_decision = _read_json_object(args.policy_decision_file, "policy-decision")
                ticket_bundle = _read_json_object(args.ticket_bundle_file, "ticket-bundle")
                eval_manifest = _read_json_object(args.eval_manifest_file, "eval-manifest")
                pr_packet = _read_json_object(args.pr_packet_file, "pr-packet")
                prompt_contract = _read_json_object(args.prompt_contract_file, "prompt-contract")
                tool_schema = _read_json_object(args.tool_schema_file, "tool-schema")
                golden_dataset = _read_json_object(args.golden_dataset_file, "golden-dataset")
                latency_baseline = _read_json_object(
                    args.latency_baseline_file,
                    "latency-baseline",
                )
                eval_report = _read_json_object(args.eval_report_file, "eval-report")
                security_review = _read_json_object(args.security_review_file, "security-review")
                merge_decision = (
                    None
                    if args.merge_decision_file is None
                    else _read_json_object(args.merge_decision_file, "merge-decision")
                )
                promotion_decision = _read_json_object(
                    args.promotion_decision_file,
                    "promotion-decision",
                )
                monitoring_report = _read_json_object(
                    args.monitoring_report_file,
                    "monitoring-report",
                )
                work_item_document = _read_json_object(args.work_item_file, "work-item")

            if args.feedback_window_days < 1:
                parser.error("--feedback-window-days must be >= 1")

            pipeline = Stage9FeedbackSynthesisPipeline(args.repo_root)
            result = pipeline.process(
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
                _load_work_item(work_item_document, "work-item"),
                merge_decision=merge_decision,
                feedback_window_days=args.feedback_window_days,
                unexpected_user_behaviors=args.unexpected_user_behavior,
                positive_surprises=args.positive_surprise,
                spec_mismatches=args.spec_mismatch,
                eval_misses=args.eval_miss,
            )
        except (CommandInputError, FeedbackSynthesisError) as exc:
            print(f"Stage 9 feedback synthesis failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result.to_document(), indent=2))
        return 0

    parser.error(f"Unhandled command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
