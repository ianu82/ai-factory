from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from .agent_runtime import build_agent_connector
from .automation import AutomationError, FactoryAutomationCoordinator, FactoryRunStore
from .build_review import BuildReviewError, Stage3BuildReviewPipeline
from .connectors import (
    FactoryConnectorError,
)
from .cli_parser import build_parser
from .product_commands import handle_product_command
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
from .reliability import ReliabilityError
from .reliability_commands import (
    handle_reliability_command,
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
    arguments = sys.argv[1:] if argv is None else argv
    if arguments and arguments[0].startswith("product-"):
        # Product source is untrusted. Credentials come from the process or an
        # explicit controller file, never automatic discovery in the target cwd.
        return []
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


class CommandInputError(RuntimeError):
    """Raised when a CLI artifact or input file cannot be loaded safely."""


def _build_agent_connector(args: argparse.Namespace):
    provider = getattr(args, "agent_provider", "none")
    if provider == "none":
        return None
    try:
        return build_agent_connector(
            provider,
            model=args.agent_model,
            fallback_model=args.agent_fallback_model,
            reasoning_effort=args.agent_reasoning_effort,
            max_output_tokens=args.agent_max_output_tokens,
            timeout_seconds=args.agent_timeout_seconds,
            base_url=args.agent_base_url,
        )
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


def main(argv: list[str] | None = None, *, inference_context=None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if inference_context is not None and (not arguments or arguments[0] != "product-deliver"):
        print("Product workflow failed: Trusted inference context is supported only for product-deliver", file=sys.stderr)
        return 1
    try:
        _load_local_env_files(argv)
    except EnvironmentSetupError as exc:
        print(f"Environment setup failed: {exc}", file=sys.stderr)
        return 1

    parser = build_parser()
    args = parser.parse_args(argv)
    options = {} if inference_context is None else {"inference_context": inference_context}
    product_result = handle_product_command(args, parse_env=_parse_env_file, **options)
    if product_result is not None:
        return product_result

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
        except (AutomationError, LinearWorkflowError, ReliabilityError, CommandInputError) as exc:
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
        except (
            LinearTriggerError,
            LinearWorkflowError,
            AutomationError,
            ReliabilityError,
            IntakeError,
        ) as exc:
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
        except (
            AutomationError,
            LinearWorkflowError,
            ReliabilityError,
            IntakeError,
            CommandInputError,
        ) as exc:
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
            ReliabilityError,
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
            ReliabilityError,
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
        except (AutomationError, ReliabilityError, OSError) as exc:
            print(f"Factory doctor failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2))
        return 0 if result["status"] == "passed" else 1

    reliability_exit = handle_reliability_command(args)
    if reliability_exit is not None:
        return reliability_exit

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
            ReliabilityError,
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
        except (AutomationError, FeedbackSynthesisError, LinearWorkflowError, ReliabilityError) as exc:
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
            summary = build_cockpit_summary(args.store_dir, repo_root_override=args.repo_root)
        except (AutomationError, ReliabilityError) as exc:
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
