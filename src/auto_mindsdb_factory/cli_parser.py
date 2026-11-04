"""CLI argument declarations, separate from stage execution and environment loading."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from .reliability_commands import register_reliability_commands


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

    register_reliability_commands(subparsers)

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

    return parser


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


