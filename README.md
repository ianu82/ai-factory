# Auto MindsDB Engineering Factory

This repository is the starting point for an agent-first software factory.

## Start Here

- Strategy and operating model: `docs/software-factory-plan.md`
- Control plane and MVP execution spec: `docs/factory-control-plane-spec.md`
- Current architecture diagram: `docs/ai-factory-architecture.md`
- Lane and eval policies: `factory/policies/`
- Machine handoff schemas: `schemas/`

## Current Scope

The first implementation target now includes executable Stage 1 through Stage 9 behavior for the `spec -> tickets -> PR -> integration -> eval -> security -> staging -> monitoring -> feedback` lane:

1. Detect a relevant upstream change.
2. Normalize Anthropic release notes into source items.
3. Produce a deterministic Stage 1 spec packet.
4. Assign a risk lane and policy decision.
5. Generate scoped 1-2 day tickets plus a tiered eval manifest.
6. Draft and review a structured PR packet up to `PR_REVIEWABLE`.
7. Generate versioned prompt, tool, golden-dataset, and latency-baseline artifacts for model-touching work.
8. Execute merge-gating eval tiers, emit an eval report, and finalize the PR packet up to `PR_MERGEABLE` or `PR_REVISION`.
9. Run security threat analysis and sign-off, then advance to `SECURITY_APPROVED`, `SECURITY_REVIEWING`, or `PR_REVISION`.
10. Run a merge orchestration pass that can auto-merge low-risk lanes, pause in `MERGE_REVIEWING` for human merge approval, or return to `PR_REVISION`.
11. Run a staging-soak and promotion-decision pass, then advance to `PRODUCTION_MONITORING`, `STAGING_SOAK`, or `PR_REVISION`.
12. Run production monitoring and incident handling, then emit healthy, auto-mitigated, or human-escalated monitoring records while the work item stays in `PRODUCTION_MONITORING`.
13. Synthesize production learnings into a Stage 9 `feedback-report`, incident learning packets, and backlog candidates while keeping the work item in `PRODUCTION_MONITORING`.
14. Persist Stage 1 through Stage 9 bundles plus merge bundles into a run store and execute recurring Stage 1 poll cycles plus weekly Stage 9 feedback cycles from that store.
15. Advance persisted active-build runs through autonomous Stages 2 through 8 plus merge orchestration in one progression cycle, stopping only at real review or release gates.
16. Trigger immediate per-work-item handoff from Stage 1 intake or manual bundle registration so new active-build runs can advance without waiting for the next scheduled progression pass.
17. Trigger immediate Stage 9 incident follow-up synthesis when a Stage 8 monitoring result reports a live or still-open production incident instead of waiting for the weekly rollup.
18. Protect recurring automation with renewable per-run leases so concurrent workers skip locked runs instead of double-advancing the same work item, even during longer stage executions.
19. Protect `automation-state.json` with a state lease plus compare-and-swap checks so concurrent Stage 1 or weekly cycles do not clobber each other’s dedupe state.
20. Run a unified supervisor cycle that can execute Stage 1 intake, progression, and optional weekly feedback in one safe control-plane pass.
21. Run a first real vertical slice that keeps the deterministic stage contracts but creates real GitHub branch/PR evidence, gates Stage 5 on local eval commands, and drives Stage 7 through Stage 9 from file-backed ops signals.

The first recurring automation layer is now executable through a persisted run store. The first external seam is GitHub PR creation; staging, monitoring, and rollback remain intentionally file-backed until real deployment and observability credentials are connected.

## Useful Commands

- `uv run python scripts/validate_contracts.py`
- `uv run pytest`
- `uv run auto-mindsdb-factory scout-anthropic --html-file fixtures/intake/anthropic-release-notes-sample.html --limit 3`
- `uv run auto-mindsdb-factory stage1-intake --html-file fixtures/intake/anthropic-release-notes-sample.html --entry-index 0`
- `uv run auto-mindsdb-factory stage2-ticketing --stage1-result-file stage1-result.json`
- `uv run auto-mindsdb-factory stage3-build-review --stage2-result-file stage2-result.json`
- `uv run auto-mindsdb-factory stage4-integration --stage3-result-file stage3-result.json`
- `uv run auto-mindsdb-factory stage5-eval --stage4-result-file stage4-result.json`
- `uv run auto-mindsdb-factory stage6-security-review --stage5-result-file stage5-result.json`
- `uv run auto-mindsdb-factory stage-merge --stage6-result-file stage6-result.json`
- `uv run auto-mindsdb-factory stage7-release-staging --merge-result-file merge-result.json`
- `uv run auto-mindsdb-factory stage8-production-monitoring --stage7-result-file stage7-result.json`
- `uv run auto-mindsdb-factory stage9-feedback-synthesis --stage8-result-file stage8-result.json`
- `uv run auto-mindsdb-factory automation-register-bundle --stage stage1 --result-file stage1-result.json --store-dir .factory-automation --advance-immediately`
- `uv run auto-mindsdb-factory automation-register-bundle --stage merge --result-file merge-result.json --store-dir .factory-automation --advance-immediately`
- `uv run auto-mindsdb-factory automation-stage1-cycle --store-dir .factory-automation --html-file fixtures/intake/anthropic-release-notes-sample.html --advance-immediately`
- `uv run auto-mindsdb-factory automation-advance-runs --store-dir .factory-automation`
- `uv run auto-mindsdb-factory automation-weekly-feedback --store-dir .factory-automation --window-label 2026-W17`
- `uv run auto-mindsdb-factory automation-supervisor-cycle --store-dir .factory-automation --html-file fixtures/intake/anthropic-release-notes-sample.html --run-weekly-feedback --window-label 2026-W17`
- `uv run auto-mindsdb-factory factory-vertical-slice --store-dir .factory-automation --repository ianu82/ai-factory`
- `uv run auto-mindsdb-factory factory-cockpit --store-dir .factory-automation`
- `uv run auto-mindsdb-factory demo-controller fixtures/scenarios/fast_lane_feature`

## Repository Layout

```text
docs/
  software-factory-plan.md
  factory-control-plane-spec.md
factory/
  policies/
schemas/
```
