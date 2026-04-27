# Auto MindsDB Engineering Factory

This repository is the starting point for an agent-first software factory.

## Start Here

- Strategy and operating model: `docs/software-factory-plan.md`
- Control plane and MVP execution spec: `docs/factory-control-plane-spec.md`
- Current architecture diagram: `docs/ai-factory-architecture.md`
- Lightsail production runbook: `docs/lightsail-production-runbook.md`
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

The first recurring automation layer is now executable through a persisted run store. The production v1 target is a PR-ready factory: Linear triggers, real code-worker branch/PR creation, command-backed local gates, Linear stage sync, and a deliberate stop before human merge/deploy. Staging, monitoring, rollback, and feedback remain simulation-only until real deployment and observability credentials are connected.

## Useful Commands

- `uv run python scripts/validate_contracts.py`
- `uv run pytest`
- `uv run auto-mindsdb-factory scout-anthropic --html-file fixtures/intake/anthropic-release-notes-sample.html --limit 3`
- `uv run auto-mindsdb-factory stage1-intake --html-file fixtures/intake/anthropic-release-notes-sample.html --entry-index 0`
- `uv run auto-mindsdb-factory stage1-intake-manual --provider github --external-id github-issue-2 --title "Factory cockpit should surface GitHub check conclusions and eval status" --body "Update the factory cockpit JSON schema and dashboard output to include pull request check conclusions, local eval status, and a clear health summary for each run." --url https://github.com/ianu82/ai-factory/issues/2`
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
- `uv run auto-mindsdb-factory linear-webhook-server --store-dir .factory-automation --host 0.0.0.0 --port 8080`
- `uv run auto-mindsdb-factory automation-linear-trigger-cycle --store-dir .factory-automation --repository ianu82/ai-factory`
- `uv run auto-mindsdb-factory automation-advance-runs --store-dir .factory-automation`
- `uv run auto-mindsdb-factory factory-doctor --store-dir .factory-automation --repository ianu82/ai-factory`
- `uv run auto-mindsdb-factory factory-worker --store-dir .factory-automation --repository ianu82/ai-factory --once`
- `uv run auto-mindsdb-factory automation-weekly-feedback --store-dir .factory-automation --window-label 2026-W17`
- `uv run auto-mindsdb-factory automation-supervisor-cycle --store-dir .factory-automation --html-file fixtures/intake/anthropic-release-notes-sample.html --run-weekly-feedback --window-label 2026-W17`
- `uv run auto-mindsdb-factory factory-vertical-slice --store-dir .factory-automation --repository ianu82/ai-factory`
- `uv run auto-mindsdb-factory factory-cockpit --store-dir .factory-automation`
- `uv run auto-mindsdb-factory demo-controller fixtures/scenarios/fast_lane_feature`

## Live OpenAI Agent Mode

The factory can now use a live OpenAI agent for the judgment-heavy drafting work in Stage 2 and Stage 3 while keeping the rest of the stage contracts deterministic and eval-gated.

Edit the repo-local `.env` file or override values in `.env.local`:

- `.env`
- `.env.local`

The CLI auto-loads `.env` first and `.env.local` second from the current working directory and optional `--repo-root`. Existing shell environment variables still win.

Set the environment:

- `export OPENAI_API_KEY=...`
- `export AI_FACTORY_AGENT_PROVIDER=openai`
- `export AI_FACTORY_OPENAI_MODEL=gpt-5.4`
- Optional: `export AI_FACTORY_OPENAI_FALLBACK_MODEL=gpt-5.4-mini`
- Optional: `export AI_FACTORY_OPENAI_REASONING_EFFORT=medium`
- Optional: `export AI_FACTORY_OPENAI_MAX_OUTPUT_TOKENS=4000`
- Optional: `export AI_FACTORY_OPENAI_TIMEOUT_SECONDS=120`
- Optional: `export AI_FACTORY_CODE_WORKER_PROVIDER=codex_cli`
- Optional: `export AI_FACTORY_CODE_WORKER_MODEL=gpt-5.4`
- Optional: `export AI_FACTORY_CODE_WORKER_TIMEOUT_SECONDS=1800`
- Optional: `export AI_FACTORY_AUTONOMY_MODE=pr_ready`
- Optional: `export AI_FACTORY_INTAKE_PAUSED=false`

Run agent-assisted stages directly:

- `uv run auto-mindsdb-factory stage2-ticketing --agent-provider openai --stage1-result-file stage1-result.json`
- `uv run auto-mindsdb-factory stage3-build-review --agent-provider openai --stage2-result-file stage2-result.json`

Run the full vertical slice with a live OpenAI agent:

- `uv run auto-mindsdb-factory factory-vertical-slice --agent-provider openai --store-dir .factory-automation --repository ianu82/ai-factory`

If `OPENAI_API_KEY` is missing, the CLI fails fast with a friendly initialization error before the run starts.

## Linear Stage 1 Intake Trigger

The factory can accept Linear issues through the dedicated `Stage 1 Intake` workflow state and hand them into Stage 1 manual intake.

Set the Linear trigger environment in `.env` or `.env.local`:

- `LINEAR_WEBHOOK_SECRET`
- `LINEAR_API_KEY`
- `LINEAR_TARGET_TEAM_ID`
- `LINEAR_TARGET_STATE_ID`
- Optional: `LINEAR_COMMENT_ON_ACCEPT=true`
- Optional: `LINEAR_COMMENT_ON_REJECT=true`
- Optional: `LINEAR_FACTORY_CREATE_STATES=true`
- Optional: `LINEAR_MATERIALIZE_STAGE2_TICKETS=false`
- Optional: `LINEAR_FACTORY_SYNC_DISABLED=false`
- Optional: `FACTORY_TRIGGER_BASE_URL`

Run the webhook receiver and the drain worker as separate processes:

- `uv run auto-mindsdb-factory linear-webhook-server --store-dir .factory-automation --host 0.0.0.0 --port 8080`
- `uv run auto-mindsdb-factory automation-linear-trigger-cycle --store-dir .factory-automation --repository ianu82/ai-factory`

Set up the Linear stage workflow and backfill existing factory runs:

- `uv run auto-mindsdb-factory linear-ensure-stage-states --store-dir .factory-automation`
- `uv run auto-mindsdb-factory linear-stage-setup --store-dir .factory-automation --verify-only`
- `uv run auto-mindsdb-factory automation-linear-sync-cycle --store-dir .factory-automation`

Practical setup notes:

- Configure the Linear webhook to send only `Issues` events. Extra event types are harmless because the receiver ignores them, but they create noise.
- Point Linear at a public `https://.../hooks/linear` URL. The built-in server is the local receiver; in practice you usually place it behind a reverse proxy or HTTPS tunnel.
- The receiver only verifies, filters, and persists the event. The worker is the process that actually turns the issue into a Stage 1 run and kicks off immediate handoff.
- `automation-linear-trigger-cycle` is a one-shot drain pass, not a daemon. Run it from cron, systemd, or another heartbeat loop if you want near-real-time processing.
- Keep `LINEAR_TARGET_STATE_ID` pointed at the `Stage 1 Intake` workflow state for the target team. `New Feature` can remain a human requirements-quality state before manually moving an issue into factory intake.
- If a run starts from a Linear issue, the factory reuses that issue and moves it forward. If a run starts elsewhere, the factory creates a new Linear issue once the work becomes an active build candidate.
- When the factory cannot advance automatically, it keeps the issue in the current stage and writes a short explanatory comment for a human reviewer.
- In production `pr_ready` mode, Stage 6 is the stop line: the factory comments that the PR is ready for human merge/deploy and does not advance through simulated Stage 7-9 evidence.

Incoming webhook envelopes are persisted under `.factory-automation/linear-trigger-inbox/`, and dedupe state lives in `.factory-automation/linear-trigger-state.json`.

## Production PR-Ready Mode

Production v1 uses `factory-worker` rather than cron-style one-shot commands. The worker drains Linear triggers, advances runs, calls `codex exec` in an isolated git worktree, lets the orchestrator inspect the diff, commits/pushes/opens the PR, runs command-backed gates, syncs Linear, and stops before merge/deploy.

Run the preflight:

- `uv run auto-mindsdb-factory factory-doctor --store-dir .factory-automation --repository ianu82/ai-factory`

Run one production-style cycle:

- `uv run auto-mindsdb-factory factory-worker --store-dir .factory-automation --repository ianu82/ai-factory --once`

Default real gates are:

- `python -m pytest -q`
- `python scripts/validate_contracts.py`

Configure additional gates with `AI_FACTORY_GATE_LINT_COMMAND`, `AI_FACTORY_GATE_TYPECHECK_COMMAND`, `AI_FACTORY_GATE_INTEGRATION_COMMAND`, and `AI_FACTORY_GATE_MIGRATION_SAFETY_COMMAND`. Checks without real commands are recorded as `not_configured`; later-stage checks are recorded as `deferred` rather than fake-passed.

## Repository Layout

```text
docs/
  software-factory-plan.md
  factory-control-plane-spec.md
factory/
  policies/
schemas/
```
