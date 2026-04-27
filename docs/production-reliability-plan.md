# Production Reliability Plan

This plan tracks the reliability hardening work for the AI Factory production-facing runtime.

## Accepted Architecture Critiques

The following review points are now part of the production-readiness path:

- Make the simulation/production boundary explicit and enforceable. `factory-worker` is production-facing and must stop at PR-ready human merge/deploy unless simulation mode is explicitly enabled by config.
- Keep the no-stuck reliability work first. Observability, heartbeats, stuck marking, scheduler fairness, and manual recovery reduce operational risk before large refactors.
- Split `__main__.py` after the reliability layer lands. The CLI should become thin dispatch into command modules, but not while the scheduler/recovery state is still moving. The reliability command surface now lives in `reliability_commands.py`.
- Split `automation.py` after the reliability layer lands. Run store, leases, scheduler, progression, and automation state should become separate modules with stable tests.
- Replace GitHub CLI subprocess coupling with an API-backed connector after the recovery model is reliable. A GitHub REST PR connector now exists behind `AI_FACTORY_REPO_CONNECTOR_PROVIDER=github_api`; local `git` remains responsible for worktree/commit/push operations.
- Extract prompt construction and response parsing into model-task modules after the runtime is stable, so model switching is easier and safer.

The following review points are intentionally not first-priority:

- Rewriting the orchestrator to async. The near-term production model is multi-process workers with leases, worktrees, and one active slot by default. Async can wait until there is real in-process fan-out pressure.

## Phase 1: Operation Tracker and Cockpit Visibility

- Add `runs/<work_item_id>/operation.json`.
- Track active operation metadata: work item, stage, operation, worker id, pid, timestamps, status, message, and subprocess metadata.
- Wrap Stage 3 builder execution, Stage 5 gates, artifact persistence, and Linear sync in operation heartbeats.
- Extend `factory-cockpit` with queue status, active operation, heartbeat age, skip reason, recovery status, worker ownership, and health.

## Phase 2: Safe Stuck Marking

- Add `factory-reap-stale-operations`.
- Mark stale operation heartbeats and expired run leases in `runs/<work_item_id>/recovery-state.json`.
- Treat PID/process checks as advisory only; stale heartbeat is the source of truth.
- Sync Linear with one idempotent stuck comment and apply `blocked/stuck` when Linear sync is configured.
- Do not mutate the core controller state to `STUCK`.

## Phase 3: Scheduler Fairness

- Classify runs into deterministic queues: `new_build`, `eval`, `revision`, `blocked`, `dead_letter`, and `complete`.
- Prioritize new Stage 1/2 build work ahead of old PR revisions.
- Record scheduler state and skip reasons for cockpit visibility.
- Keep production default to one active run.
- Keep scheduling as an explicit planning step in `scheduler.py` so queue ordering and slot limits can be tested independently from stage execution.

## Phase 4: Manual Recovery

- Add `factory-retry`, `factory-unblock`, and `factory-dead-letter`.
- Persist operator recovery actions in `recovery-state.json`.
- Clear stale operation files and expired locks when retrying or unblocking.
- Clear `blocked/stuck` in Linear when retrying or unblocking.
- Treat dead-letter recovery as terminal so the reaper and scheduler do not keep resurrecting intentionally closed runs.
- Surface recovery reason, recommended action, last operator action, and action count in `factory-cockpit`.
- Keep invalid operator inputs on the friendly CLI error path; recovery commands should not traceback during an incident.
- Prove retry re-enters scheduler rotation and dead-letter leaves scheduler rotation with tests.

## Phase 5: Conservative Auto-Recovery

- Add auto-recovery only for proven infrastructure failures after manual recovery is stable.
- Retry only with explicit budgets by failure class.
- Never auto-retry unbounded model failures.
- Never create duplicate PRs; reuse existing branch/PR evidence.

## Phase 6: Multi-Worker Slots

- Support `AI_FACTORY_WORKER_ID` and `AI_FACTORY_MAX_ACTIVE_RUNS`.
- Allow multiple worker processes only when each owns a distinct run lease.
- Keep one branch, worktree, operation file, and lease per active run.
- Extend cockpit with active slot usage.

## Production Boundary

- `AI_FACTORY_AUTONOMY_MODE=pr_ready` is the only production default.
- `factory-worker` refuses `simulation_full` unless `AI_FACTORY_ALLOW_SIMULATION_RUNTIME=true` is set.
- File-backed Stage 7-9 artifacts remain simulation evidence only.
- Production movement past Stage 6 requires future real deployment, observability, rollback, and feedback connectors.
