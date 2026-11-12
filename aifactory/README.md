# AI Factory (v2)

A product for teams to turn outcomes into verified, merged and released software. A team
connects repositories, describes what should be true, and the Factory plans the work,
runs tasks in parallel, implements each change, gets independent QA from a fresh session,
passes checks and CI, merges under the team's policy, smoke-tests the merged result and
reports honestly what happened and what it cost. People are asked only for genuine
decisions or approvals the policy requires.

Live: <https://factory-production-f28b.up.railway.app> (Railway project "AI Factory", service `factory`).

## How a delivery works

```
Outcome ──► Plan (reviewed) ──► Tasks (parallel, dependency-ordered)
                                  │
                                  ├─ Build      fresh sandbox, setup, builder session (Native | Codex | Anton)
                                  ├─ Checks     repository checks run by the Factory, failures → next attempt
                                  ├─ Verify     a *fresh* session of the verifier harness checks every criterion;
                                  │             runtime QA boots the app (in its own Railway VM when configured),
                                  │             screenshots pages and probes endpoints as evidence
                                  ├─ PR         patch applied to a pristine clone, pushed, PR opened with evidence
                                  ├─ CI         waits for GitHub checks; failures + logs → next attempt
                                  ├─ Merge      policy (auto/approval); same-repo merges serialized and revalidated
                                  ├─ Smoke      fresh checkout of the merge commit runs smoke commands
                                  ├─ Release    optional deploy-on-merge health check
                                  └─ Recover    smoke failure → revert PR (policy `recovery.smoke_failure: revert`)
```

Before building starts, every plan is reviewed. By default the plan reviewer (Opus 5.5, or the
`plan_reviewer` model in Settings → Models) checks it against the request and the code, approves it,
or sends it back to the planner with findings, at most twice before a person decides. With plan
approval set to "A person", a person reviews and approves every plan instead.

Every stage is durable (`task.stage`), so approvals and restarts resume without repeating
side effects. Failed attempts feed precise feedback (failing checks, verifier findings, CI
logs) into the next attempt instead of starting over blind. Completion is honest: a task is
`completed` only when merged and smoke-tested; `smoke_failed`, `failed`, `blocked`,
`needs_input` and `awaiting_approval` are explicit and reach the Inbox.

Each outcome has a **Lead**, an agent that owns it end to end: it answers agents' questions, retries
or acknowledges blocked tasks, requests changes, and leaves people a recommendation on what it may
not decide. A workspace can also turn on the **Director** (Settings → Policies; off by default), which
stands in for the people who run the workspace while they are away. Once a decision's judge and Lead
have had their turn, the Director decides what would otherwise wait on a person: plan, merge and
release approvals, budgets (up to a ceiling), blocked work, Supervisor findings, alerts and signal
triage. It decides through the same choices a person has, so every gate still applies. It never
takes work over, lifts a provider hold or changes settings, and it stays within its own spend per
run and per day (`pipeline/director.py`).

## Product surfaces

- **Inbox** — decisions that need a person (plan approval, questions, merge/release approval,
  unblock), plus what changed since you last looked.
- **Outcomes** — list with per-task gate tracks; detail panel with tasks, acceptance evidence,
  attempts, CI, PRs, cost, plan, activity and a guidance channel (optional intervention,
  recorded and delivered to the active agent).
- **Runs** — every agent session with model, tokens, cost and outcome; throughput stats.
- **Signals** — GitHub issues labelled `factory` (proposed) or `factory:auto` (pre-authorized),
  failing CI on the default branch, post-merge smoke and release failures, and product feedback
  (sent by your app with the workspace feedback key, or logged by hand).
  **Proposed improvements** come from the Factory's own history: consistent approvals suggest
  a lighter policy, recurring reviewer findings and failed checks become editable team notes,
  runtime QA that cannot boot, budget stops and better-performing workers are flagged — each
  with its evidence and one change an admin can apply (attributed, and quiet until the evidence
  changes if set aside).
- **Decisions** — attributed history; decisions marked "remember" become precedents that later
  planning and building follow.
- **Settings** — autonomy policies, concurrency (1–50), attempts, budgets, models per role,
  harness per role, masked credentials with save/test, repositories (setup/checks/smoke,
  protected paths, merge and release policy), members.

## Identity, tenancy and credentials

- Sign-in uses MindsHub Auth: either MindsHub OIDC (`MINDSHUB_OIDC_CLIENT_ID`, needs the
  Factory domain on the realm's redirect allowlist) or a MindsHub API key. Both resolve the
  user, organization and workspace through `https://auth.mindshub.ai/v1/authenticate/`.
- Factory organizations mirror MindsHub organizations; members of an organization can access
  its workspaces (MindsHub org owners/admins become admins). `AIFACTORY_ALLOWED_EMAILS`
  (e.g. `@mindsdb.com`) restricts who may sign in.
- Workspaces isolate repositories, credentials, policies, history and budgets.
- Credentials (MindsHub Inference key, GitHub token, optional OpenAI key) are encrypted at rest
  with `AIFACTORY_SECRET_KEY`, never returned by the API, never written to sandboxes, logs,
  PRs or evidence.

## Execution and isolation

- Each job gets a private directory and, when the server runs as root, its own unprivileged
  UID; commands get a minimal environment with no credentials. Cleanup kills everything the
  UID started.
- Native and Anton sessions run their model loop in the control plane and execute the shared
  Factory tools inside the sandbox. Codex runs inside the sandbox and reaches models only
  through `/gateway/v1` with a run-scoped token; the gateway pins the model, enforces the
  run budget, injects the real key and records usage.
- MindsHub requests send `X-MindsHub-Allow-Failover: false` (no silent model substitution)
  and `Langfuse-Session-Id: <run id>`, and per-run cost is read back from MindsHub usage.
- Git: credential-free clones for jobs; publication applies the job's patch to a pristine
  control-plane clone (job-written hooks/config never execute there), enforces protected
  paths and size limits, and pushes with environment-scoped auth.

- Each job is pinned to a small CPU set (`AIFACTORY_SANDBOX_CPUS`, default 4) so toolchains
  size their worker pools for the job rather than the host.
- Verification and runtime QA can run in an isolated Railway sandbox VM (`qa.isolation: "vm"`,
  needs `RAILWAY_API_TOKEN`): the checkout is uploaded, no Factory credential enters the VM, and
  the VM is destroyed afterwards. VMs boot from a *prepared environment* (a checkpoint keyed by
  the repository's lockfiles and setup commands) when one exists, so unchanged dependencies are
  not reinstalled; a clean setup saves one for next time.

Builders can work in their own VM as well (`build.isolation: "vm"`): the checkout is uploaded,
setup runs there (or is skipped from the prepared environment) and the patch is extracted for the
normal publish path. Codex runs in VMs too; it reaches models through the Factory's public HTTPS
gateway with its run-scoped token, which is revoked when the run ends.

## Running locally

```bash
cd aifactory
uv venv .venv && uv pip install --python .venv/bin/python -e '.[dev]'
(cd web && npm ci && npx vite build)
AIFACTORY_ALLOW_DEV_SIGN_IN=1 .venv/bin/python -m aifactory.cli serve --port 8000
```

Tests: `.venv/bin/python -m pytest` (includes end-to-end pipeline tests against a local git
remote with a scripted model). Set `AIFACTORY_TEST_DATABASE_URL=postgresql://…` to run the
suite against a throwaway Postgres (CI does both). UI fixture for rendering checks: `scripts/seed_local_fixture.py`.

## Releases

Pull requests merge into `main` as soon as they are ready. Staging (its own Railway environment) deploys
`main`, and production deploys the `release` branch. `aifactory release-check` runs every 15 minutes:
- it confirms production is live on the current release;
- at most every 2 hours, it moves `release` forward to the commit staging runs, once that commit has soaked,
  passed its checks and smoke tests, and changed Factory code;
- a release that fails in production puts releases on hold.

See [docs/RELEASING.md](docs/RELEASING.md).

## Server configuration

| Variable | Purpose |
| --- | --- |
| `AIFACTORY_PUBLIC_URL` | Public base URL (links, cookies, OIDC redirect) |
| `AIFACTORY_SECRET_KEY` | Encryption key for stored credentials |
| `AIFACTORY_DATA_DIR` | SQLite database, mirrors, job sandboxes (`/data/aifactory` on Railway) |
| `DATABASE_URL` | Optional Postgres (`postgres://…`; install the `postgres` extra). Defaults to SQLite in the data dir |
| `RAILWAY_API_TOKEN` | Project token for verification VMs (`qa.isolation: "vm"`) |
| `AIFACTORY_SANDBOX_CPUS` | CPUs per job (default 4; 0 disables pinning) |
| `AIFACTORY_ALLOWED_EMAILS` | Comma list of emails or `@domain` allowed to sign in |
| `AIFACTORY_BOOTSTRAP` | JSON: organization, workspace, invited admins, repositories (+config), settings, declared outcomes |
| `AIFACTORY_BOOTSTRAP_GITHUB_TOKEN` / `..._OPENAI_API_KEY` / `..._MINDSHUB_API_KEY` | Server fallback credentials until a workspace stores its own |
| `AIFACTORY_GLOBAL_CONCURRENCY` | Server-wide cap on concurrent tasks |
| `AIFACTORY_MINDSHUB_REQUESTS_PER_MINUTE` / `AIFACTORY_OPENAI_REQUESTS_PER_MINUTE` | Positive per-credential rolling-minute model request limits (default 50 each; minimum 1) |
| `MINDSHUB_OIDC_CLIENT_ID` | Enables "Continue with MindsHub" |

Model admission is shared across native, Anton and gateway calls in the Factory process,
including retries and credentials reused by multiple workspaces. Each provider/base URL and
full credential digest shares a 16-request concurrency cap; gateway streams hold capacity
until cleanup. Waiting emits run activity with elapsed time, never credential material.
