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
Outcome ──► Plan ──► Tasks (parallel, dependency-ordered)
                       │
                       ├─ Build      fresh sandbox, setup, builder session (Native | Codex | Anton)
                       ├─ Checks     repository checks run by the Factory, failures → next attempt
                       ├─ Verify     a *fresh* session of the verifier harness checks every criterion
                       ├─ PR         patch applied to a pristine clone, pushed, PR opened with evidence
                       ├─ CI         waits for GitHub checks; failures + logs → next attempt
                       ├─ Merge      policy (auto/approval); same-repo merges serialized and revalidated
                       ├─ Smoke      fresh checkout of the merge commit runs smoke commands
                       └─ Release    optional deploy-on-merge health check
```

Every stage is durable (`task.stage`), so approvals and restarts resume without repeating
side effects. Failed attempts feed precise feedback (failing checks, verifier findings, CI
logs) into the next attempt instead of starting over blind. Completion is honest: a task is
`completed` only when merged and smoke-tested; `smoke_failed`, `failed`, `blocked`,
`needs_input` and `awaiting_approval` are explicit and reach the Inbox.

## Product surfaces

- **Inbox** — decisions that need a person (plan approval, questions, merge/release approval,
  unblock), plus what changed since you last looked.
- **Outcomes** — list with per-task gate tracks; detail panel with tasks, acceptance evidence,
  attempts, CI, PRs, cost, plan, activity and a guidance channel (optional intervention,
  recorded and delivered to the active agent).
- **Runs** — every agent session with model, tokens, cost and outcome; throughput stats.
- **Signals** — GitHub issues labelled `factory` (proposed) or `factory:auto` (pre-authorized),
  failing CI on the default branch, post-merge smoke and release failures.
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

Isolation today is per-UID inside one container. Stronger isolation (gVisor/microVM per
job, as MindsHub Cowork uses, or Railway sandboxes) is the next step for untrusted tenants.

## Running locally

```bash
cd aifactory
uv venv .venv && uv pip install --python .venv/bin/python -e '.[dev]'
(cd web && npm ci && npx vite build)
AIFACTORY_ALLOW_DEV_SIGN_IN=1 .venv/bin/python -m aifactory.cli serve --port 8000
```

Tests: `.venv/bin/python -m pytest` (includes end-to-end pipeline tests against a local git
remote with a scripted model). UI fixture for rendering checks: `scripts/seed_local_fixture.py`.

## Server configuration

| Variable | Purpose |
| --- | --- |
| `AIFACTORY_PUBLIC_URL` | Public base URL (links, cookies, OIDC redirect) |
| `AIFACTORY_SECRET_KEY` | Encryption key for stored credentials |
| `AIFACTORY_DATA_DIR` | SQLite database, mirrors, job sandboxes (`/data/aifactory` on Railway) |
| `AIFACTORY_ALLOWED_EMAILS` | Comma list of emails or `@domain` allowed to sign in |
| `AIFACTORY_BOOTSTRAP` | JSON: organization, workspace, invited admins, repositories (+config), settings, declared outcomes |
| `AIFACTORY_BOOTSTRAP_GITHUB_TOKEN` / `..._OPENAI_API_KEY` / `..._MINDSHUB_API_KEY` | Server fallback credentials until a workspace stores its own |
| `AIFACTORY_GLOBAL_CONCURRENCY` | Server-wide cap on concurrent tasks |
| `MINDSHUB_OIDC_CLIENT_ID` | Enables "Continue with MindsHub" |
