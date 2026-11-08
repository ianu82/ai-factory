# AI Factory v2 — status against the roadmap

Updated 8 November 2026. Evidence links point to real runs on the live service
(https://factory-production-f28b.up.railway.app) against `ianu82/calorie-tracker` and
`ianu82/ai-factory`. "Unattended" means no human or operator action after the work was
admitted (a GitHub issue labelled `factory:auto`). Anything else is marked assisted, and an
operator action taken on Ian's behalf is recorded in the Factory as "Operator (delegated by
ian@mindsdb.com)".

## Live delivery evidence

| Outcome | Worker | What happened | Classification |
| --- | --- | --- | --- |
| calorie-tracker #13 → PR #14 | Native (OpenAI fallback) | Plan → build → checks → fresh-native verification → PR → CI → merge → smoke; issue closed | Assisted: two platform fixes during the run, each followed by an operator retry |
| calorie-tracker #15 → PR #17 | Codex (via gateway) | Same flow end to end, about 3 minutes after pickup | Unattended |
| calorie-tracker #16 → PR #18 | Anton | Fresh Anton verifier rejected attempt 1 for a real defect (empty body → 500); attempt 2 fixed it; rebased onto #17's merge, rechecked and re-ran CI before merging | Unattended |
| calorie-tracker Ticket 40 (#6), T2 → PR #20 | Native on MindsHub (`gpt-terra`) | The planner stopped on a genuine product conflict (requested ranges vs DB precision); the answer was given as a delegated operator decision; T2 merged and smoke passed | Assisted (one human product decision, by design) |
| calorie-tracker Ticket 40 (#6), T1 → PR #22 | Native on MindsHub | First three attempts ended unverified: the production build hit `EAGAIN` in the shared container (48 visible CPUs, 1000-pid cgroup) while the builder's throwaway Postgres was still running. After platform fixes (per-job CPU sets, builder processes stopped before verification, boot logs in QA events) an operator retry built it in one attempt; verification ran in a Railway VM (the verifier started Postgres with Docker inside the VM to run the integration test), CI passed, the Factory merged it and post-merge smoke passed; outcome #6 delivered | Assisted (operator retry after platform fixes) |
| calorie-tracker #21 → PR #23 (#8) | Anton on MindsHub (planner, builder and verifier) | Planned, built, checked; verified by a fresh Anton session with **runtime QA inside a Railway VM** (app ready in 21 s, 3/3 pages screenshotted, probe passed, the verifier added its own screenshot); first live prepared environment saved; CI → merge → smoke; issue closed. A Factory redeploy interrupted the first build attempt, which resumed on its own | Unattended |
| calorie-tracker #24 → PRs #25, #26 (#9) | Native on MindsHub | The planner split the fix into two parallel tasks (route handlers, middleware). Each was verified in its own Railway VM booted from the prepared environment (setup skipped, app ready in 35 s / 53 s, pages and probe passed, the verifier added screenshots). #25 merged first; #26 was rebased and revalidated because the base moved, then merged; smoke passed for both | Unattended |
| ai-factory #195 (Factory improving itself) | Codex on MindsHub | Fresh verifiers rejected several candidates for real races; the approved candidate then conflicted on rebase and was abandoned (delegated). PR #196 closed, issue #195 open | Assisted, not delivered |

MindsHub Inference is the workspace provider (`gpt-terra`, failover disabled); per-run cost is read
back from MindsHub usage (sessions cost about $0.06–0.16).

### Runtime QA in a VM

Verification now boots the candidate app and captures evidence before the verifier session
starts: Playwright screenshots of configured pages, HTTP probes, and the boot log, attached to the
task and summarised in the pull request. With `qa.isolation: "vm"` this happens in an isolated
Railway sandbox VM (no Factory credentials inside, destroyed afterwards).

Live: #6 T1 retry, 16:34 UTC — VM `f97dfb4f…` (us-west2) ready with the checkout in 157 s (first
template build), `npm ci` inside the VM passed, and the app booted inside the VM (`✓ Ready in 570ms`
in its log). The Factory nevertheless recorded "did not become ready": its readiness check polled
the control plane's own loopback instead of the VM's. Verification still passed on the verifier's
own evidence, and PR #22 says "the app did not start" — an honest report of what the Factory saw,
caused by a platform bug. Fixed in 1301e3a (readiness and probes are now requested from inside the
VM, and the VM test fails if the control plane ever calls the app directly). The next VM-backed run,
#8 at 16:50 UTC, did: VM ready in 7 s (cached template), app ready in 21 s, pages `/login`, `/signup`,
`/styleguide` → 200 with screenshots, probe passed, and the PR (calorie-tracker #23) carries that
evidence. Since then builders also propose change-specific runtime checks (for example the status a
fixed endpoint must now return), which the Factory runs in the same environment and attaches.

## Roadmap

| Item | Status in v2 | Evidence / remaining |
| --- | --- | --- |
| F01 Codex qualification | Unattended delivery done | PR #17 |
| F18 Anton unattended delivery | Done, including independent rejection of a defective candidate | PR #18 |
| F02 MindsHub Inference | Live: workspace key from MindsHub sign-in, readiness check, per-run cost readback | Deliveries on MindsHub: #6 (native), #8 (Anton, unattended); Codex ran on MindsHub in #195 |
| F03 Factory builds Factory | `ai-factory` connected (merge needs approval, deploy config protected) | First self-change verified then lost to a rebase conflict (#195) |
| F05 Parallel execution | Per-workspace and global concurrency, dependency ordering, serialized revalidated merges | PRs #17/#18, #25/#26 |
| F06 Prepared environments | Verifier VMs boot from a checkpoint keyed by repository, lockfiles and setup; setup skipped on a hit, saved after a clean miss, older ones pruned | Live: checkpoint saved in #8 (11 s); both parallel tasks of #9 booted their own VM from it in 6–9 s with setup skipped |
| F04 Coding model choice | Models per role from the provider's live list | — |
| F19 MindsHub sign-in & onboarding | API-key sign-in live; orgs/workspaces from MindsHub Auth; invited admins; setup checklist | OIDC coded; needs the domain on MindsHub's redirect allowlist (deliberately not blocking) |
| F07 Workspaces | Isolated repos, credentials, policies, budgets, history | Two-workspace live test not yet run |
| F08 Internal app | Inbox, Outcomes, Runs, Signals, Decisions, Settings; responsive | Rendered with fixtures and live data |
| F09 Acceptance & recovery | Durable stages; restarts resume without cancelling; operator retry/resolve; revert on smoke failure | Recovery runbook not yet in-app |
| F10 Visual & risk review | Runtime QA screenshots/probes as verifier evidence and in PRs; protected paths and change-size limits | Visual diffing against the base not built |
| F11 Watch & intervene | Live activity, guidance delivered to the active agent, cancel, **take over** (candidate pushed as an unverified draft PR, task ends `handed_over`) | — |
| F12 Product signals | GitHub issues, failing main CI, smoke/release failures, **product feedback API** (workspace key) and hand-logged feedback | Analytics/error-tracker connectors not built |
| F13 Reuse human decisions | Attributed decisions; "remember" → precedents; **improvement proposals** from decisions and run history (policy, team notes, budgets, workers), applied by admins with attribution | — |
| F14 Bounded remediation | Smoke failure → revert PR (policy or decision); signals → proposed outcomes | Auto-remediation of failing main CI is opt-in via `factory:auto` only |
| F15 Customer onboarding | Sign-in restricted to `@mindsdb.com`; **Postgres supported** (CI runs the suite on Postgres 17) | Builders still run per-UID in the control-plane container; GitHub App onboarding not built (PAT per workspace) |
| F16/F17 | Deferred | — |
