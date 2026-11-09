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
| calorie-tracker #27 (#10) | Native on MindsHub, **builder in a Railway VM** | First run with builder VMs: setup ran in the build VM and saved the prepared environment the verifier VM then booted from. Runtime QA ran the builder's change-specific checks in the VM (2/2, then 3/3). The verifier, exercising the running app, rejected attempt 1 (`/api/health/` redirected) and attempt 2 (a global trailing-slash change altered unrelated routes); the builder escalated the conflict, answered as a delegated operator decision (keep Next.js's default redirect, saved as a repository precedent). Attempt 3 was verified and passed CI (PR #29), but #11 merged first, the rebase conflicted, and the rebuild tripped the attempt limit — a Factory bug, fixed in ai-factory #202 (fresh attempt budget after the base moves). Ian abandoned the task in the app at 18:32 before the fix shipped, so it was not retried; PR #29 is still open | Not delivered (abandoned by Ian) |
| calorie-tracker #28 → PR #30 (#11) | **Codex on MindsHub, building and verifying inside Railway VMs** | Codex CLI ran in its build VM and in a separate verification VM through the Factory's public gateway with run-scoped tokens; the verifier checked the real 404 with Supabase unset, the protected-route redirects and the API 401s; CI → merge → smoke; ran in parallel with #10 | Unattended |
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

## Release path

v2 was merged to `main` (ianu82/ai-factory#194). The `factory` Railway service now deploys from `main`
only after GitHub checks pass (tests on SQLite and Postgres, the Codex/Anton/sandbox jobs and the
contract validation). Changes since go through pull requests (#198 builder VMs, #199 recovery guide,
#200 cost reconciliation, #201 Codex in VMs, #202 rebuild budget), each merged once green, and each deploy waits for its checks. The v1
service has no deploy trigger and is untouched. Declared bootstrap values apply when the operator
changes them, so settings changed in the app survive redeploys.

## Factory building Factory (overnight, 8–9 November)

Ian filed about twenty ambitious `factory:auto` issues on `ianu82/ai-factory` (Linear intake, onboarding,
attachments, image-aware workers, Anton tools in the sandbox, evals and measurement, regret-free merge rate,
risk judge and autonomy levels, decision judges, workstreams, knowledge and skills, Mind, review-comment
handling, safer parallel delivery and more), and three more were added for evals monitoring (#234), Factory
health and SLOs (#235) and browser end-to-end tests (#236). The Factory plans each into tasks and builds them
in Railway VMs (builders and verifiers), on MindsHub models. Merges into `ai-factory` need a person by policy;
under Ian's standing overnight directive an operator merges a Factory PR only when it is waiting on a person,
its checks are green and its review comments are addressed (the reviewer bot re-reviews the revised commit
and finds nothing new).

What the operator did, all recorded in the Factory as decisions "delegated by ian@mindsdb.com":
- review findings from the reviewer bot were sent back as "Request changes" with the exact findings, so the
  Factory revised the same branches, re-verified and re-ran CI;
- tasks that ran out of attempts were retried with specific guidance taken from the verifier's last findings;
- two planner questions were answered from the issues' own acceptance criteria.

Platform fixes made along the way: verifier VMs failed three tests even on main (a fake-VM path collision and
the Codex CLI tests), fixed in #247; rebuilds after a parallel merge no longer exhaust the attempt budget (#202);
PRs merged or closed on GitHub are reconciled (#230); live updates recover after deploys (#230).

### Factory-built PRs merged overnight (times UTC)

None of these is unattended. "Assisted" means the operator acted under Ian's delegation: sending reviewer
findings back as "Request changes", retrying exhausted attempts with guidance, or approving the merge that this
repository's policy reserves for a person.

| PR | Factory task | What the operator did | Classification |
| --- | --- | --- | --- |
| #237 (23:38) | #17 T3 attributed memory | One round of review findings; merged once the revision fixed them | Assisted |
| #218 (00:17) | #14 T1 Linear client, encrypted credential | One review round; one retry after environment-caused failures; the branch auto-rebased after a parallel merge | Assisted |
| #214 (00:17) | #12 T2 benchmark suites | Two review rounds; auto-rebased | Assisted |
| #213 (01:14) | #13 T3 preview detected commands | Two review rounds; one retry | Assisted |
| #251 (01:14) | #15 T3 attachments for every agent session | One review round (an out-of-scope finding was left to #16 T1 and noted on the PR); one retry | Assisted |
| #259 (01:48) | #14 T2 Linear configuration and team discovery | One review round | Assisted |
| #231 (01:50) | #13 T4 full-page onboarding | Two review rounds; retries, including one after a transient VM bridge failure | Assisted |
| #261 (02:05) | #13 T2 credential diagnostics | Three retries, one correcting the operator's own too-broad guidance; clean on first review | Assisted |
| #263 (02:44) | #13 T6 searchable repository selection | Merge approval only | Assisted (merge approval only) |
| #255 (02:48) | #12 T5 delivery metrics | Three review rounds | Assisted |

Every merge followed the same rule: the Factory was waiting on a person, CI and the Factory's verification
were green, and the reviewer bot found nothing on the final commit. Leftover threads were answered with where
the fix is before being resolved.

### Platform changes made because of what the night showed

- **Revisions start from today's main** (#250). A revision built on a stale base got no CI and stalled. It now
  rebases first (`--onto` the recorded base). When CI never arrives because main moved, the branch is
  rebased and rechecked. A branch someone else pushed to is never adopted.
- **Conflicts keep reviewed work** (#257). Parallel tasks kept conflicting in `models.py` and `api.py`. A
  conflicting revision now carries the earlier change over with a three-way apply and resolves only the
  overlapping lines, instead of re-implementing from scratch. Leftover conflict markers are rejected.
- **Verifiers stop failing tasks over pre-existing failures** (#260). A failure identical on the base commit
  and unrelated to the task is evidence, not a failed criterion. Opt-in suites run only when the task needs them.
- **Verifiers judge by the same decisions as builders** (#262). Answers that clarified a criterion reached the
  builder but not the verifier, so tasks failed on the original wording (#14 T4, #15 T1). Repository-scoped
  precedents now apply only to their repository; before, they leaked to every repository.
- **Pull requests page** (#253): unresolved review threads mean Needs attention, and Factory revisions show as
  In progress. **Signals** (#252): check notes are proposed only for checks the repository still runs.
- **One automatic retry round** (#258, in review at the time of writing): when attempts run out on concrete,
  relayed findings, the Factory retries once with a fresh budget before asking a person.

### Operator mistakes, owned

- Reused a nonce (`o16t5-retry1`) from earlier in the night, so the bootstrap correctly skipped that retry
  twice as already applied. Nonces must be unique across the workspace's history.
- Told #13 T2 that a failed retest must never downgrade a valid token. That is right only for transient
  failures; the verifier caught it and the corrected retry delivered #261.
- Told #16 T1 to redact only long base64 runs, which let small images leak. The corrected guidance redacts
  the exact payloads the session sent.
- Clock estimates in the overnight notes ran an hour fast for a while. The times above come from GitHub.

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
| F08 Internal app | Inbox (grouped compact rows, inline actions, detail pane), Outcomes, Pull requests (GitHub, grouped by what each needs), Runs, Signals, Decisions, Settings; responsive; live updates that recover after deploys | Rendered with fixtures and live data |
| F09 Acceptance & recovery | Durable stages; restarts resume without cancelling (an interrupted attempt is redone, not counted); operator retry/resolve; revert on smoke failure; in-app recovery guide (Inbox → How recovery works); costs reconciled from MindsHub usage when it lags | — |
| F10 Visual & risk review | Runtime QA screenshots/probes as verifier evidence and in PRs; protected paths and change-size limits | Visual diffing against the base not built |
| F11 Watch & intervene | Live activity, guidance delivered to the active agent, cancel, **take over** from a task or from a stuck task's decision (candidate pushed as an unverified draft PR, task ends `handed_over`) | — |
| F12 Product signals | GitHub issues, failing main CI, smoke/release failures, **product feedback API** (workspace key) and hand-logged feedback | Analytics/error-tracker connectors not built |
| F13 Reuse human decisions | Attributed decisions; "remember" → precedents; **improvement proposals** from decisions and run history (policy, team notes, budgets, workers), applied by admins with attribution | — |
| F14 Bounded remediation | Smoke failure → revert PR (policy or decision); signals → proposed outcomes | Auto-remediation of failing main CI is opt-in via `factory:auto` only |
| F15 Customer onboarding | Sign-in restricted to `@mindsdb.com`; cross-tenant isolation tested over every workspace route; **Postgres supported** (CI runs the suite on Postgres 17); **builders can run in their own Railway VM** (`build.isolation: "vm"`, enabled for calorie-tracker and ai-factory) | GitHub App onboarding not built (a PAT per workspace). Codex runs in VMs through the public HTTPS gateway (#201; live in #11) |
| F16/F17 | Deferred | — |
