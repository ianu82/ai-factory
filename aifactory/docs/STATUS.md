# AI Factory v2 — status against the roadmap

Updated 8 November 2026. Evidence links point to real runs on the live service
(https://factory-production-f28b.up.railway.app) against `ianu82/calorie-tracker` and
`ianu82/ai-factory`. "Unattended" means no human or operator action after the work was
admitted (a GitHub issue labelled `factory:auto`). Anything else is marked assisted.

## Live delivery evidence

| Outcome | Worker | What happened | Classification |
| --- | --- | --- | --- |
| calorie-tracker #13 → PR #14 | Native (OpenAI fallback, `gpt-6.1-terra`) | Plan → build → checks → fresh-native verification → PR → CI → merge → smoke; issue closed | Assisted: two platform fixes during the run (Responses API, Checks-API fallback), each followed by an operator retry |
| calorie-tracker #15 → PR #17 | Codex (via gateway, OpenAI fallback) | Same flow end to end, about 3 minutes after pickup | Unattended |
| calorie-tracker #16 → PR #18 | Anton (OpenAI fallback) | Fresh Anton verifier rejected attempt 1 for a real defect (empty body → 500); attempt 2 fixed it; rebased onto #17's merge, rechecked and re-ran CI before merging | Unattended |

Runs after 14:00 UTC use MindsHub Inference (`gpt-terra`, workspace key saved through sign-in).

## Roadmap

| Item | Status in v2 | Evidence / remaining |
| --- | --- | --- |
| F01 Codex qualification | Useful unattended delivery done; semantic rejection and cancel/cleanup for Codex not yet separately shown | PR #17 |
| F18 Anton unattended delivery | Done, including independent rejection of a defective candidate | PR #18 |
| F02 MindsHub Inference | Workspace key saved via MindsHub sign-in; provider readiness verified (70 models); per-run cost readback via `/v1/usage/sessions` implemented | Live MindsHub deliveries in progress; Anton/Codex each still need a MindsHub run |
| F03 Factory builds Factory | `ai-factory` connected (base `factory-v2`, merge needs human approval, deploy config protected) | First self-change: ai-factory #195 |
| F05 Parallel execution | Concurrency per workspace (1–50) plus a server cap; dependency ordering; serialized, revalidated same-repo merges | PRs #17/#18 ran in parallel; #18 revalidated after #17 merged |
| F06 Prepared environments | Not built (setup reruns per sandbox; npm ci ≈ 8 s here) | Cache setup by lockfile hash |
| F04 Coding model choice | Models per role selectable in Settings from the provider's live list | — |
| F19 MindsHub sign-in & onboarding | API-key sign-in live; org/workspace from MindsHub Auth; invited admins; setup checklist | OIDC coded but needs the domain on MindsHub's Keycloak allowlist (deliberately not blocking) |
| F07 Workspaces | Isolated repos, credentials, policies, budgets and history per workspace; multi-repo outcomes | Two-workspace live test not yet run |
| F08 Internal app | Inbox, Outcomes, Runs, Signals, Decisions, Settings; masked key save/test; responsive | Rendered with fixtures; real-data walkthrough pending |
| F09 Acceptance & recovery | Durable stages; restart resumes; failures explicit; this document | Recovery runbook not yet in-app |
| F10 Visual & risk review | Not built | Browser screenshot QA for UI changes |
| F11 Watch & intervene | Live activity, guidance channel delivered to the active agent, cancel; interventions recorded | Pause/take-over not built |
| F12 Product signals | GitHub issues (`factory`, `factory:auto`), failing main CI, post-merge smoke/release failures → deduplicated signals | Error/analytics sources not connected |
| F13 Reuse human decisions | Attributed decisions; "remember" turns them into precedents used by planning and building | — |
| F14 Bounded remediation | Not built beyond signals → proposed outcomes | — |
| F15 Customer onboarding | Sign-in restricted to `@mindsdb.com` | Stronger sandbox isolation (gVisor/microVM) and Postgres first |
| F16/F17 | Deferred | — |
