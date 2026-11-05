# Linear product delivery (opt-in)

Run the product worker from a **pinned controller checkout**, separate from the
candidate checkout, trusted configuration, credentials and durable evidence store.
Use a trusted host and protect all of these from candidate writers. The target
checkout's origin must match the profile's GitHub repository. Polling needs no
webhook endpoint. The legacy signed webhook/staged lane is separate and remains
PR-ready; it is not this automatic-merge worker.

An issue is eligible only in the configured team and Ready state with the required
label. The worker posts a progress comment, builds and independently reviews the
exact tested commit, runs configured checks, waits for required GitHub CI, confirms
merge of that commit, then runs fresh post-merge smoke. Only after all of these does
it post a completion comment with the PR and merge SHA. Version-3 routes also
keep the board synchronized with execution. Labels, assignment and requirements
remain untouched. A blocked result is not completion.

The board follows **Backlog → Ready → Building → Verifying → CI / Merge → Merged**,
with **Blocked**, **Canceled** and **Duplicate** available separately. Building
includes environment preparation; Verifying includes executable checks and the
fresh independent reviewer. CI / Merge includes publication, required GitHub CI,
merge and fresh post-merge smoke. Only complete delivery reaches Merged. This
does not mean the application or Factory controller has been deployed. Planning
will become a distinct stage when the mandatory planner is implemented.

## Trusted configuration

Place the following JSON files outside the target. Route criteria are executable
controller policy, never commands supplied by an issue. Substitute real IDs and
commands. Version 3 explicitly opts into status writes and requires all six distinct
state IDs below. Output states must belong to the configured team: active/blocked
states have Linear type `started`, and the final state has type `completed`.
Versions 1 and 2 remain compatible and comment-only; version 1's old output-state
fields do not enable writes. Never change an active run's route to enable this.

`route.json`:

```json
{
  "version": 3,
  "team_id": "TEAM_ID",
  "queued_state_id": "READY_STATE_ID",
  "building_state_id": "BUILDING_STATE_ID",
  "verifying_state_id": "VERIFYING_STATE_ID",
  "merging_state_id": "CI_MERGE_STATE_ID",
  "completed_state_id": "MERGED_STATE_ID",
  "blocked_state_id": "BLOCKED_STATE_ID",
  "required_label": "factory",
  "acceptance_criteria": [
    {"id": "tests", "description": "Product tests pass", "command": ["pytest"]}
  ]
}
```

`profile.json` (replace the digest with 64 lowercase hex characters and use the
actual target's commands and GitHub check names):

```json
{
  "version": 1,
  "name": "product",
  "repository": "OWNER/REPOSITORY",
  "image": "python@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "checks": {"tests": ["pytest"]},
  "smoke": [["pytest", "tests/smoke"]],
  "auto_merge": true,
  "required_status_checks": ["validate", "sandbox"]
}
```

Automatic delivery requires `auto_merge: true`. Configure GitHub branch protection
and the profile's required check names for the actual `validate` and `sandbox` CI
jobs; CI must pass on the exact head. The default builder and independent reviewer
models are both `gpt-6.1-terra` (separate roles). Attempts and time/token budgets
are bounded by the profile; see the [product guide](product-delivery-plan.md).
Commands in both files are argv arrays, not shell strings.

Docker runs untrusted candidate checks in a digest-pinned image without controller
credentials, Git metadata or Docker socket. Keep the controller, Docker host,
profile, route, env file and store trusted; isolation is not a substitute for
securing the host. Supply `LINEAR_API_KEY`, model and GitHub credentials in the
process environment or an explicitly selected external `--env-file`. Existing
process values win. Product commands never auto-load the target `.env`; the Linear
credential-file filter accepts `LINEAR_API_KEY` but not Linear state or endpoint
overrides. Team and Ready state come only from the trusted route.

## Operation and recovery

For a service supervisor, run one cycle per invocation (paths are examples):

```sh
auto-mindsdb-factory product-linear-worker --profile /controller/profile.json \
  --route /controller/route.json --target-root /targets/product \
  --store-dir /evidence/product --env-file /controller/credentials.env --once
```

Omit `--once` for a long-running poller; `--poll-seconds` defaults to 60 and is
bounded 5–3600, `--max-issues` defaults to 1 and is bounded 1–100. Each cycle
flushes JSON results. A one-shot blocked, pending or conflict result exits nonzero;
fatal errors exit nonzero without provider details. Interrupt exits cleanly.

Claims and outcomes are durable: a bounded server-side exclusion filter and
client pagination skip known claims, so completed issues still in Ready do not
starve new intake as retained history grows. Recovery reconciles and
syncs an existing outcome, never reruns a claimed delivery; interrupted work without
proof of completion is blocked rather than duplicated. Comments are at-least-once;
bounded comment history can permit an old marker to be repeated. Inspect the
product evidence and resolve the cause; to retry blocked work, explicitly create
a new issue rather than recycling the claimed issue.

On v3 routes, status writes are checked before and after the API call. A failed
stage update stops execution without another builder attempt. Outcomes are stored
before final status synchronization: a lost response or restart retries reporting,
not the build or merge. Observed cancellation or movement outside the owned
workflow is preserved. Historical comment-only claims are not rerun or rewritten
when upgrading; reconcile their board statuses separately from recorded outcomes.

Requirements, label, ownership and the expected current state are guarded before
promotion and again after CI before merge. A change or cancellation blocks promotion. Linear and
Git are separate services: there is an unavoidable interval between the last Linear
read and Git push, not an atomic cancellation transaction. Monitor results and
keep evidence for reconciliation.
Linear also lacks atomic compare-and-set for status writes: an edit precisely
between the final read and mutation can race synchronization. Do not use board
movement as an instantaneous kill switch for a remote command already executing.

This is code readiness, not a live milestone or deployment claim. After publication,
the controller must separately validate live Linear delivery, GitHub CI/merge and
any deployment in its own environment.
