# Product delivery milestone

The product delivery path uses a trusted profile outside the target repository.
It will share the factory's durable lease and operation infrastructure while
avoiding mandatory domain-specific planning stages for small, scoped tasks.
The existing staged and simulation commands remain compatible.

Deliver in small PRs: reproducible CI; product profiles; durable run evidence;
disposable execution; tool-enabled implementation and independent verification;
frozen replay cases; exact-head automatic merge and post-merge verification;
bounded failure-driven improvement proposals.

Completion requires an actual change in a separate product repository, independent
verification tied to its commit, confirmed merge, and post-merge smoke evidence.
Deliberately broken candidates must fail. Preserve failed attempts and costs;
do not count a proposed PR or a favourable model verdict as task completion.

Profiles use JSON version 1. Required fields are `name`, `repository` (GitHub
owner/name), a digest-pinned Docker `image`, nonempty named command `checks` and
nonempty `smoke` commands. Commands are argument arrays, not implicit shell scripts.
Optional setup commands, setup network access, model selections, path restrictions,
GitHub check names and attempt/token/time/diff budgets have strict validation.
Unknown keys fail validation. Automatic merge defaults to off. A profile is resolved
to immutable canonical JSON and a SHA-256 identifier before execution; candidate
source cannot replace it. Profiles are activated only after complete validation.

The trusted profile can select an agent harness separately through
`builder_harness` and `reviewer_harness`. Both currently support only `native`,
the existing Factory agent. `anton` and `codex` are reserved names and fail
validation until their adapters are available; there is no automatic fallback.
These fields select the agent runtime independently of the pinned role models.
Task descriptions and repository files cannot select a different harness.

Omitting both fields keeps existing canonical profiles and fingerprints unchanged.
Explicit selections are frozen in the profile and change its fingerprint, so
operators must validate and activate that configuration deliberately.
`product-validate` reports both effective harnesses even when the fields are omitted.

`ProductPlan` provides a bounded, versioned planning artifact tied to the exact
task/criteria, base commit, profile and trusted historical planner role. Every
criterion needs a validation mapping. Ready guidance requires investigated source,
constraints, intended changes, risks and no unresolved questions. Blocked and
unverified guidance retains its reason. Plans cannot change executable policy.
The journal freezes `plan.json` once; matching reads do not rewrite it and conflicts
or tampering fail closed. This contract alone does not enable planner execution
or alter legacy runs. Delivery integration follows separately.

The embedded Anton SDK checks VCS provenance at `537cc1fb11849e9d152870738af22ca55c1d59d8`.
It exposes only isolated `run` and validated `finish`; native scratchpads, web,
memory, personal settings and telemetry are disabled.
Every provider call, including SDK helpers, passes shared accounting and admission.
Active request cancellation closes transport. Selection awaits runtime and delivery qualification; SDK skips are not evidence.
The controller build installs the exact binary dependency hashes in
`anton_requirements.txt`, then builds only the pinned SDK commit without resolving
dependencies. Runtime validation rejects changed package versions. The dedicated
`anton` CI job installs that same environment and imports the SDK before testing.
Candidate setup and credential isolation remain separate from this controller SDK.

## Supported product workflow

`product-validate` checks a profile and the target checkout identity.
`product-deliver` accepts an explicit task JSON file and produces a verified
candidate. `--publish` enables GitHub publication; `auto_merge` in the trusted
profile decides whether it stops at PR-ready or promotes the change. Existing
Linear/staged automation retains its separate PR-ready boundary.

`product-inspect` reads a run summary and optionally its event history. Product
commands never discover `.env` files in the target working directory. Use process
credentials or an explicitly selected controller `--env-file` outside the target.
No credentials, Git metadata or Docker socket enter execution containers.

Task files contain `id`, `title`, `goal` and `acceptance_criteria`. Every criterion
has a unique `id`, a `description`, and an executable argument-array `command`.
These files are authenticated controller input, not raw issue text. They must be
outside candidate source. Builder/verifier prompts receive descriptions; the
controller runs the executable criteria independently. Required repository checks
come from the frozen profile and cannot be waived by an agent report.

The initial automatic promotion policy supports one small commit based directly
on the tested base. It rechecks actual scope, evidence, task/profile hashes, current
GitHub checks and PR head. A fast-forward push uses an atomic expected-base lease;
concurrent remote changes reject it. Existing branch protections apply without a
bypass. Repositories requiring a merge queue or another merge strategy need a
separate adapter; this path fails closed. GitHub must confirm that the exact
candidate is merged, and fresh post-merge smoke commands must pass before the
task becomes complete. An ambiguous network result or a failed post-merge smoke
leaves explicit evidence for reconciliation; it never becomes success by default.

## Frozen comparisons

`product-replay` accepts a version-1 JSON suite and a repetition count. Each case
contains `name`, `mode` (`build` or `verify`), a full `base_sha`, a complete `task`,
and `expected` (`verified` or `rejected`). Verification cases also require a full
`candidate_sha`. Commits must be available in the checkout. The profile pins the
runner image and configuration. Neither case names nor expected outcomes enter
agent prompts. Replay has no publication callback and cannot create or merge PRs.

Every repetition gets fresh evidence and an immutable scoring event, preserving
previous results. Defect evidence can establish rejection; timeouts, unavailable
commands, setup failures and missing model usage remain unverified. Report those
outcomes separately. Representative real tasks and held-out checks belong in
controller-owned suites, including failures, modest features and dependency work.
Role verification cases complement full build cases; they are not interchangeable.

The current runner supports regular-file repositories, without symlinks or
submodules, on one persistent non-root controller host with Docker and GitHub CLI.
Setup may have explicitly enabled networking; implementation and verification do
not. Setup cannot change source. Verification source is read-only, so projects
must direct generated output to temporary locations or provide appropriate checks.
Run time is checked between operations; API and command calls are individually
bounded. Token usage is checked after each response, so the input cost of the last
response may cross the token threshold.

Profiles also pin `platform` (`linux/amd64` by default, or `linux/arm64`). Optional
`pricing` maps exact returned model versions to explicit per-million `input`,
`cached_input` and `output` rates, `max_input_tokens`, a `source` URL and `as_of`
date. Every quoted response retains its rate and usage. Missing rates, unknown
usage and requests beyond the quoted context regime remain unpriced. Estimates
exclude execution costs and endpoint-specific billing adjustments; they are not
invoices. No model alias is silently mapped to another model's rate.

`product-metrics` reports accepted work items, blocked work, recorded intervention
and corrective events, and completion latency p50/p90 with sample counts. Inference
cost per accepted item includes unsuccessful delivery attempts. Replay and
improvement work are separate. Missing cost coverage yields an unknown total,
with the known subtotal retained; external human work is never inferred to be zero.

`product-improvements` groups established verification failures or model transport
failures across distinct delivery work items. Defaults require three work items,
produce at most three investigation proposals, and impose a 24-hour cooldown.
Previously covered evidence is deduplicated. Replays and tasks with
`origin: improvement` are excluded, preventing recursive observation. This command
does not invoke models, change code, schedule itself or adopt proposals. Proposed
fixes must acquire independent regression criteria and pass the normal delivery
and repeated replay process. Per-role models are explicit profile choices; adaptive
routing requires evidence and is not enabled by these proposals.

`agent_max_output_tokens` is an explicit profile setting (default 8,000), bounded
by the remaining run token budget. Builder and verifier use `builder_model` and
`reviewer_model` respectively. Incomplete responses retain their status/reason and
never execute a partial tool call. Base-resolution failures are recorded before
attempts begin; live runs retain the resolved base in an immutable event. Source
exports read raw committed blobs, including files marked export-ignore, and retain
executable bits. Archive attributes cannot hide source from verification.
