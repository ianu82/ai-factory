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
response may cross the token threshold. Monetary cost is unavailable; token usage
is not an invoice.
