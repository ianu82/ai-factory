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
