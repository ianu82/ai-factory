# Bounded planning reports

Planner role version 5 investigates enough source to propose a plan, rather than
implementing or proving the eventual fix. Historical versions remain unchanged.
The controller supplies remaining request/time budgets on every planning request.
After actual investigation, only the structured report tool is offered on the
last allowed request or when at most 60 seconds remain. An unoffered command is
rejected before execution, even if the provider returns it.

The earlier reporting window accounts for setup time and a response followed by
a command crossing the previous 30-second threshold. It reserves capacity inside
the same work deadline; it grants no extra time, requests or tokens. Investigation
is still required before reporting, and insufficient evidence still blocks a plan.

`ready` describes a justified implementation plan with every original criterion
mapped to future validation. It is not implementation approval or proof that any
check ran. The controller supplies its exported-base binding and read-only/no-Git
environment facts; the planner records these limits and deferred checks honestly.
Missing source evidence, unresolved product questions or infeasible validation
still require `unverified` or `blocked`. No later delivery gate is waived, and an
earlier unverified report is never relabelled or reused.

The existing eight-request, 100,000-token and 120-second phase limits remain.
Planning consumes the shared run budget. A one-request allocation cannot support
both investigation and reporting and fails before hardware preparation. Deadline,
cancellation, missing usage, unresolved questions and invalid acceptance mappings
still stop implementation. The controller never manufactures a ready plan.

Twenty seconds of that same phase are reserved for stopping the job, returning
and checking source, and retaining the plan. Setup, commands and inference use
the earlier deadline. Finalization still fails closed at the original phase
deadline; exact investigation/source/mode/deadline checks are journaled where
they are observed, without deriving a cause from exception text.

Builder and verifier prompts, tools and budgets are unchanged. Existing failed
runs stay terminal; a new planning policy requires a fresh intake and fresh proof.
