# Bounded planning reports

Planner role version 3 investigates enough source to propose a plan, rather than
implementing or proving the eventual fix. Historical versions remain unchanged.
The controller supplies remaining request/time budgets on every planning request.
After actual investigation, only the structured report tool is offered on the
last allowed request or when at most 30 seconds remain. An unoffered command is
rejected before execution, even if the provider returns it.

The existing eight-request, 100,000-token and 120-second phase limits remain.
Planning consumes the shared run budget. A one-request allocation cannot support
both investigation and reporting and fails before hardware preparation. Deadline,
cancellation, missing usage, unresolved questions and invalid acceptance mappings
still stop implementation. The controller never manufactures a ready plan.

Builder and verifier prompts, tools and budgets are unchanged. Existing failed
runs stay terminal; a new planning policy requires a fresh intake and fresh proof.
