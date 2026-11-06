# Factory Internal Rollout Plan

Approved direction: 6 November 2026. Execution owner: the main Factory build thread.

## Outcome

MindsHub and MindsDB team members can sign in to a dedicated Factory app, select a workspace spanning their repositories, request useful work, follow its progress, inspect evidence, and receive a verified result. The Factory then observes the released product and turns relevant feedback into the next piece of work. After this works reliably for our own team, onboard customers with their own context, environments and permissions.

Engineering is the opinionated starting point. Non-coding work uses the same ownership, planning, independent review, evidence and delivery loop, with checks appropriate to its artifact.

## Ownership and execution

- The main thread implements this plan directly in small, reviewable PRs. Factory execution is a capability under certification; the plan does not require the Factory to build itself.
- Continue the current certification work without interrupting a running operation. Reconcile the original backlog and its acceptance criteria with this sequence; retain outstanding obligations and credit completed work from its actual evidence.
- Use the existing [SOF-166–173 UI acceptance backlog](https://linear.app/mindsdb/issue/SOF-166) for the screenshot work. Audit the delivered UI against those items before implementing gaps. Historical screenshot fixtures are not product requirements.
- Each phase needs an implementation slice, checks, runtime or rendered evidence where applicable, and a short completion record. A PR, model verdict or local test pass alone does not complete a delivery.
- Keep failed claims and prior evidence immutable. A necessary continuation gets fresh identity and authorization; it does not resume or relabel a failed run.

The source of execution status remains the current main-thread evidence and authoritative service state. This roadmap does not replace those records or authorize an extra restart, budget increase, permission expansion or release-gate waiver.

At the start of each phase, map its criteria to existing code, evidence and backlog items. Implement the smallest missing vertical slice, verify it, and then advance its dependents. Preserve useful parallel preparation without crossing the existing runtime and rollout boundaries.

## Phase 0 Complete core certification

**First action:** finish the current guarded verification-fixture work and its exact CI, then qualify the exact execution environment before fresh negative and cancellation proofs. [PR129](https://github.com/ianu82/ai-factory/pull/129) was open when this plan was prepared; check its actual state before acting.

Reuse the useful Anton delivery already recorded through independent verification, exact CI, merge, fresh smoke and terminal audit. Finish the remaining negative, cancellation, deadline, accounting and cleanup obligations. Reconcile all previously agreed Anton, Codex and native proof requirements individually; one harness passing does not certify another.

The criteria below supplement existing commitments; they do not replace or relax their acceptance tests.

**Completion criteria**

- Useful real coding delivery follows the actual provider and SDK path with an independent report covering every frozen criterion, all named checks, exact candidate CI, confirmed exact merge, fresh smoke results and durable reporting.
- Deliberately defective candidates are rejected for the defect. Setup failure, a timeout or absent model execution remains unverified and earns no rejection credit.
- Cancellation and deadlines stop admission of new work, drain active requests or commands, clean up owned workers, preserve observed usage and prevent later promotion. Recovery does not duplicate external writes.
- Trusted roles, plans, model allocation, environments and limits remain frozen. Verification fixtures cannot bypass normal implementation planning or executable policy.
- Required audits and a deliberate safe idle rollout are complete, with backups, history, configuration and worker cleanup verified. Document the remaining limitations precisely.

Only then enable additional execution paths. Source readiness, a configured adapter and a live UI are separate milestones.

## Phase 1 Make workspaces reproducible

Create a small, versioned workspace configuration for product goals, repository bindings, relevant architecture, environment profiles, skills and product decisions. Resolve MindsHub engineering defaults with explicit workspace overrides and show the effective configuration in Settings.

Keep executable policy in trusted control-plane configuration. Repository content and conversation text can provide context but cannot grant tools, credentials, model changes, larger budgets or release authority. Freeze resolved inputs and their versions for each run.

**Completion criteria**

- Two internal workspaces with different repositories, setup requirements and skills run independently without configuration or context leaking between them.
- A workspace supports multiple repository bindings; each task names its allowed repositories and each receipt identifies the source it tested.
- A clean environment can run the declared setup and checks. Unsupported capabilities fail clearly before expensive work begins.
- Operators can inspect the selected context, skill versions, environment and policy; drift blocks execution rather than silently changing a run.

Start with files and structured records. Add a retrieval service only after a measured context problem requires one.

## Phase 2 Deliver the internal app workflow

Keep Factory as its own app. Inspect supported MindsHub Cowork interfaces and reuse authentication, inference and connectors through narrow adapters where available. Prefer shared primitives over a fork; propose a fork only if a concrete integration constraint warrants its ongoing maintenance.

Complete the existing UI backlog: Master chat, global and workspace Boards, Attention, Runs and evidence, Settings, New work, and responsive bounds. Retain working components and the compact MindsHub design language.

**Completion criteria**

- An internal team member can sign in through the chosen supported identity path, access an authorized workspace and submit a bounded engineering task without console commands.
- Master chat creates a reviewable task and frozen plan, submits it once through the qualified execution path and displays real progress. Reconnection never dispatches an old saved brief or duplicates a submission.
- A workspace can coordinate work across repositories using explicit child tasks and per-repository gates. The parent completes only when every required result is verified; partial merges remain explicit and do not trigger dependent promotion.
- Inbox supports priority with oldest-first ties and chronological order. Items needing human action show the problem, evidence and a concrete next action.
- Evidence distinguishes recorded commands, independent criterion approval, CI, merge, smoke, environment qualification and manual attachments. No missing screenshot or VM identity is inferred.
- A small non-coding task produces a source-referenced artifact, receives independent factual and editorial review, and is delivered inside the app. External publication or messaging follows its actual authorization rules.
- Session expiry, revoked access, stale feeds, interruptions and duplicate requests have tested outcomes. Secrets remain outside candidate execution and customer context.

This phase is complete when the internal team can use the product end to end, not merely save briefs or inspect controller activity.

## Phase 3 Connect product feedback to work

Start with scoped read-only production signals: errors, failed journeys, user reports and relevant product measures. Convert a signal into a deduplicated investigation containing its source, affected workspace, impact, reproduction information and evidence. Logs and user reports are untrusted input, not execution instructions.

**Completion criteria**

- A real internal product problem can be traced from detection through investigation, a bounded fix, independent verification, safe release and a post-release check of the original user journey.
- Repeated signals update the existing investigation instead of producing duplicate work. Noise, unresolved causes and failed reproductions remain explicit.
- Connector permissions and collected data are limited to the relevant workspace. Evidence excludes credentials and unnecessary personal data.
- Initially, proposals enter the existing backlog and priority view. Automatic execution waits for the narrower remediation authority in Phase 5.

## Phase 4 Reuse human decisions

Record product principles and precedents with their scope, approving human, source, version and supersession. Seed this with explicit decisions already made, including compact Inbox rows, minimal explanatory copy and inspectable verification evidence. Curate the decisions; do not indiscriminately copy private conversations into repositories.

**Completion criteria**

- A repeated question can be answered using a valid, cited precedent; a poorly framed question returns for revision; a genuinely new decision reaches the global Inbox.
- Workspace and customer boundaries hold. An agent-generated suggestion cannot approve itself or acquire authority by being stored as memory.
- Changed or conflicting human decisions invalidate the affected precedent. A correction influences subsequent runs without rewriting historical evidence.
- Resolved decisions leave the attention queue, while their reasoning remains accessible. Measure repeated escalations and human interventions.

## Phase 5 Permit bounded remediation and rehearse recovery

Allow automatic action only for explicitly authorized, reversible classes of work after the earlier phases pass. Bind authority to the workspace, resources, operation, risk and budgets. Enforce it outside prompts.

**Completion criteria**

- Detection can trigger a scoped repair through the existing independent review and release gates, with observable canary or post-release success criteria.
- Tests exercise a bad release, ambiguous external result, controller interruption, cancellation and rollback or safe forward recovery. The original history remains recoverable and external writes are not duplicated.
- Operators can stop admission and inspect active ownership. Automatic repair cannot change its own permissions, gates or budgets, rotate protected credentials, delete history or authorize irreversible actions.
- Recovery is demonstrated before widening autonomy. Measure time to detection and safe recovery; do not impose a deployment-count quota or treat an idle hour as an outage.

## Phase 6 Onboard customers

Use the same app and execution contracts with a separate workspace configuration for each customer. Provide validated templates for repository access, environment setup, context, skills, acceptance checks and permitted actions instead of maintaining bespoke Factory forks.

**Completion criteria**

- Tenant isolation, membership, secret boundaries, connector access, artifacts, decisions and resource attribution pass positive and negative tests.
- A second organization can onboard without editing Factory source and complete a useful authorized delivery with the same evidence standard.
- Customer-facing permissions and release policies are explicit. Support, retention, incident handling and recovery procedures have been exercised internally.
- The customer pilot starts only after the internal team can repeatedly complete the workflow and recover from its tested failure modes.

## Measures and completion

Baseline verified useful outcomes, time to safe delivery or repair, escaped defects, recovery time, cost per accepted outcome and human interventions. Include unsuccessful work in cost attribution; distinguish known subtotals from unknown totals and inference estimates from execution costs.

Every completed phase must link its accepted criteria to actual artifacts and observations. The release record identifies the source, configuration, environment, test results and deployed behavior. Keep screenshots and reports where they help a person judge the result; avoid copying routine polling data or unchanged exports.

Defer large agent fleets, a vector store, speculative routing optimization and broad production write access until a demonstrated bottleneck justifies them. Small PRs, independent review, exact-source checks and existing release protections remain in force.

The closed-feedback-loop and decision-memory ideas were informed by [the Spawn operating-model article](https://x.com/jsnnsa/status/2115623382721126826). Its scale, spending and model-superiority claims are not prerequisites or evidence for this plan.
