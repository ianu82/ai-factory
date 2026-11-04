# Product delivery milestone

Initial delivery validated on 2026-11-04 using factory commit
[`47ca210`](https://github.com/ianu82/ai-factory/commit/47ca210c1f03ab1b57659d680f06714ff0651957).
The subsequent revision-loop proof used
[`536821b`](https://github.com/ianu82/ai-factory/commit/536821b6a42a38022ff4e6d2e49f2828190130df).

A scoped request in a separate private TypeScript application completed the product
workflow: implementation, independent verification, GitHub PR creation, confirmed
merge of the verified commit, and fresh post-merge smoke checks. The product
repository contained no factory schemas or policy files. Product source and
task-specific evidence remain outside this public repository.

## What ran

1. A trusted external profile and task specified the checkout, pinned Linux runner,
   models, allowed paths, checks, budgets and executable acceptance criteria.
2. The builder changed the calculation and added regression tests in a disposable
   container. The controller derived actual paths and created the commit.
3. A fresh read-only verifier inspected the candidate, surrounding code and tests,
   and independently reran validation. Controller-owned acceptance commands were
   executed separately from the model's verdict.
4. Five required command gates passed: one module typecheck, one command covering
   three product unit tests, and three independent acceptance checks. Review covered each criterion
   with evidence and reported no blocking findings.
5. The controller published the PR, rechecked scope, current GitHub checks and the
   tested base, and promoted the single commit with an atomic expected-base lease.
   GitHub confirmed the exact candidate as merged.
6. A fresh environment reran the three product tests after merge. Only then did
   the durable run become complete.

The earlier live attempt exposed a verifier-model configuration bug and an
insufficient output allowance. It was blocked before publication. Its attempts,
usage and cost evidence were preserved; regression tests and diagnostics were
added before another live run. Accepted-work cost includes this unsuccessful work.

## Validation boundaries

The factory suite contains 340 tests. The standard suite passes with five
Docker-dependent cases skipped, and the separate Docker job covers those cases.
Real-container checks include credential/socket exclusion, blocked networking,
read-only source, timeout cleanup, incorrect-candidate rejection, scope rejection
and executable-bit preservation. Transaction tests exercise stale heads, stale
bases, a real concurrent Git-ref update and post-merge smoke failure.
Import and commit boundaries also reject case/Unicode portability aliases of Git
metadata, protecting the controller's worktree on case-insensitive filesystems.

The product had unrelated pre-existing full-application type errors. The accepted
task explicitly scoped its typecheck to the changed module and imported types.
This milestone establishes delivery of that verified change; it does not establish
full application readiness, UI correctness or deployment readiness.

## Frozen replay evidence

The original five-case suite produced four expected outcomes:

| Case | Observed outcome |
| --- | --- |
| Review the merged candidate | Verified |
| Review a deliberately restored regression | Rejected by executable checks |
| Rebuild the original bug fix | Unverified after a rounding-compatibility ambiguity |
| Implement a small aggregation feature | Verified; unpublished replay |
| Pin an existing dependency without changing resolved packages | Verified; unpublished replay |

Separate repeated verification accepted the good candidate three times and
rejected the deliberately broken candidate three times. Each run retained fresh
evidence; the initial four-of-five result was not rescored or overwritten.

The ambiguous case exposed two useful issues. Exact preservation of floating-point
artifacts conflicted with a direct per-serving numerical rule near half-cent
boundaries. The revision loop also discarded concrete review findings. The factory
now passes those findings to the builder and subsequent verifier, with unresolved
ambiguity remaining unverified. A separate product task defines the numerical
rule explicitly and requires a real regression test. A mutation check first proved
that the existing tests missed the edge by substituting a zero-safe legacy
calculation during test execution. The original suite and the clarified task use
separate versioned evidence.

The follow-up completed after a genuine review/revision cycle: the verifier
requested a missing function-level contract comment; the next candidate supplied
it, and the next verifier explicitly adjudicated the prior finding. Seven required
command gates passed, including mutation-based regression coverage. The factory
published and merged the follow-up PR, then passed fresh post-merge checks with
five product tests. A separate build replay of the clarified numerical contract
also verified successfully. The original ambiguous case remains unverified in
its original report.

These are limited smoke observations across representative task types. A larger
held-out corpus is needed before changing models or claiming broad reliability.

The short product workflow accepts authenticated controller-owned tasks. The
existing Linear/staged worker retains its PR-ready boundary. Automatic promotion
currently supports one small fast-forward commit on the tested base. Broader
merge strategies and autonomous intake need separate integration work.

## Reviewable changes

| Change | Pull requests |
| --- | --- |
| Locked, bounded CI; trusted profiles; immutable run evidence | [20](https://github.com/ianu82/ai-factory/pull/20), [21](https://github.com/ianu82/ai-factory/pull/21), [22](https://github.com/ianu82/ai-factory/pull/22) |
| Disposable execution and investigating agents | [23](https://github.com/ianu82/ai-factory/pull/23), [24](https://github.com/ianu82/ai-factory/pull/24) |
| Delivery and exact-base promotion | [25](https://github.com/ianu82/ai-factory/pull/25), [26](https://github.com/ianu82/ai-factory/pull/26) |
| Report diagnostics and lease concurrency | [27](https://github.com/ianu82/ai-factory/pull/27), [28](https://github.com/ianu82/ai-factory/pull/28) |
| CLI, frozen replay, outcomes and bounded proposals | [29](https://github.com/ianu82/ai-factory/pull/29), [30](https://github.com/ianu82/ai-factory/pull/30), [31](https://github.com/ianu82/ai-factory/pull/31) |
| Live-run fixes and exact committed-source export | [32](https://github.com/ianu82/ai-factory/pull/32) |
| Explicit output truncation and actionable revision feedback | [33](https://github.com/ianu82/ai-factory/pull/33), [34](https://github.com/ianu82/ai-factory/pull/34) |
