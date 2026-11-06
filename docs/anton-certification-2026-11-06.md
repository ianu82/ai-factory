# Anton certification evidence — 6 November 2026

The useful coding delivery, independent defect rejection and live cancellation
qualifications below passed. The production controller still runs `8e9cc25130b23653a85f8ed4c965b266cd02ad19`,
with intake paused and activation absent. A safe runtime rollout remains required
before declaring Anton production certified. Codex, the separate native coding
proof and the rest of the [internal rollout plan](factory-internal-rollout-plan.md)
remain outstanding.

## Frozen execution contract

The official Anton SDK is pinned to `537cc1fb11849e9d152870738af22ca55c1d59d8`.
Builder and independent verifier use `gpt-6.1-terra`, high reasoning, with no fallback.
The calorie-tracker task and profile digests are respectively
`2ea47f25b3a8588d385d321dfee26dfbcd73dc0238caa184a435f5efc873c897` and
`9c412b42fdc9192c59474a54f0e01812f52bf375bb1d2ce8cffda33e112ecee9`.
Limits remain 1,000,000 shared tokens, 3,600 seconds, two attempts, 35 turns,
600 seconds per command and 300 changed lines. Helper calls count toward the run.

Jobs use the pinned Linux/amd64 image, non-root execution and the declared setup.
QA source is read-only, with network disabled, Git metadata absent and all five
controller credential variables absent. SDK credentials stay in the controller;
candidate code cannot grant tools or change trusted roles, models or limits.

## Useful coding delivery

`anton-calorie-build-20261106e` completed at 16:22:24 UTC on controller source
`c66b60669cb2e9bf97e16ad2296403ae4caa8f94`. A ready, frozen native plan preceded
the builder. Actual requests: three planner, 13 Anton builder and ten independent
Anton verifier. Observed usage: 562,312 tokens, complete; monetary cost unknown.

[Calorie-tracker PR12](https://github.com/ianu82/calorie-tracker/pull/12) repaired
the tooltip formatter's TypeScript types and removed overwritten Tailwind keys,
with eight compiler, configuration, formatter and existing nutrition tests. Its
93-line, three-file candidate and exact merge are
`15f7d092e51021aa2832528b46934eafb715d701`, based on
`2da5fa40ccf4d81b19b4864a10e8380578912436`.

All six named and criterion commands passed. The fresh independent report passed
every frozen criterion with no findings. Exact PR CI `42249713794` and main CI
`42251251095` passed validate and sandbox. The actual merge was confirmed and all
three fresh post-merge smoke commands exited zero before terminal completion.

Publication was **operator-assisted**, using eleven genuine controller-issued
requests through the public network-only handoff. The controller's repository
HTTP 404 was respected; no credential was moved and no access was widened. This
proves the recorded assisted delivery, not unattended repository publication.

## Independent semantic rejection

`replay-7edb2a46350341c2bcd7b28b8d1405dd` rejected the private one-line defect
candidate `83efde0e63aaff186045485278881a1d199306d0` at 18:46:01 UTC. All six
original executable checks passed first. Seven real Anton verifier requests then
produced an independent failed report covering all three original criteria.

The verifier read the actual chart-data initializer and `Line` data key with the
TypeScript AST, then exercised installed Recharts `getValueByDataKey`. The changed
`weight_kg` key returned `[undefined, undefined]` instead of the actual weight data
`[83.4, 82.5]`; the reproduction exited one. This is defect-specific rejection,
not credit for an absent model, setup failure or timeout. The candidate was never
approved or published. Usage: 132,456 tokens, complete; cost unknown. Nested
Docker was unavailable in this QA job, as the original report records.

## Live cancellation and deadline contracts

`replay-31e8d2aac834467a84a2bc9fe4fadad0` used a fresh read-only reference at the
good candidate. After all six checks passed, the actual provider request body
was sent and waiting for response headers. A separate credential-free child
invoked public `product-cancel`, which acknowledged cancellation with exit zero.
The SDK raised `CancelledError`, drained the active request and closed its HTTP
client. No later model request or agent tool occurred; the worker was destroyed.

The passive observer bound the transmitted body's SHA-256 to `model_requested`:
`da438b91018809144b28b8dac08f71d25d249e0662fb902f90812ceb4fff2c0a`.
It did not block requests or close the client itself. The original terminal result
remains unverified after intentional cancellation, operation failed, fixture
grade false; none is rewritten as approval or delivery. One request, no response,
zero **observed** tokens and incomplete usage leave cost and provider-side
completion unknown. The post-agent integrity check was not reached after cancel.

A separate pinned-SDK test against a real loopback HTTP socket stopped an in-flight
180-second request at an eight-second global fixture deadline (8.008 seconds
observed), drained it and closed the client. It made no provider request and used
no worker. This establishes the controlled deadline contract, not a paid live
provider or VM deadline proof. Original live profile limits were unchanged.

## Retained evidence and release boundary

All three terminal archives passed safe regular-member, checksum, frozen-input,
usage, cleanup and source-recovery audits. Existing bundles were reused; only new
temporary extraction/recovery directories were removed. The authoritative files
remain under `/data/direct-calorie-tracker-20261106/`; verified offline copies and
concise audit records are retained in the operator's existing evidence directory.

| Archive | SHA-256 | Verified files / safe members |
| --- | --- | --- |
| `anton-calorie-build-20261106e-evidence.tar.gz` | `30c692fadb2e2dcf94755da6bef3fc72f67ab9e14d3a1d82d802eb866397db5e` | 198 / 199 |
| `anton-negative-qa-20261106d-evidence.tar.gz` | `63e4e8d8003cdbfc20b11117062cefb34178db6af5c5a107104321680379582f` | 52 / 53 |
| `anton-live-cancel-verifier-20261106-evidence.tar.gz` | `2fd5024ba2250f61dad54151572f80de9e7d776ec3f44f425d85528880f09416` | 31 / 32 |

Negative QA and cancellation used exact source
`70338daa1cbcdcd40a47aec50e67aaee81bc990b` (PR129). Exact PR CI `42267802919`
and main CI `42269105909` passed validate, sandbox and Anton. Private qualification
passed 190 tests without skips, non-root, credentials absent and source unchanged.
Verification-only fixtures share real checks and independent review; normal
delivery and build replay still require a ready trusted implementation plan.

At 19:56 UTC, all 1,450 baseline hashes, 150 retained refs, eleven earlier terminal
manifests and ten direct terminal manifests/archives remained intact. The current
controller had no pending reporting, no active child and no active worker. The
bounded UI observation period had ended; its last accepted snapshot contained ten
runs. These are dated observations, not a continuing liveness guarantee.

The current backup list cannot accommodate a fresh pre/post-release pair without
additional retention action. No further deletion or plan change is authorized.
Keep rollout paused until the backup gate can actually pass, then perform the
deliberate exact-source idle release with configuration, history, worker cleanup,
fresh activation and post-release checks. No old activation is restored blindly.
