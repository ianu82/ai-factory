# Railway controller and execution

Use one persistent controller replica (2 vCPU / 2 GB maximum) with a 10 GB volume
mounted at `/data`. Keep serverless off and restart policy Always. No public HTTP
or Docker endpoint is needed: the controller polls Linear every 60 seconds.

Each source export gets a fresh Railway VM, capped at 4 vCPU / 8 GB. Setup and
execution share their environment; builder, independent verifier and post-merge
smoke use separate exports. Only one VM is held at a time. VMs are destroyed on
completion and have a five-minute idle expiry if the controller disappears.
Heartbeats keep a worker alive while a model is reasoning. Railway bills actual
CPU and memory use, including VM filesystem cache; resource caps are not reserved
capacity. Model API usage is billed separately by OpenAI.

The controller holds OpenAI, Linear, repository-scoped GitHub and Railway project
credentials. A project token can read that project's environment variables, so
it belongs only in this private controller. Workers receive no controller env,
no private networking and no public domains. Candidate commands run inside the
existing non-root, digest-pinned, resource-limited Docker policy; network access
is available only for trusted setup. Source transfers reject links, Git metadata,
special files, traversal and oversized data. Generated environments stay on VMs.

The Docker image records the exact release commit. Deploy a dedicated release
branch, not automatic main updates: a factory merge must not restart its own
controller before smoke and completion evidence are durable. Upgrade only when
the controller is idle. Railway volume locking also excludes a second controller
process; do not attach another independently writable state store to this route.

First boot stays paused. It seeds trusted config under `/data/config`, initializes
a bare target repository at `/data/target`, and writes `/data/status.json`. Before
activation, stop the previous controller, copy and verify its complete immutable
store into `/data/evidence`, and import its retained Git refs from a bundle. Never
discard old claims or rewrite failed outcomes. Back up the volume and verify a
restart preserves claims before accepting new work.

Activation requires `/data/activation.json` containing exactly the canonical
`profile_sha` and `route_sha` reported in status. Missing or mismatched activation
keeps polling paused. To pause before an upgrade, move this activation file aside
and wait for the current cycle to finish. Restoring it re-enables polling. Treat a
partly completed claim after an unexpected restart as an incident: reconciliation
fails closed rather than rebuilding or guessing whether publication succeeded.

The deployed profile pins GPT-6.1-terra for builder and reviewer, limits PRs to 300
changed lines, protects deployment/credentials/dependencies, and requires both
`validate` and `sandbox` CI plus fresh post-merge smoke before reporting completion.
