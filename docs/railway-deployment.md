# Railway controller and execution

Use one persistent controller replica (2 vCPU / 2 GB maximum) with a persistent
volume mounted at `/data`. Production uses Railway's default 50 GB maximum;
volume billing follows data stored, not that ceiling. Keep serverless off and
restart policy Always. No public HTTP
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

The Docker image records the exact release commit. Disable automatic main
deployments: a factory merge must not restart its own controller before smoke
and completion evidence are durable. Production currently uses manual releases
from `main` with auto deploy disabled. A dedicated release branch is also suitable
when Railway's branch selector is available. Keep `codex/railway-production` at
the deployed commit as the release record. Upgrade only when
the controller is idle. Railway volume locking also excludes a second controller
process; do not attach another independently writable state store to this route.

First boot stays paused. It seeds trusted config under `/data/config`, initializes
a bare target repository at `/data/target`, and writes `/data/status.json`. Before
activation, stop the previous controller, copy and verify its complete immutable
store into `/data/evidence`, and import its retained Git refs from a bundle. Never
discard old claims or rewrite failed outcomes. Back up the volume and verify a
restart preserves claims before accepting new work. Production has daily backups
retained for six days and weekly backups retained for 27 days. Confirm a completed
backup before relying on the schedule. Keep the previous controller unloaded,
including after a Mac login; its disabled launchd plist is retained outside
`LaunchAgents` for deliberate recovery only.

Activation requires `/data/activation.json` containing exactly the canonical
`profile_sha` and `route_sha` reported in status. Missing or mismatched activation
keeps polling paused. To pause before an upgrade, move this activation file aside
and wait for the current cycle to finish. Restoring it re-enables polling. Treat a
partly completed claim after an unexpected restart as an incident: reconciliation
fails closed rather than rebuilding or guessing whether publication succeeded.

The current default route opts into Linear status synchronization (version 3).
See the [workflow and recovery guide](linear-product-delivery.md). Existing volume
configuration is intentionally never overwritten at boot. To upgrade an older
comment-only route, pause intake and let its active cycle finish and synchronize
its outcome first. Back up the trusted route, activation and evidence; add the
Building, Verifying, CI / Merge, Merged and Blocked state IDs to the volume route
without changing its team, criteria or required label. Validate the route against
Linear using the new release, then restart the controller while still paused.
Verify the exact release and the new route fingerprint in status before writing
activation for that fingerprint. Do not restore the old activation blindly.
Retain all old claims: reconcile their board status from recorded outcomes, not
by returning them to Ready or dispatching them again.

The deployed profile pins GPT-6.1-terra for builder and reviewer, limits PRs to 300
changed lines, protects deployment/credentials/dependencies, and requires both
`validate` and `sandbox` CI plus fresh post-merge smoke before reporting completion.

The Linux amd64 job setup also installs uv 0.12.5 from its hash-pinned binary
wheel. Regression and smoke commands require that exact version and add the job's
`.venv/bin` to PATH before pytest. This runs the invalid-dependency-lock regression
inside source exports; missing uv must fail the command rather than skip coverage.
For an independent manual run, use the trusted profile's regression command so
the same capability check and PATH apply. Git metadata and Docker integration
limitations still require the separate exact-commit CI gates.

Existing volume profiles are not updated by a code release. Apply this setup
change only while intake is paused and all deliveries and reporting are idle.
Retain the old profile and activation, copy only the reviewed setup and regression
command changes, validate the new profile with the unchanged route, and write a
fresh activation for their exact fingerprints. The supervisor loads fingerprints
at startup, so restart the reviewed controller while still paused before activation.
Verify a source-export run executes
the invalid-lock test without a skip before accepting another task. Old claims
keep their original profile fingerprints and must not be replayed.
