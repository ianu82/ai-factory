# Preview persistence and policy contract (T1)

Previews are independent resources, **not Runs or delivery Tasks**. T1 stores intent and
policy only: it does not allocate a VM, issue a GatewayGrant, expose a public app link,
start an agent, or participate in delivery concurrency. VM scheduling, access proxying,
source refresh, sleep/wake and teardown belong to dependent tasks.

## Identity and transactions

`models.Preview` has exactly one identity shape:

* PR: workspace_id + repository_id + positive pr_number; outcome_id is NULL.
* Outcome: workspace_id + outcome_id; repository_id and pr_number are NULL.

Database unique constraints enforce one row per PR and one row per outcome, including
stopped/destroyed records. Optional task_id is a link, never work created for a preview.
Outcome cost for a PR preview can be attributed through its associated task.

`previews.request_preview(session, workspace_id, PreviewRequest(...))` is the internal
get-or-create entry point. **Authorize membership before calling**. It validates all
repository/outcome/task ownership, and locks the workspace before looking up identity
(an UPDATE works on SQLite and PostgreSQL). Caller commits/rolls back the transaction.
Concurrent callers return the same durable identity and original expiry; duplicate
requests do not silently refresh source. Direct inserts are also constrained by the DB.
No network I/O is permitted while holding this transaction.

Repository disconnect retains this history rather than cascading preview deletion:
`DELETE /api/workspaces/{workspace_id}/repositories/{repo_id}` returns **409** if any
PR preview references the repository, or an outcome preview's desired_manifest or
running_manifest contains its repository_id. The same retention policy applies to both
identity shapes in every state, including asleep, stopped, failed or destroyed records.
Manifest references are matched in SQL on SQLite and PostgreSQL; the diagnostic loads
at most 21 rows and identifies up to 20 preview IDs, PR numbers or outcome IDs, and states
(and indicates additional references). Stopping a preview is not a way to delete its
identity or accounting evidence. Disconnect takes the same workspace lock as preview
creation before checking references, and holds it until commit; a concurrent request therefore
either persists its preview and blocks disconnect, or sees the disconnected repository
and returns 404. The existing restrictive foreign key remains unchanged on SQLite and
PostgreSQL. Source-refresh writers must take this same workspace lock before validating
repository ownership and replacing either manifest, so they cannot introduce references
to a concurrently disconnected repository. There is no preview-history deletion endpoint
in T1.

Source manifests are bounded lists (1–100) of `{repository_id, commit_sha}`; commit_sha
is an exact lowercase 40- or 64-character hexadecimal commit ID, never a moving branch.
A PR manifest contains only its repository; outcome manifests contain repositories in
Outcome.repo_ids. Duplicate repository entries and unknown fields are rejected. No
credentials, arbitrary environment configuration or production connection data belong
in manifests. Desired source is persisted before reconciliation; running_manifest is
NULL until a VM has confirmed what it actually runs. Replace JSON values rather than
mutating them in place (SQLAlchemy JSON is not mutable-tracked).

## Lifecycle and fencing

Explicit states: queued, starting, running, asleep, waking, stopped, failed.
Queued and waking are transient, **not aliases of running**. Starting/waking/running
consume active capacity; queued is waiting for admission. An asleep preview must not
still own a billable VM. Workers must account for unreleased allocations even after a
failure. `reason` is a bounded, credential-scrubbed human explanation, not raw logs.

`generation` starts at 1. Dependent reconcilers must increment it for new source or new
allocation intent, and condition updates on generation + claim. `claim` and
`lease_until` fence worker ownership across restarts. Recheck expiry, policy and claims
under a database lock immediately before acting; release the lock before VM calls.
Persist allocation intent first; never claim a verifier VM to satisfy preview capacity.
Provider operations should use PreviewAllocation.id as their idempotency key.

`expires_at` is computed once from creation time and the effective lifetime. ORM writes
reject changes to identity, creation time and expiry. Waking, updates, retries, visits
and policy changes **never extend expiry**. Bulk SQL bypasses ORM guards and must not
modify these columns. T1 does not install database triggers.

All timestamps are naive UTC, following db.utcnow(). `last_human_visit_at` is updated
only by an authenticated human visit (not health probes/polling). `state_changed_at`
tracks the latest state transition; started/stopped/destroyed timestamps are retained
for lifecycle evidence. Terminal records are retained, not deleted/recreated to reset
the lifetime. Do not return vm_id, claim or lease identifiers to workspace clients.

## Allocation and usage (T3/T9)

PreviewAllocation records a provider allocation intent per (preview_id, generation),
with provider, optional VM ID, pending state/reason, creation/allocation/release times.
Use a new generation for a replacement VM. A crash between provider allocation and
recording its response must reconcile using the allocation's stable idempotency key.

PreviewUsageInterval records actual billable intervals per (allocation_id, sequence).
Its end is NULL while open; persist the end when VM billing stops. Cost and hourly rate
are nullable USD numbers: **NULL means unknown, not zero**. Attribute usage through
allocation -> preview -> workspace and, if present, outcome/task. Workers must validate
finite nonnegative rates/costs before writing and must not double-count an interval.
No Run/Task/GatewayGrant is needed for accounting. Allocation state vocabulary and
provider reconciliation are owned by T3; cost estimation/aggregation by T9.

## Policy API

All paths have prefix `/api/workspaces/{workspace_id}/previews`:

* `GET /policy`: any workspace member (including viewer); returns factory, overrides,
  effective. Does not create policy rows as a side effect.
* `PUT /policy`: workspace admin/owner; replaces local idle_minutes/lifetime_hours
  overrides. Missing or NULL values inherit Factory limits. Unknown fields (including
  active_cap) are rejected.
* `PUT /policy/factory`: admin/owner of the bootstrap-managed workspace **only**;
  replaces Factory policy. Its trusted `_bootstrap_workspace` marker cannot be set
  through the generic workspace settings API. No marked workspace means fail closed.
* `GET /{preview_id}`: member-only durable record read, scoped to the path workspace.

Unauthenticated access returns 401; nonmembers and cross-workspace record reads return
404; insufficient role or global-policy authority returns 403; invalid values return
422. Successful policy writes record attributed `preview_policy_changed` events.
There is intentionally no public start/wake endpoint in T1.

Factory defaults: active_cap=5, idle_minutes=30, lifetime_hours=72. Values are strict
positive integers (not booleans, numeric strings, fractions, NaN or Infinity); integer
fields fit signed 32-bit DB precision and Factory lifetime is bounded to 8760 hours.
Workspace overrides cannot exceed current Factory limits. Later Factory reductions
clamp effective overrides without rewriting their saved preference or existing expiry.
Policy updates serialize through the singleton Factory row, including concurrent first
creation. Global cap is never taken from Workspace.settings; local policy also has a
dedicated table so generic settings writes cannot bypass validation.

## Schema evolution

All preview tables are new and registered in models.py. db.init_engine() creates them
through existing metadata.create_all; no legacy table columns, data rewrite or manual
migration are needed. Reopening a populated pre-preview database adds the tables and
their constraints/indexes. Subsequent reopening preserves identity, usage and policy.
Tests live in tests/test_previews.py and use the shared SQLite/PostgreSQL test fixture.
