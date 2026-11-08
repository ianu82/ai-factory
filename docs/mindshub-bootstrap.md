# MindsHub controller bootstrap

This is an administrator bootstrap for a separately reviewed, privately qualified controller source. Source readiness is not live protocol certification or unattended delivery qualification. Ordinary validation still reports configured inference as not execution-ready. No default provider, harness, model, budget, release or repository permission changes here. The initial bootstrap uses controller-scoped inference; workspace admission and in-app key management remain separate work.

The `auto_mindsdb_factory.mindshub_bootstrap` module accepts an authorization JSON path and its independently approved SHA256, followed by the original `product-deliver` argument list. Never put a key in arguments, authorization JSON, environment, logs or candidate jobs. An owned inference worker may receive only a bounded private anonymous-pipe packet inside the qualified controller boundary; the context itself is never serialized. The existing root-only credential file is read only by the qualified resolver, once; the frozen context stays in controller memory after the permanent privilege drop.

The authorization document has exactly these fields:

- `version`: integer 1.
- `argv_sha256`: digest from `argv_digest` for the complete original normal-delivery arguments.
- `profile_sha256`: canonical resolved profile digest, including explicit inference and harness/model settings.
- `target_root`: canonical absolute target path.
- `receipt_path` and `receipt_sha256`: approved credential-free Auth receipt identity.
- `credential_file` and `credential_version`: approved private path and exact six-integer file-version metadata; never the key.
- `reasoning_effort` and `timeout_seconds`: explicit approved settings.
- `uid` and `gid`: approved nonroot controller identity.

The authorization file must be external, regular, non-symlink, root-owned and not group/other writable, under a root-owned 0700 parent. Its complete bytes are hash-pinned; duplicate/unknown JSON fields, nonfinite numbers, boolean integers, unsupported commands and profile/argument/target drift stop before credential resolution. The existing resolver independently checks the Auth receipt, policy/reference, key-file ownership/mode/version and import boundary.

A stdlib-only guard rejects untrusted startup paths before application/SDK imports, without modifying sys.path. Bootstrap validates imports and profile schema/hash as root, resolves once, clears supplementary groups, drops GID then UID and verifies the permanent identity. Normal repository origin validation runs after the drop; no root Git ownership exception is added. The shared internal `prepare_context` boundary returns only the private immutable context and admitted argument tuple; normal bootstrap immediately enters the same main/dispatcher/planning/builder/verifier/publication lifecycle. Fixed operator diagnostics may exercise that same boundary, with no unattended coding credit. Replay and Linear context admission remain unsupported. Required CI, independent review, every fresh smoke, durable outcome, cleanup and terminal audit remain mandatory.

Only main coordinates real execution after exact source CI/private qualification. The implementing engineer must not assist an admitted qualifying delivery. Retain authorization identity and normal run evidence without exporting secrets; failures remain failures and require a fresh authorized run after any repair.

Owned inference retirement retains the original absolute deadline. A slow spawn or failed OS kill/reap can leave `retirement_unconfirmed=true`; the safe journal retains the owned PID/process group, deadline-exceeded and actual elapsed facts. That run remains failed/unverified, cannot admit another request/tool, and must not be certified clean. Independent actual retirement verification is required before the terminal audit/idle gate permits qualification or re-admission; a kill attempt alone is not cleanup evidence.
