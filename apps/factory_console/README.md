# MindsHub Factory Console

An independent initial UI for the Factory. Workspaces support multiple repositories,
engineering and non-coding work. The global Inbox sorts by priority then original
attention age, or chronologically. Workspace boards combine console-owned drafts
with read-only Factory run projections. Run detail shows actual recorded stage,
checks, PR links and terminal outcomes, with unknown usage/cost retained explicitly.

Master chat captures requests and unsent drafts in the selected workspace. Until a
fresh controller observation advertises a chat capability, requests are **saved**,
not queued for inference or delivery. Converting a message to work adds a backlog
item. No UI endpoint starts, resumes or retries a Factory claim, writes to Linear,
changes a deployment, or modifies authoritative run evidence.

## Deployment

Deploy this directory as a separate Railway service with its own volume at `/data`.
Use the Dockerfile in this directory. The pinned image contains only Python and
this console app; it needs no provider, repository or Railway API credential.
The service reads `PORT` (default8080) and persists `/data/console.db`.

The first boot waits for its independent preview configuration before opening HTTP.
Upload `credentials.json` to the console volume through Railway's authenticated
file browser. It contains two independently generated values of at least20characters:
`FACTORY_CONSOLE_ACCESS_KEY` and `FACTORY_CONSOLE_BRIDGE_TOKEN`. Boot makes the file
private, owns only this new volume and drops to UID10001 before starting HTTP.
Explicit environment variables can supply the same configuration for CI.
Never place controller/provider credentials here. `/health` is a fixed public
readiness response; application data requires the preview session or bridge token.
MindsHub identity/organisation integration is a later stage, not implied by this gate.

## Read-only Factory connection

The core owner reviews and launches `python -m apps.factory_console.bridge` in an
existing trusted controller environment. Supply explicit `--store` paths,
`--status-file`, `--runtime-sha`, exact HTTPS console `--endpoint`, and a private
UI-only `--token-file`. Configure `--interval` (5–300seconds) and `--max-seconds`
(10–86400seconds). Record and classify the owned observer PID separately from
Factory workers; do not restart or interfere with a delivery to add this UI.

Only `POST /bridge/snapshot` is used. The producer reads RunJournal with
`read_only=True`, bounds/caches observations, excludes raw commands, prompts,
provider errors and private paths, and refuses all HTTP redirects. An observation
is a read-only projection, not independent certification or a process heartbeat.
Missing/malformed executable outcomes stay unverified. No inference or dispatch
occurs in the publisher. The console marks the feed stale after90seconds.

## Validation

The root pytest entry point discovers `apps/factory_console/tests`. The dedicated
console workflow runs model/store/HTTP/bridge tests, module syntax checks, builds
the pinned image and qualifies the actual container: UID10001, read-only root,
private state, secure cookies, source isolation and signed projection ingestion.
Frontend composition and interactions are also checked in the rendered app at
desktop and narrow viewports; synthetic interaction fixtures stay in local test
state and are not deployed as product activity.

Brand assets and font files come from the existing MindsHub Cowork source.
