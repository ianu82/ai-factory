# Preview VM boundary (Railway SDK 3.16.0)

## Provider audit and availability

Inspected the installed `railway@3.16.0` package (`vmbridge/package-lock.json`),
particularly `dist/index.d.ts`: `SandboxNetworkIsolation`,
`SandboxCreationOptions`, `ExecOptions`, `ExecHandle`, and `Sandbox`.
On 12 November 2026, `npm view railway version dist-tags --json` returned
`3.16.0` as both the published version and `latest`; no newer supported SDK
policy was available. The pinned dependency is unchanged.

* `ISOLATED` is **peer isolation with public NAT egress**, not deny-egress.
  `PRIVATE` joins the environment network. Neither mode meets preview safety.
* Creation accepts `env` and `domains`; verification already supplies `{}` and
  `[]`. Preview creation refuses **before calling the provider**, regardless of
  supplied environment, domains, template or checkpoint arguments. No preview
  VM, build, app, preparation command, agent or grant is created.
* There is no supported egress firewall/policy API, native suspend/resume,
  private app-port proxy, or capacity/quota API. Quota is unknown, not zero or
  unlimited. Do not fabricate provider options or use list length as capacity.
* `exec` supports ephemeral processes, backpressured stdin, and stdout callbacks.
  Output callbacks are UTF-8, not binary: a private byte tunnel must encode on
  the VM, before the SDK sees it. `execHttp` truncates and is not suitable.
* `connect`, `refresh`, `destroy`, heartbeat and checkpoint operations exist;
  there is no need to change the verifier's existing lifecycle or shutdown
  behavior. Previews must never restore/fork verifier or builder checkpoints.

**Current result: previews are unavailable on this pinned provider.** This is
intentional, not a transient startup failure or a reason to use a local sandbox.
`preview_vm_available()` and `PreviewVM.create()` report this explicitly. There
is no application HTTP endpoint or UI change in this transport task.

## Private transport

`preview.mjs` implements a bounded, pull-based duplex byte tunnel over SDK exec
stdin/stdout to fixed VM-loopback ports 3000, 8000 or 8080. It never uses domains,
provider port publication, control-plane fetch, or URL-name filtering. HTTP
headers (including repeated Set-Cookie and Location), bodies, upgrades and
WebSocket frames remain bytes; redirects are not followed by the tunnel.
Frames are at most 49,152 decoded bytes; each tunnel buffers at most 1 MiB
and 256 frames (including empty frames);
there are at most 32 tunnels. A slow consumer/overflow or interrupted exec
fails the connection rather than returning truncated success. The remote
helper has socket deadlines and the exec has a one-hour maximum lifetime.
Callers await writes for stdin backpressure and poll reads; empty/non-EOF is
not end of stream. Remote diagnostics are not forwarded to logs.

Preview ownership is separate from ordinary bridge boxes: unknown, verifier
or builder IDs cannot open tunnels or reconnect as previews. Since no safe
preview can currently be created, the bridge's ownership map stays empty.
The transport implementation is tested with fakes, not claimed as a working
Railway preview deployment. It must not be enabled just because it works in a
verifier VM.

## Requirements to enable a future provider

The adapter must establish and attest a provider/network-enforced deny-all
IPv4 **and** IPv6 runtime policy, covering public, private, DNS, production,
metadata, gateway and control-plane destinations, before any app process runs.
An untrusted app must not be able to remove it, including through privilege
escalation or subprocesses. In-VM root firewall commands, hostname heuristics,
HTTP proxy filtering and application promises are not evidence of isolation.
If preparation needs dependency downloads, it must be a separate phase with
no controller credentials and with the runtime policy applied and verified
before app startup. There is no preparation phase executed in this adapter
while that transition cannot be guaranteed.

Always boot a fresh clean image (not QA_TEMPLATE, an agent image, or a
checkpoint). `clean_preview_archive` reads only blobs from an immutable Git
commit, never the host checkout, and excludes dot paths, local configuration,
credential helpers/key files, dependencies/build outputs, symlinks and
submodules. It limits file counts and bytes before loading across boundaries.
Use the trusted credential inventory for forbidden-content checks. This is not
an arbitrary-secret detector: never treat a filename filter alone as proof
that source contains no secrets. There is currently no upload/start API, so
no unscanned source can escape via preview creation.

Start commands, app environment and fixtures must come explicitly from trusted
QA configuration, never committed `.env`, `.factory` or PR start configuration;
only fixture credentials may be supplied. Never copy controller/operator,
GitHub, model, workspace credentials, agent grants, logs or host artifacts.
A future adapter needs independent canary tests of actual provider creation,
preparation, restriction attestation, upload, start and fixture injection
before enabling its capability. These are not satisfied by a fake saying
`available: true`; the Python adapter explicitly rejects that as well.

Sleep is confirmed destroy, not checkpoint/suspend. Wake builds fresh from a
pinned source again; exploratory data is disposable. A failed delete remains
retryable and must not be reported asleep/stopped. A missing VM or bridge
restart requires an explicit rebuild, never automatic replay of HTTP requests
or silently reconnecting a verifier VM. Existing shared bridge stdin-EOF
cleanup of owned sandboxes remains unchanged.

## Checks

`python -m pytest tests/test_preview_vm.py tests/test_vm.py` (omit the existing
browser-dependent QA test if browser binaries are unavailable). Run `npm ci
--ignore-scripts` in `vmbridge` first to exercise the pinned-sidecar integration
test. Node-only transport unit tests do not require Railway credentials.
No real-provider isolation or end-to-end preview is claimed: the SDK cannot
supply it, so the tested safe behavior is actionable unavailability.


## PR #375 Postgres investigation (12 November 2026)

The public GitHub API confirms that commit `b792f3f746c858b87c4f48cc7b51d7636cdf5276`
failed its `postgres` job, ID `130059654329`, in run `42881783668`.
The test step ran from 19:06:25 to 19:29:35 UTC on 10 November; install and
container startup succeeded. The annotations say only “Process completed with
exit code 1”; they name no failed test. The job-log API returns HTTP 403
(“Must have admin rights to Repository”), and the step-log web endpoint returns
404 without sign-in. Therefore no specific historical test failure or root
cause is claimed. Repository-authorized logs are needed to establish it.

Main has since merged Postgres performance/isolation changes (#391, #428,
#446); none were modified by this task. On this checkout, the complete SQLite
suite passed with four workers (same exclusions as the factory check).
A disposable local Postgres 18 run of the targeted VM/preview tests passed
(19 passed, one optional dependency skip, one deselected browser-dependent QA
test). A broader four-worker Postgres run reached about 60% without reported
failures before a 12-minute execution limit. This does not establish a full
Postgres pass or retroactively explain the old job; CI uses Postgres 17.
No unrelated test, helper, schema or CI setting was changed.
