# Codex action boundary and manual qualification

The isolated official CLI 0.167.0 loopback probe confirmed that dynamic tools use
the supported app-server `item/tool/call` callback. Its request catalogue can still
include built-ins after shell tools are disabled. Approval policy `never` is not
a tool deny list. No live provider call, key, candidate, deployment or backend
selection is involved in these probes.

`codex_actions.codex_action` accepts one Code Mode `functions.exec` action whose
entire program is `text(await tools.run(JSON))` or `text(await tools.finish(JSON))`.
It decodes JSON, rejects duplicate keys and validates the existing command/report
schemas without evaluating code. Built-ins, arbitrary JavaScript, multiple calls,
changed models and incomplete responses fail closed. This is a prerequisite for
a future inference bridge: that bridge must buffer and validate the complete
response before sending **any** executable stream item to app-server, account for
every request and enforce cancellation/deadline and cleanup outside prompts.

This parser is not an enabled adapter or a sandbox. A credential-free, isolated
app-server, frozen instructions/configuration, scoped inference transport, actual
SDK integration tests and real coding/negative/cancellation proofs remain required.
Ordinary Codex selection remains unavailable; native remains the default.

The controller image installs the official CLI from `codex_bridge/package-lock.json`
using `npm ci --ignore-scripts`. The wrapper and every platform package are fixed
at 0.167.0 with npm integrity hashes. Linux CI runs `--version` with an empty,
temporary home and no authentication variables. Installation and this version
check do not establish runtime isolation, protocol integration or certification.

`tests/test_codex_transport.py` exercises the actual pinned app-server with a
loopback-only provider, no inherited credentials and a one-request bound. It
checks a permitted report's real callback and rejects an attempted built-in
before streaming any action. The fixture terminates its owned process group and
joins its reader/server threads. Dedicated Linux CI requires the installed CLI;
an ordinary test environment without the optional CLI reports a capability skip.

`codex_stream.codex_sse` returns immutable SSE bytes after detaching and validating
the complete response. It yields no early executable item. The real-CLI tests use
this encoder directly. Admission, usage accounting, cancellation and lifetime
checks are still required before a future bridge sends these bytes to app-server.

`codex_request.codex_request` rejects model/reasoning/storage/tool-policy drift,
hosted tool declarations and unsupported history before inference. It detaches
context without changing it and supplies a missing output cap from the caller's
trusted limit and remaining aggregate tokens; a smaller explicit cap is preserved.
The caller still owns complete usage accounting, admission, deadlines and cleanup.

`codex_inference.CodexInference` admits and journals each controller-side request,
disables SDK retries, preserves unknown usage/cost, enforces aggregate tokens and
request/deadline/cancellation limits, and drains an interrupted request. It buffers
the provider response before returning validated SSE. The session must still own
and close the client and credential-free app-server; no backend is enabled here.

The pinned-client transport tests exercise actual HTTP serialization, disabled
retries, active request cancellation/deadline drain and client closure. They use
only a local mock transport and test credentials; they provide no live-model or
isolated app-server certification credit.

`codex_process.open_codex_process` owns a pinned CLI process group and fresh empty
home/work directories. It allows only the controller's canonical loopback proxy,
inherits only HOME/PATH, disables optional tools/background daemons, and retires
the group on normal or exceptional exit. The session still owns JSON-RPC reading,
thread sandbox policy, the scoped proxy, global lifetime and tool dispatch.
The real-CLI loopback action tests use this production process owner directly,
including both the accepted report callback and rejected built-in response.

`codex_http.read_codex_http` limits the future loopback proxy to an authenticated,
bounded JSON POST on its exact host/path (16KiB headers, 8MB body, 128 nested
containers). It rejects ambiguous framing/JSON,
browser-origin, compressed and upgraded requests without leaking raw errors.
The local session capability is separate from the controller's provider key.
This parser alone starts no listener, inference, tool or enabled backend.

`codex_proxy.open_codex_proxy` owns the scoped loopback listener and request tasks.
It authenticates each request, routes inference through the accounting gate and
buffers the validated SSE. Stop/deadline, disconnect and context exit cancel and
drain active provider work; the owning session still closes the provider client.

The combined transport tests connect the real pinned CLI to this proxy with a
fresh local capability header, then the accounting gate and actual pinned client
with a mock upstream. They check report delivery, pre-stream built-in rejection,
in-flight cancel/drain and cleanup. No actual provider key reaches app-server,
and this remains local integration evidence rather than live coding certification.

The inference gate also retains the approved action from the same detached
snapshot used to encode SSE. A session must consume that exact callback once
and acknowledge tool completion before another inference is admitted. Changed,
duplicate, unsolicited and overlapping callbacks stop the session. Thread/turn
ownership and actual tool execution remain the session's responsibilities.

`codex_rpc.codex_turn` binds callbacks to its owned thread/turn and verified
model, effort and read-only/network-disabled settings. It returns command results
using the pinned CLI's generated `contentItems`/`inputText`/`success` schema;
a command's nonzero exit remains a successful tool transport carrying that exit.
Only the exact approved run/finish action is dispatched. Unknown server requests
are rejected and bounded asynchronous pipes close on every exit. Actual CLI
round trips cover both zero and nonzero command results. Report acceptance,
process/proxy/client ownership and live certification remain separate gates.

`codex_session.CodexAgent` owns that client/proxy/process/turn lifetime, uses the
frozen historical role and shared report acceptance, and requires an abortable
owned job-command callback. Command work runs off-loop; interruption signals its
owner and waits for confirmed completion. Provider work drains before process
waits, and the client closes on every exit. Dependency drift fails before startup.
Client configuration, pricing and task context are detached; unreviewed SDK
environment overrides are rejected before they can alter headers or logging.
Local full-session tests cover report rejection, active provider/command stop and
cleanup. This still enables no selection or runtime; production wiring, exact
configuration/environment qualification and live delivery proofs remain required.

Delivery supplies an abortable command callback to each role. Its abort signal
latches a local stop; the existing job-command watcher remains the sole owner of
sandbox retirement. Repeated signals cannot issue duplicate retirement calls.

`codex_execution_snapshot` binds the exact CLI version, both installation locks,
model, effort, output/timeout limits and provider configuration without credentials.
CodexAgent requires an exact retained receipt before resources or inference;
missing, changed or ambiguous JSON fails closed. This receipt supports truthful
qualification manifests. It does not enable the public selection or replace
the delivery's frozen plan, role, source, profile and environment checks.

Protocol references: [official app-server documentation](https://learn.chatgpt.com/docs/app-server)
and [official configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference).

Manual certification uses the normal `product-validate`, `product-deliver` and
`product-replay` paths with BOTH `--codex-qualification` and a trusted profile
containing `"execution_lane":"codex-qualification"` plus at least one explicit
Codex role. The lane is retained in the canonical profile/hash. Ordinary loading
and automatic Linear intake reject it. The selected role uses CodexAgent and its
truthful execution receipt; planning stays native. Missing installation or drift
fails closed. This opt-in authorizes no claim replay or release: all planning,
isolation, independent QA, CI, exact merge, smoke, budget and audit gates remain.
Source and mock-inference qualifications do not establish live coding or deployed
certification. Production remains paused pending those proofs and release gates.
