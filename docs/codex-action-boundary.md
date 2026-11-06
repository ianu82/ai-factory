# Codex action boundary (preparation only)

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
Codex selection remains unavailable; no native or Anton behavior is changed.

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

Protocol references: [official app-server documentation](https://learn.chatgpt.com/docs/app-server)
and [official configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference).
