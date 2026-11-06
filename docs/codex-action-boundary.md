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

Protocol references: [official app-server documentation](https://learn.chatgpt.com/docs/app-server)
and [official configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference).
