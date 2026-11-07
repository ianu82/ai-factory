# Execution startup observations

The opt-in WebSocket collector records fixed bounded counters, numeric HTTP/close
codes and whether known control sends returned. It stores no URLs, headers,
credentials, command/input/output bytes, close reasons or exception messages.
It delegates to the pinned `ws` implementation without adding event listeners,
changing emit return values, retries or SDK execution policy. Snapshots are detached.
Counters cover the collector's lifetime; code zero means unknown/unobserved, and
the last observed codes do not describe current health.

The controller bridge enables the collector and returns its fixed snapshot only
for exact pinned SDK execution-start/interruption/control errors. Python accepts
only the complete bounded schema and freezes primitive values outside the mutable
exception. The accessor requires intact original failure provenance and returns a
detached copy; absent or malformed snapshots remain unknown. The pool binds the
current preparation journal and records fixed failure facts before cleanup or
cancellation can replace the runner error. This includes worker bootstrap and
source preparation and accepted current-worker heartbeat failures. Diagnostic write failure preserves the primary error and
retirement; missing evidence remains unknown, including a killed bridge process.
A send returning is not remote acknowledgement. Counts can saturate, control
frames can be unclassified, and missing observations remain unknown. These facts
cannot approve a task, establish liveness or diagnose an earlier failed run.
