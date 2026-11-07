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

# Controller execution budget

The bridge accepts an optional positive integer `budget_ms` for its existing SDK
abort signal. It can shorten the existing `timeoutSec + 30` signal limit and
covers connection negotiation and stdin as well as execution. Without it,
behavior is unchanged. Lifecycle/command owners bind the existing absolute phase
or run deadline. Python recalculates the remaining budget under the pool lock
before each upload, entry, command, exit and download; finalization receives its
existing reserve. The bridge deducts time spent looking up the sandbox before
admitting exec. No budget is sent to candidate code, heartbeat or destruction.
The Python bridge also bounds and reaps its Node child at that deadline,
rechecking after process spawn and excluding the absolute deadline from stdin.
A killed bridge has unknown transport metadata; the pool still owns VM retirement.
Worker bootstrap retains its separately bounded preparation owner.
SDK request draining and owned VM retirement remain necessary; this timer does
not establish a cleanup wall-time guarantee or diagnose an earlier failure.
