# Execution startup observations

The opt-in WebSocket collector records fixed bounded counters, numeric HTTP/close
codes and whether known control sends returned. It stores no URLs, headers,
credentials, command/input/output bytes, close reasons or exception messages.
It delegates to the pinned `ws` implementation without adding event listeners,
changing emit return values, retries or SDK execution policy. Snapshots are detached.
Counters cover the collector's lifetime; code zero means unknown/unobserved, and
the last observed codes do not describe current health.

This preparatory module is not enabled by the production bridge. Subsequent
reviewed bridge/error integration must validate and retain only this fixed schema.
A send returning is not remote acknowledgement. Counts can saturate, control
frames can be unclassified, and missing observations remain unknown. These facts
cannot approve a task, establish liveness or diagnose an earlier failed run.
