# Factory health evaluator v1

`auto_mindsdb_factory.health.evaluate_health(observation, *, now, max_age_seconds)`
is a pure, read-only adapter boundary, not a supervisor, HTTP endpoint, or Railway
monitor. It performs no I/O, probes, inference, scheduling or evidence writes.
The future adapter owns observation collection, trust, and atomic sampling; do not
feed unverified task/journal events into this contract as controller liveness.
In particular, the last journal event is activity, not a heartbeat. No event
history is needed or examined. This does not change run summary semantics.

The input is `None` (unavailable) or a plain dict. Its required fields are:

* `observed_at`: timezone-aware ISO 8601 timestamp (at most 40 characters).
  This is when the controller's state **and liveness** were actually verified by
  the controller or an independent observer, not the time an old event was read.
* `state`: `idle`, `paused`, `busy`, or `blocked`. Idle means no active
  work and no queued runnable work; paused is deliberate; busy has an active
  operation; blocked means the controller cannot progress (for example, it
  cannot process queued work), not merely that one task is blocked. If the
  producer cannot establish the state it must not invent one.
* `task_outcome`: `none`, `running`, `succeeded`, `failed`, `blocked`, or
  `unknown`, independently of controller state.
* For `busy`, `live` is a dict with `kind` (`process` or `heartbeat`) and
  `observed_at` (same timestamp format), and `deadline` is an aware ISO 8601
  timestamp. A `process` observation means the relevant process was verified
  alive at that time; a `heartbeat` must be a genuine live controller/command
  signal, not a journal event. The producer must associate both with the current
  operation. Snapshot and live evidence must each be refreshed during a long
  command; a start time alone is not sufficient.

Other fields are ignored, never echoed or traversed. Inputs must be bounded,
trusted adapter samples, not arbitrary decoded histories. `now` is an injected
aware `datetime`; `max_age_seconds` is an integer from 1 to 86400, inclusive,
used for both snapshot and live evidence. Invalid configuration raises a fixed
`ValueError`. At exactly the age limit evidence is fresh; at exactly the deadline
the command is overdue. Future-dated evidence is never accepted as fresh.

The JSON-serializable result has `version` (1), `state` and `task_outcome`
(the validated observed values, or `unknown`), `observation_age_seconds` and
`live_evidence_age_seconds` (nonnegative numbers or null), `liveness` (`alive`
or `unknown`), `readiness` (`ready`, `not_ready`, or `unknown`), `reason` and
`action` (fixed safe strings; action is null when none is needed). No raw input,
exception text, command, model payload, or credential is returned.

Fresh idle is alive/ready, even after a failed or blocked task (with a separate
inspection action). Paused and controller-blocked are alive/not_ready. Busy with
fresh live evidence and a future deadline is alive/not_ready (occupied, not a
claim of available capacity), with a separate task inspection action if needed; an exceeded deadline is also alive/not_ready and
requires inspection, not a claim that the process is dead. Missing, invalid,
future or stale required evidence makes liveness and readiness unknown. A fresh
snapshot is still required even when live evidence exists. These classifications
are conservative, not proof of service availability or task success.

Reason codes are `observation_missing`, `observation_invalid`,
`observation_future`, `observation_stale`, `idle`, `paused`, `blocked`, `busy`,
`busy_evidence_missing`, `busy_evidence_invalid`, `busy_evidence_future`,
`busy_evidence_stale`, `deadline_exceeded`, and `task_attention`. Consumers
should branch on version and codes, not on action text. Deployment integration,
actual monitoring, alert thresholds and operator workflows remain separate work.
