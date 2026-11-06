# Anton request and rejection evidence

The controller validates both SDK tool names and their complete parameter schemas
before inference. It sends its own strict `run` / `finish` definitions and requires
one tool action, matching the native session contract. Missing, duplicate, widened
or undeclared tools cannot reach the provider. Responses still require exactly one
approved, schema-valid call before SDK dispatch; no compatibility fallback or
extra retry is introduced.

Known policy rejections append `harness_policy_stopped` with a fixed allowlisted
reason at the actual decision site. The record never comes from exception state,
provider content, tool arguments or arbitrary names. Unknown labels remain unknown;
an evidence-write failure still stops without retry. Validated tool dispatch is
recorded before command execution. Cleanup uncertainty remains a stop.

Model selection, role versions, budgets, isolation and independent acceptance gates
are unchanged. Historical generic failures retain their original unknown cause;
this change does not retroactively diagnose or resume them. The separate one-call
protocol probe established that a valid call can pass, not why run c was rejected.
