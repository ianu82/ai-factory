# Runner preparation and planning budgets

Railway delivery admits the initial worker bootstrap under the existing run
deadline before starting the bounded planning phase. The first planning source
export reuses that prepared worker; no additional VM is allocated. Builder,
independent verifier and smoke exports still receive fresh workers.

The run clock starts before this preparation and is never reset or extended.
Preparation checks known usage, frozen configuration, cancellation and remaining
run time before allocating hardware. Its timeout is at most 300 seconds and is
clamped to the remaining run budget. Cancellation retires a worker even if its
creation acknowledgement arrives late.

Planning remains limited to 120 seconds, including source upload, dependency
setup, read-only investigation, model requests and plan validation. Its eight
request / 100,000-token ceilings and the shared run limits are unchanged. The
manifest explicitly records that runner preparation uses the shared run deadline.
`runner_preparation_started` and `runner_preparation_finished` retain the actual
worker identity and bootstrap duration; `planning_started` records its phase
limit and remaining run time. Unknown/exhausted usage or a retained plan cannot
admit another preparation. Historical runs remain inspectable and cannot resume.
