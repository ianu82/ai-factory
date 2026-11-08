# Bounded planning reports

Planner role version 8 investigates enough source to propose a plan, rather than
implementing or proving the eventual fix. Historical versions remain unchanged.
Repository discovery stays inside `/workspace`, prunes generated directories and
bounds output. The first read-only batch should read applicable guidance, source,
test patterns and referenced requirements. Minimal images may lack `rg`; the role
requires available portable tools or the project runtime, with checked optional
tools and a fallback in the same batch. This guidance grants no new capability
and does not prove a model followed it. Unread evidence still blocks readiness.

Before the first command and on every later request, the controller supplies the
existing command capture policy: at most 12,000 bytes per stdout/stderr stream,
retaining its tail and decoding UTF-8 with replacement. Both Docker and Railway
execution use the same Docker capture implementation. Overflow can discard early
source even when the command succeeds. The new role budgets the whole batch,
including labels and discovery, with margin; individual line limits alone do not
bound combined bytes. It prioritizes essential guidance and source control flow,
then concise configuration, test patterns and requirement excerpts. A truncated
result remains incomplete evidence. These are planning instructions, not proof
that an actual model obeyed them. Capture limits and all readiness gates remain.

The first planning context also includes `source_inventory`: raw byte sizes from
the exact controller export, without file contents. This metadata is bounded to
6,000 UTF-8 bytes, prioritises permitted source and guidance, and explicitly counts
omitted entries. It helps allocate each section before reading; it cannot satisfy
investigation or establish readiness. The role requires allowances for all sections
and labels before output, rather than slicing a combined batch that loses evidence.
The actual read-only command, source/mode/deadline checks and report reserve remain
mandatory and unchanged. An inventory is not proof that the model used it correctly.

The controller supplies remaining request/time budgets on every planning request.
After actual investigation, only the structured report tool is offered on the
last allowed request or when at most 60 seconds remain. An unoffered command is
rejected before execution, even if the provider returns it.

The earlier reporting window accounts for setup time and a response followed by
a command crossing the previous 30-second threshold. It reserves capacity inside
the same work deadline; it grants no extra time, requests or tokens. Investigation
is still required before reporting, and insufficient evidence still blocks a plan.

`ready` describes a justified implementation plan with every original criterion
mapped to future validation. It is not implementation approval or proof that any
check ran. The controller supplies its exported-base binding and read-only/no-Git
environment facts; the planner records these limits and deferred checks honestly.
Missing source evidence, unresolved product questions or infeasible validation
still require `unverified` or `blocked`. No later delivery gate is waived, and an
earlier unverified report is never relabelled or reused.

The existing eight-request, 100,000-token and 120-second phase limits remain.
Planning consumes the shared run budget. A one-request allocation cannot support
both investigation and reporting and fails before hardware preparation. Deadline,
cancellation, missing usage, unresolved questions and invalid acceptance mappings
still stop implementation. The controller never manufactures a ready plan.

Twenty seconds of that same phase are reserved for stopping the job, returning
and checking source, and retaining the plan. Setup, commands and inference use
the earlier deadline. Finalization still fails closed at the original phase
deadline; exact investigation/source/mode/deadline checks are journaled where
they are observed, without deriving a cause from exception text.

Setup and read-only planning sandbox entry now use the existing cancellation
watcher under the same work deadline. The entry operation drains before any
body or inference is admitted; a late entry retires its owned environment and
cannot download source as a successful context exit. The entry watcher ends
before command execution, whose existing watcher remains its sole abort owner.
If removal races creation and fails, retirement is retried after entry drains;
an attempted removal is never treated as confirmed cleanup.
Normal context cleanup and source validation still run on successful entry.
After the body and command watcher drain, a separate lifecycle watcher owns
context exit and source download. Setup uses its existing work deadline;
read-only planning uses the phase deadline, including its existing 20-second
reserve. An already expired exit retires the environment before starting a
download. An interrupted exit/download drains before failure is reported, and
a completion between polls still cannot admit work after its deadline. Failed
retirement remains unconfirmed and stops execution. No watcher spans the body.
Confirmed Docker removal is reused across command and finalization cleanup;
failed removals remain retryable. Cleanup failures retain the finalization phase
even when they replace an earlier planner exception.
Confirmed draining can take time; this does not claim that cleanup always ends
within the phase deadline or establish the cause of an earlier failed run.

Planning source preparation records fixed operation starts and finishes for
export, inventory, setup entry, each setup command, finalization and validation.
Finishes retain elapsed time and whether that operation returned, without command
arguments, exception text, source or provider content. Entry can include upload
and remote entry; finalization can include remote exit and download. A return is
not admission or proof of source integrity, cleanup or deadline compliance. A
missing finish leaves the operation outcome unknown. Observation writes fail
closed, and the existing cancellation owners, limits and ready-plan gates remain.
These operation observations describe activity; they do not establish liveness.

Builder and verifier prompts, tools and budgets are unchanged. Existing failed
runs stay terminal; a new planning policy requires a fresh intake and fresh proof.
# Bounded source inspection

Planner version 9 offers `inspect_source` on the existing approved Node image. Its first request selects that tool; later requests retain the existing investigation and report-reserve policy. Other images keep `run` and `finish`. Builder and verifier tools remain unchanged.

The planner selects relative paths, one-based line windows and optional literal matches. The controller constructs a fixed Node command and passes it through the same read-only job command owner, deadlines and cancellation. It reads no controller files. Results identify selections by request index and report returned line ranges, total line counts and explicit omissions. `ok` describes the requested window, not necessarily the whole file.

Serialized UTF-8 output stays below the existing capture limit. A section that does not fit is marked `omitted_budget` while other readable selections remain available. Missing essential source still requires an unverified or blocked plan. The helper adds no request, token, time, network, source-write or publication authority.

