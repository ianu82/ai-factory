# Real browser fixtures (T1; shared contract for T2/T3)

Run from `aifactory/`:

- `.venv/bin/python -m pytest` collects these tests normally, alongside API tests.
- `.venv/bin/python -m pytest -m browser` selects the subtree (marker registered locally).
- `AIFACTORY_SKIP_BROWSER=1 .venv/bin/python -m pytest` skips the entire subtree
  during collection, before any build, server, Chromium, or per-test setup.

Prerequisites: the existing Python dev environment, Node/npm compatible with the
web package, and Playwright's matching Chromium (`.venv/bin/python -m playwright
install chromium`; Linux also needs the usual browser system libraries). No
pytest-playwright plugin, new dependencies, external credentials, or CI changes.

## Lifecycle and isolation

`web_build` runs the existing `npm run build -- --outDir <temporary directory>`
once per session, always fresh. It uses `npm ci --no-audit --no-fund` when Vite or
the installed dependency lock/tree is missing or mismatched. Builds/install have
120-second command bounds and captured logs. No checked-in dist is reused.

`browser_server` spawns a new Python interpreter, allowlisting only OS/runtime
environment variables: inherited bootstrap, provider, GitHub, OIDC, external DB
URLs (including `AIFACTORY_TEST_DATABASE_URL`) and data-source secrets never enter
the child. It explicitly sets temporary data/SQLite/jobs paths, loopback public
and internal URLs, dummy encryption secret, dev auth, and disabled orchestration.
It assigns `app.WEB_DIST` before `create_app(start_orchestrator=False)`. The parent
`tests/conftest.py::isolated_env` stays untouched; its per-test DB/settings resets
cannot affect this server. Readiness checks `/api/health` and the built HTML with
a 30-second bound. An OS-bound loopback socket is passed directly to Uvicorn, so
there is no free-port race. Startup errors include the server log and its path.
The child is terminated/joined (killed/joined if necessary) on every exit path.

`browser` starts and closes Chromium per test, so synchronous Playwright’s event
loop cannot leak into unrelated asyncio tests. `browser_context` creates fresh
cookies/storage for every test/viewport and always closes it. The centrally parametrized
`viewport` is 1440x900 or 375x812. Tests using pages automatically run at both.
`scenario` creates a fresh identity/workspace through the real `/auth/dev` handler
and seeds that workspace only. The UI defaults to “Local development”; the
previous test's organization binding is retired before creating the next one,
so the next identity cannot inherit memberships. These fixtures are serial per
pytest worker; do not create overlapping scenarios on a single server. Each
worker gets its own server if an external parallel runner is used.

## Fixture interface

- `signed_in_page`: opens the real sign-in page, fills the scenario email, clicks
  **Dev sign-in**, and waits for that workspace's Inbox heading and URL.
- `unsigned_page`: fresh page with no auth cookie. Request `scenario` as well when
  testing sign-in explicitly; fill `scenario['email']` into Local development email.
- `browser_context`: use for extra tabs; all open pages are diagnosed on failure.
- `scenario`: dict with `email`, `name` (workspace display name), `workspace_id`,
  `repo_id`, `outcomes`/`plans`/`tasks` maps keyed by outcome status, `decisions`
  keyed by kind, `feedback_id`, and `improvement_id`.
- `navigate(page, 'Outcomes')`: opens mobile navigation when necessary and clicks
  the visible sidebar link. Supports Inbox, Pull requests, Runs, Signals,
  Decisions, Settings. Assert the destination's visible state afterward.
- `visible(locator, text=None)`: Playwright visible-state assertion with optional
  text. Prefer role/label selectors and Playwright `expect`, never sleeps.
- `no_overflow(page)`: bounded explicit wait/assertion on document width, with
  viewport/content sizes on failure. Mobile search is hidden by the product;
  do not invent a mobile search control.
- `checkpoint('outcomes')`: capture every open page after its visible assertions,
  before navigating elsewhere. Saves earlier-page evidence if a later step fails.

### Seeded scenarios

Outcomes numbered 1–9 are titled `Scenario <status>` in this order: `planning`,
`awaiting_approval`, `needs_input`, `running`, `attention`, `blocked`, `completed`,
`partially_completed`, `cancelled`. `outcome_seq=9`, so new captures start at #10.
Each has a linked plan. The proposed `awaiting_approval` plan contains a valid
materializable `T1` for `acme/app` and deliberately has no pre-existing task.
Other plans have T1, verifier runs, per-attempt checks/criterion evidence, and a
`Verification evidence` report artifact. Provider/GitHub credentials are encrypted dummy
values with explicit fake validation metadata. Feedback intake starts configured
with a throwaway, one-way hashed key, matching the production credential format. No real provider runs execute;
model catalogue returns `gpt-test`, and unhandled outbound async HTTP fails closed.

Six open decisions are titled `Browser <kind>`: `approve_plan`, `approve_merge`,
`question`, `unblock`, `budget`, `approve_release`. The question supports
`answer` (use a note and Remember for precedent testing); retry decisions are
linked to failed tasks. Approve-plan uses `approve`/`replan`/`cancel`.
The editable improvement is **Teach builders to test empty states** with a
`team_note` action and linked evidence; feedback is **Make empty states clearer**.
Repository context initially contains `Use pytest.`; autonomy is approval-only.

The child installs a subclass of `tests/test_pulls.py::FakeGitHub` server-side,
resets its call log and Pulls caches, and never changes shared fake helpers.
PRs: #1 Ready one, #2 Failing CI, #3 Conflicted, #4 Still checking, #5 Work in
progress (draft), #6 Factory awaiting approval (linked to running outcome T1
and its merge decision), #7 Reviewed (changes requested), #10 Unresolved bot
finding, #11 Factory revising after review; #8 Shipped and #9 Closed unmerged
are available in closed views. This supplies all open groups, check details,
conflict/review reasons and the Factory link without browser API interception.

### Private control (T3)

`control(op, **payload)` is a private multiprocessing pipe, not a route or a
browser API mock. Commands are workspace-scoped to the current seeded scenario:

- `control('wait_stream', connections=1)` waits for an active real stream and at
  least N accepted connections whose SSE response has started (10-second bound,
  no sleeps). This waits for the real `/stream` subscription, not just a request.
- `control('stream_state')` returns `active`, cumulative `connections`, and
  `rejected` reconnect counts.
- `control('interrupt_stream', reject_reconnects=True)` cancels active ASGI stream
  tasks and rejects subsequent stream requests with HTTP 503, returning
  `interrupted`. Omitting the flag retains T1's interruption-only behavior.
- `control('restore_stream')` stops rejecting reconnects. Tests restore in a
  `finally` block even if their offline assertion fails.
- `control('create_signal', title='...')` commits a new feedback signal but emits
  **no event**; returns `signal_id`. Titles are bounded to 3–100 characters.
- `control('notify_signal', signal_id=...)` validates workspace ownership, records
  a feedback event and emits the real `events.notify(..., 'signal', ...)` update.
- `control('emit', title='Updated from the server')` commits a new planning
  outcome using the workspace sequence, then emits the normal `outcome_created`
  event. Returns `outcome_id` and `event_id`. Title is bounded to 1–100 characters.

The operations test uses nonpolling Signals, a five-second visible-update bound
(below the shortest production polling interval, ten seconds), native EventSource
and the real backend `/stream`. It proves an emitted event refreshes the page,
then interrupts the stream, waits for the visible offline banner and a rejected
reconnect, commits an unannounced signal during the gap, and restores the stream.
Only production reconnect resync can recover that missed signal. A per-document
sentinel, unchanged URL and zero document requests prohibit reload/navigation.
Tests never replace EventSource, dispatch focus events, invalidate queries, or
change production reconnect timing. The low-level
`browser_server.control('create_workspace')` creates scenarios for the fixture;
other commands reject workspaces not created by this server. Do not call control
concurrently or use application imports/DB sessions in the test process to try
to change server state.

## Failure artifacts

PNG files live in pytest's temporary base under `browser-artifacts*/`. Failure
reports print node ID, viewport, each page URL, and absolute **PNG:** paths.
Names include node hash, viewport, phase/checkpoint, page index and a unique
suffix. Setup/call failures capture immediately before fixture teardown; final
screenshots are staged before contexts close to cover later teardown failures.
Checkpoints and staged captures are retained only on failure, removed on success.
Use `--basetemp=/your/artifact/path` to choose a durable location. Server logs
are under `browser-server*/server.log`, build logs under `browser-web*/build.log`.
Diagnostics mask the entire Settings Product feedback section (including the curl
example) in every screenshot. One-time-key assertions transfer only booleans to
Python and avoid locator assertion snapshots while the key is present; no key
value is printed or saved. Do not enable tracing, video, DOM dumps or raw response
logging for the key-rotation flow.

## Operations coverage (T3)

`test_operations.py` runs three grouped tests at **1440×900** and **375×812**:

- Settings: named Merges policy, unrelated Releases policy unchanged, save/reopen
  repository context, configured feedback key replacement with confirmation,
  nonempty read-only one-time input and copy-once explanation, then disappearance
  on leaving/reopening while the configured masked state remains. Persistence is
  also checked after a fresh document, rather than relying on query-cache state.
- Signals: real feedback dialog/new row, edit and apply the seeded improvement,
  handled/applied row, and Settings context containing both the original text and
  the edited note. Runs gets a seeded table smoke visit.
- Pull requests: real backend with the server-side GitHub fake, all open groups,
  collapse/expand, Open/Ready/Merged/All filters, failed checks/conflict/requested
  changes/Factory-approval reasons, internal outcome link, correct T1 and activity
  panel. No GitHub browser navigation or browser-side pull-response interception.
- Live delivery/recovery as described above. Every visited state has a horizontal
  overflow check and checkpoint, including filled dialog, expanded panels, and
  the disconnected banner.

No product source changes or locator hooks were required. T2's flows 1–4 are a
separate task; the complete marker suite measured here is everything present in
this checkout, including T1, existing layout/workstream tests and T3 (26 cases).

## Verification record (2026-11-13)

From `aifactory/`, run **five separate pytest sessions**, no retries/plugins that
retry failures, always a fresh fixture build/server. Bash wall time includes
collection, build, server startup, all tests and teardown:

```bash
TIMEFORMAT='WALL_SECONDS=%R'
for run in 1 2 3 4 5; do
  { time .venv/bin/python -m pytest -q -m browser -p no:warnings; } > /tmp/t3-browser-final-$run.log 2>&1
  status=$?
  echo "run=$run exit=$status"
  tail -3 /tmp/t3-browser-final-$run.log
  [ "$status" -eq 0 ] || break
done
```

| Run | Result | Wall seconds (limit: 180) |
| --- | --- | ---: |
| 1 | 26 passed | 84.778 |
| 2 | 26 passed | 83.503 |
| 3 | 26 passed | 84.816 |
| 4 | 26 passed | 82.168 |
| 5 | 26 passed | 81.616 |

An initial exploratory run found a Settings form initialized from stale cached
repository context during a background refetch; the persistence check now uses
a fresh document before opening the repository. The five runs above are after
that adjustment, not retries masking failed runs.

### Negative controls

A temporary pytest plugin outside the repository disables the proposal's Apply
POST (not the test assertion), or suppresses only server-side live emission.
This leaves real rendering/assertions/diagnostics intact. Exact reproduction:

```bash
cat > /tmp/t3_negative.py <<'PYTEST_PLUGIN'
import os
import pytest

@pytest.fixture(autouse=True)
def broken_operation(request, monkeypatch):
    mode = os.environ['T3_NEGATIVE']
    if mode == 'apply':
        page = request.getfixturevalue('signed_in_page')
        page.route('**/signals/*', lambda route: route.abort('failed')
                   if route.request.method == 'POST' else route.continue_())
    elif mode == 'event':
        from tests.browser.server import BrowserServer
        original = BrowserServer.control
        def control(self, op, **payload):
            if op == 'notify_signal':
                return {'event_id': 'intentionally-not-emitted'}
            return original(self, op, **payload)
        monkeypatch.setattr(BrowserServer, 'control', control)
PYTEST_PLUGIN
PYTHONPATH="/tmp${PYTHONPATH:+:$PYTHONPATH}" T3_NEGATIVE=apply .venv/bin/python -m pytest -q -p no:warnings -p t3_negative \
  'tests/browser/test_operations.py::test_settings_and_signals[1440x900]' \
  --basetemp=/tmp/t3-negative-apply > /tmp/t3-negative-apply.log 2>&1
PYTHONPATH="/tmp${PYTHONPATH:+:$PYTHONPATH}" T3_NEGATIVE=event .venv/bin/python -m pytest -q -p no:warnings -p t3_negative \
  'tests/browser/test_operations.py::test_live_signal_event_and_reconnect_resync[1440x900]' \
  --basetemp=/tmp/t3-negative-event > /tmp/t3-negative-event.log 2>&1
```

Both returned **exit 1 as expected**. Broken Apply failed at the visible proposal
remaining (`expected count 0, actual 1`), with the filled-dialog, edited-proposal
and immediate-failure PNG paths in `/tmp/t3-negative-apply.log`. Suppressed event
failed at the five-second visible-title assertion with subscribed-page/failure
PNGs in `/tmp/t3-negative-event.log`; polling could not rescue it. The plugin is
not committed or loaded by normal tests. Failure screenshots were inspected,
including the fully masked one-time feedback-key section.

Other configured checks:

```bash
# From aifactory/
.venv/bin/python -m pytest -q -p no:warnings --ignore=tests/test_codex_harness.py \
  --deselect tests/test_vm.py::test_runtime_qa_runs_inside_the_vm_sandbox
# From aifactory/web/
npx tsc --noEmit && npx vite build --outDir "${TMPDIR:-/tmp}/t3-web-check"
```

Type-check and build passed (Vite's existing large-chunk advisory only).
The configured full pytest command was stopped at its **1500-second limit**:
progress reached **53%**, with no failures printed (some environment-dependent
skips). Log: `/tmp/t3-full-pytest.log`. It did not finish, so this is **not a full
suite pass**. Multiple other factory jobs were running pytest concurrently in
this sandbox; their impact on timing was not isolated. No unrelated test or
configuration was changed to work around the limit. T1's reported main-branch
check also exceeded 1500 seconds, but no independent unchanged-main timing was
measured here.

The directly related existing API tests also passed:
`.venv/bin/python -m pytest -q -p no:warnings tests/test_insights.py tests/test_pulls.py`
(**18 passed**, 18.081 seconds). The browser runs above include
`test_sync_playwright_does_not_leak_into_later_async_tests` on every run.
