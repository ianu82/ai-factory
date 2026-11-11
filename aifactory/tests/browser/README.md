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
`Verification evidence` report artifact. All credentials are encrypted dummy
values with explicit fake validation metadata. No real provider runs execute;
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
  least N accepted connections (10-second bound, no sleeps).
- `control('stream_state')` returns `active` and cumulative `connections` counts.
- `control('interrupt_stream')` cancels active ASGI stream tasks, returning
  `interrupted`; the browser must use its real EventSource recovery.
- `control('emit', title='Updated from the server')` commits a new planning
  outcome using the workspace sequence, then emits the normal `outcome_created`
  event. Returns `outcome_id` and `event_id`. Title is bounded to 1–100 characters.

Wait for a stream, interrupt it, wait for `connections=previous+1`, emit, then
assert the new title becomes visible without a page reload. The low-level
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
The smoke tests exercise rendering, fake Pulls, independent identities/decisions,
materializable plans/counters and actual stream reconnection; full product flows
remain T2/T3 work.
