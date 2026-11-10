# Releasing the Factory

The Factory builds itself, and every deploy restarts it. A restart interrupts every running build, and each
interrupted step starts again. So merging and releasing are separate:

- **`main`** takes pull requests as soon as they are ready: CI green, review comments addressed, and the merge
  result tested. Merging no longer restarts production.
- **Staging** is a separate Railway environment that deploys every commit of `main`. It is where a change
  runs for the first time.
- **`release`** is the branch production deploys from. It only moves forward, except during a rollback, and
  only to a commit that staging has already run.

## The release checker

`aifactory release-check` runs every 15 minutes as a Railway cron service. Each run is short and safe to
interrupt. It works out the state from GitHub and the two deployments, does at most one thing, and stops. Moving
`release` is always the last action of a run, so the deploy it starts (which can restart the checker itself)
never cuts a run in half.

A run, in order:

1. **Stops while releases are paused.** That means `AIFACTORY_RELEASE_PAUSED=true`, or an open issue labelled
   `release-hold`.
2. **Confirms the current release.** Production must report the `release` commit on `/api/health` and have run it
   for 5 minutes. The commit then gets a green `factory/release` commit status. If production has not gone live
   on the release within 40 minutes of `release` moving:
   - the commit is marked **do-not-release** (a failed `factory/release` status);
   - the checker opens a `release-hold` issue, which stops all releases until a person closes it.

   A release that was already confirmed is never marked bad afterwards. If production stops answering (after
   retries over 5 minutes) or answers on another commit, the run skips and opens one "Production unhealthy" issue.
   The checker closes that issue itself when production recovers. If the outage lasts 45 minutes, the issue
   becomes a `release-hold`.
3. **Keeps releases at least 2 hours apart**, counted from when `release` last moved.
4. **Promotes the newest commit that has been on staging for 30 minutes**, when all of these hold:
   - staging has run it, or a later commit that contains it, for 30 minutes. Every run (even one that then waits
     for the release interval) smoke-checks staging and records on its current commit, as a `factory/staging`
     commit status, when staging started it and whether smoke passed. A busy `main` that keeps replacing
     staging's commit therefore still releases every 2 hours instead of waiting for a quiet half hour. Staging's
     data comes from earlier versions, so this also proves the upgrade. A restart of the same commit starts its
     30 minutes again, and a smoke failure sticks until then;
   - it changes the Factory's code (`aifactory/`) since the current release. Docs-only changes wait, because a
     release restarts production;
   - it is on `main`, ahead of `release`, and not marked do-not-release;
   - every GitHub check on it passed (checks still running hold the release);
   - staging passes smoke checks: the web app and its script bundle load, `/api/meta` answers, and `/api/me`
     refuses an anonymous caller. An older commit needs a passing result recorded while staging ran that commit
     itself; staging's current commit is checked again before promotion.

   The next runs then confirm production, as in step 2.

A failure before a promotion (failed checks, failed smoke, staging down) opens one issue per commit, titled
"Release check failed at <commit>", and changes nothing. Production keeps running the current release. "Held"
and "nothing to release" are normal outcomes and stay quiet. Each run prints one JSON line with its result and
steps, and exits non-zero on a failure.

## Railway setup

**Production** (`factory` service) deploys from the `release` branch.

**Staging** (environment `staging`, same project):

- a `factory` service deploying from `main`, waiting for CI;
- its own volume and SQLite database, like production, and its own `AIFACTORY_SECRET_KEY`;
- no production data and no production credentials;
- `AIFACTORY_ALLOWED_EMAILS=@mindsdb.com`;
- no real repositories connected. A sandbox repository for a canary outcome is a later step.

**Release checker** (service `release-check` in the production environment):

- **Build:** the Factory image built from `release`, so the gate only runs code that has passed the gate.
- **Start command:** `aifactory release-check`.
- **Cron schedule:** `*/15 * * * *`.

| Variable | Purpose |
| --- | --- |
| `AIFACTORY_STAGING_URL` | Staging's public URL |
| `AIFACTORY_PRODUCTION_URL` | Production's public URL |
| `AIFACTORY_RELEASE_GITHUB_TOKEN` | Token on `ianu82/ai-factory`. **Read and write:** Contents, Issues, Commit statuses. **Read-only:** Checks, Metadata |
| `AIFACTORY_RELEASE_REPO` | Default `ianu82/ai-factory` |
| `AIFACTORY_RELEASE_BRANCH` / `AIFACTORY_RELEASE_SOURCE_BRANCH` | Defaults `release` / `main` |
| `AIFACTORY_RELEASE_PATHS` | Comma-separated path prefixes that count as Factory code (default `aifactory/`) |
| `AIFACTORY_RELEASE_INTERVAL_MINUTES` | Minimum time between releases (default 120) |
| `AIFACTORY_RELEASE_SOAK_MINUTES` | How long staging must run the candidate (default 30) |
| `AIFACTORY_RELEASE_LIVE_TIMEOUT_MINUTES` / `AIFACTORY_RELEASE_STABLE_MINUTES` | Go-live deadline (40) and minimum uptime before confirming (5) |
| `AIFACTORY_RELEASE_OUTAGE_HOLD_MINUTES` | How long a confirmed release's production outage lasts before releases go on hold (default 45) |
| `AIFACTORY_RELEASE_PAUSED` | `true` stops releases, for example during a demo |

## Operating it

- **Dry run.** `aifactory release-check --dry-run` reports what a run would do and changes nothing.
- **Release sooner.** Use "Run now" on the cron service. The interval and soak still apply, so for an urgent fix,
  lower `AIFACTORY_RELEASE_INTERVAL_MINUTES` or `AIFACTORY_RELEASE_SOAK_MINUTES` for that run.
- **Pause.** Open an issue labelled `release-hold` (closing it resumes releases), or set
  `AIFACTORY_RELEASE_PAUSED=true`.
- **Roll back** a release that went bad in production:
  1. Open an issue labelled `release-hold`, unless the checker already opened one.
  2. Mark the bad commit do-not-release: a commit status on it with context `factory/release` and state
     `failure`. The checker sets this itself when a release fails to go live.
  3. Move `release` back to the last good release, the newest commit with a green `factory/release` status.
     This is the one time `release` moves backwards. Schema changes are additive (new tables and columns), so the
     previous version normally starts on the newer database. Check `/api/health`.
  4. Merge the fix into `main`. When staging runs it, close the hold. The checker confirms the rolled-back
     release and then promotes the fix. It never promotes the marked commit again, even though staging ran it.
- **Merging.** Merge one pull request at a time and confirm `main`'s CI on it before the next. Two individually
  green pull requests can still break `main` together, and staging shows it before production does.
