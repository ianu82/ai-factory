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
4. **Promotes the commit staging runs** when all of these hold:
   - staging has run it without restarting for 30 minutes. Staging's data comes from earlier versions, so this
     also proves the upgrade;
   - it changes the Factory's code (`aifactory/`) since the current release. Docs-only changes wait, because a
     release restarts production;
   - it is on `main`, ahead of `release`, and not marked do-not-release;
   - every GitHub check on it passed (checks still running hold the release);
   - staging passes smoke checks: the web app and its script bundle load, `/api/meta` answers, and `/api/me`
     refuses an anonymous caller.

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

## Operator relays without a restart

Bootstrap `resolve`, `guidance`, `retry` and `cancel` items apply only when production starts, which now happens
only on a release. To act sooner, post the same items as a command on the control issue the bootstrap names. It
is a Linear issue or a GitHub issue:

```json
"control": {"linear": {"issue": "ENG-1234"}, "operators": ["ian@example.com"]}
"control": {"repository": "ianu82/ai-factory", "issue": 123, "operators": ["ianu82"]}
```

A command is a comment that starts with `/factory` and holds one fenced `json` block, for example:

    /factory
    ```json
    {"resolve": [{"outcome": 39, "task": "T1", "kind": "approve_merge", "resolution": "reject",
                  "head": "5591799", "comment": "Codex P1: scrub the whole payload once, at the boundary."}],
     "guidance": [{"outcome": 39, "task": "T1", "body": "..."}],
     "retry": [{"outcome": 20}]}
    ```

The Factory polls the issue every 30 seconds, applies the items with the bootstrap's own rules (the `head` guard,
relays only outside the bootstrap workspace, a live `cancel` never discards started work), and replies on the
issue with what it applied or skipped. Nonces are optional; the comment id supplies them, so each command applies
once. A command first seen more than a day after it was posted is not applied. Keep the control issue out of
intake: no `factory-auto` label on Linear, no `factory` or `factory:auto` label on GitHub.

Who counts as an operator:

- **Linear.** `operators` lists Linear users by email or user id. A command counts only when its author is listed
  and is an active member of the Linear workspace (not deactivated, not a guest, not an app); this is read with
  each comment, so removing someone takes effect on the next poll. Comments from integrations or synced external
  users have no Linear author and never count. The Factory reads and replies with the bootstrap workspace's Linear
  API key (Settings → Linear), which needs comment write access; until that key is saved the channel logs the
  missing key and waits. Replies are posted as the key's owner, often an operator: they start with
  `Factory relay for`, which the poller skips, and never with `/factory`, so they are never taken as commands.
- **GitHub.** `operators` lists logins, and GitHub is asked for each command whether that login has write access
  to the repository. The Factory uses the workspace's GitHub access, which needs Issues write to reply.

Replies that fail (a missing permission, an outage) are retried with a growing delay: the next poll, then twice
as long each time, at most one try per command every ten minutes, with one log line per try. A channel whose
polls fail (a missing Linear key, an exhausted rate limit) backs off the same way and recovers on its first good
poll. Answers that never posted are retried after a restart too. A Linear reply carries an id fixed per command,
and the Factory looks that id up before each retry, so a reply whose response was lost is recognised rather than
posted twice, however long ago it was posted. Switching the channel leaves the old
channel's unanswered replies unposted; the commands themselves were applied.
