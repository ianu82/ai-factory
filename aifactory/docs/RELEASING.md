# Releasing the Factory

The Factory builds itself, and every deploy restarts it. A restart interrupts every running build, and each
interrupted step starts again. So merging and releasing are separate:

- **`main`** takes pull requests as soon as they are ready: CI green, review comments addressed, and the merge
  result tested. Merging no longer restarts production.
- **Staging** is a separate Railway environment that deploys every commit of `main`. It is where a change
  runs for the first time.
- **`release`** is the branch production deploys from. It only moves forward, and only to a commit that staging has
  already run.

## How a release happens

The release checker, `aifactory release-check`, runs every 2 hours as a Railway cron service. It uses the same
image as the Factory. It releases the commit staging is running only when all of these hold:

1. **Staging is healthy on that commit and has run it for at least 30 minutes without restarting.** Staging's data
   carries over from earlier versions, so this also proves the upgrade: the new code started on data written by
   older code.
2. **The commit changes the Factory's code (`aifactory/`) since the last release.** Docs-only changes wait for the
   next real release, because a release restarts production.
3. **Every GitHub check on the commit passed.** Checks still running hold the release until the next run.
4. **Staging passes smoke checks.** The web app and its script bundle load, `/api/meta` answers, and `/api/me`
   refuses an anonymous caller.
5. **The commit is on `main` and ahead of `release`.**

It then fast-forwards `release` through the GitHub API, which starts the production deploy. It waits for
production's `/api/health` to report the new commit (up to 40 minutes), then checks that production stays healthy
without restarting for 5 minutes.

A failure (checks failed, smoke failed, staging down, or production not live or restarting) opens a GitHub issue
titled "Release check failed at <commit>", or comments on the open one. Production keeps running the current
release. "Held" (soak not finished, checks still running) and "nothing to release" are normal outcomes and don't
open issues. Each run prints one JSON line with the result and the steps it took.

## Railway setup

**Production** (`factory` service): deploys from the `release` branch. Wait-for-CI can stay off, because the
checker has already required green checks on the commit.

**Staging** (environment `staging`, same project):

- a `factory` service deploying from `main`, waiting for CI;
- its own volume, database and `AIFACTORY_SECRET_KEY`;
- no production data and no production credentials;
- the same database kind as production;
- `AIFACTORY_ALLOWED_EMAILS=@mindsdb.com`;
- no real repositories connected. A sandbox repository for a canary outcome is a later step.

**Release checker** (service `release-check` in the production environment):

- **Image:** the Factory's own, built from `release`.
- **Start command:** `aifactory release-check`.
- **Cron schedule:** `0 */2 * * *`.

| Variable | Purpose |
| --- | --- |
| `AIFACTORY_STAGING_URL` | Staging's public URL |
| `AIFACTORY_PRODUCTION_URL` | Production's public URL |
| `AIFACTORY_RELEASE_GITHUB_TOKEN` | Fine-grained token on `ianu82/ai-factory` only. **Read and write:** Contents, Issues. **Read-only:** Checks, Metadata |
| `AIFACTORY_RELEASE_REPO` | Default `ianu82/ai-factory` |
| `AIFACTORY_RELEASE_BRANCH` / `AIFACTORY_RELEASE_SOURCE_BRANCH` | Defaults `release` / `main` |
| `AIFACTORY_RELEASE_PATHS` | Comma-separated path prefixes that count as Factory code (default `aifactory/`) |
| `AIFACTORY_RELEASE_SOAK_MINUTES` | How long staging must run the candidate (default 30) |
| `AIFACTORY_RELEASE_LIVE_TIMEOUT_MINUTES` / `AIFACTORY_RELEASE_STABLE_MINUTES` | Production go-live wait (40) and stability window (5) |
| `AIFACTORY_RELEASE_PAUSED` | `true` stops releases, for example during a demo |

## Operating it

- **Release now.** Use "Run now" on the cron service. The gates still apply, so for an urgent fix, lower
  `AIFACTORY_RELEASE_SOAK_MINUTES` for that run. `aifactory release-check --dry-run` reports what would happen
  without moving `release`.
- **Pause.** Set `AIFACTORY_RELEASE_PAUSED=true`.
- **Roll back.** This is an operator decision. Move `release` back to the previous release commit; the release
  issue names it. That is the one time `release` moves backwards. Schema changes are additive (new tables and
  columns), so the previous version normally starts on the newer database. Check `/api/health` after the deploy.
- **Merging.** Merge one pull request at a time and confirm `main`'s CI on it before the next. Two individually
  green pull requests can still break `main` together, and staging shows it before production does.
