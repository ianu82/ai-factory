# Lightsail Production Runbook

This runbook describes the first production deployment shape for the AI Factory PR-ready runtime.

## Runtime Shape

Production v1 runs on one persistent Lightsail instance with one active worker and one filesystem run store.

- `linear-webhook-server` receives signed Linear Issue webhooks at `/hooks/linear`, verifies and persists accepted trigger events, and returns quickly.
- `factory-worker` drains persisted Linear triggers, advances factory runs, invokes the code worker, runs gates, syncs Linear, and stops at human merge/deploy.
- `AI_FACTORY_AUTONOMY_MODE=pr_ready` is the production default. It disables automatic merge, staging, production monitoring, and feedback movement unless those stages are backed by real external evidence later.

## Current Deployment

The first billable production test box is live on Lightsail and should be treated as the current AI Factory v1 host.

- Instance: `ai-factory-prod-1`
- Region/AZ: `us-west-2a`
- Image: Ubuntu 24.04 LTS
- Static IPv4: `184.33.39.108`
- Public base URL: `https://184-33-39-108.sslip.io`
- Linear webhook URL: `https://184-33-39-108.sslip.io/hooks/linear`
- Linear webhook: `AI Factory Lightsail`, scoped to `software-factory`, `Issue` events only
- Repository checkout: `/srv/ai-factory`
- Run store: `/var/lib/ai-factory`
- Environment file: `/etc/ai-factory/factory.env`
- Service user: `ai-factory`
- SSH access: `ssh -i ~/.ssh/anton_lightsail ubuntu@184.33.39.108`

The worker is initially deployed with `AI_FACTORY_INTAKE_PAUSED=true`. This lets the webhook receive and persist valid events without starting new factory work until an operator explicitly unpauses intake.

```sh
sudo sed -i 's/^AI_FACTORY_INTAKE_PAUSED=.*/AI_FACTORY_INTAKE_PAUSED="false"/' /etc/ai-factory/factory.env
sudo systemctl restart ai-factory-worker.service
```

Pause intake again with:

```sh
sudo sed -i 's/^AI_FACTORY_INTAKE_PAUSED=.*/AI_FACTORY_INTAKE_PAUSED="true"/' /etc/ai-factory/factory.env
sudo systemctl restart ai-factory-worker.service
```

The Linear trigger state for this deployment is the team-specific Stage 1 intake state. Issues should move into that state when the team wants the factory to begin scoping and execution. `New Feature` remains a human requirements-quality state before factory intake.

## Required Host Setup

- Attach a Lightsail static IP.
- Point a DNS name at the static IP.
- Terminate HTTPS with Caddy or nginx and proxy `https://<host>/hooks/linear` to the local webhook server.
- Run services as a non-root user, for example `ai-factory`.
- Store secrets outside the repo, for example `/etc/ai-factory/factory.env`, owned by the service user and mode `0600`.
- Enable Lightsail snapshots or another backup for the run store.
- Keep only ports `22`, `80`, and `443` open publicly; restrict SSH by key.
- Configure log rotation for service logs.

## Environment

Minimum production environment:

```sh
OPENAI_API_KEY=...
LINEAR_API_KEY=...
LINEAR_WEBHOOK_SECRET=...
LINEAR_TARGET_TEAM_ID=...
LINEAR_TARGET_STATE_ID=...
AI_FACTORY_PUBLIC_BASE_URL=https://<your-host>
FACTORY_TRIGGER_BASE_URL=https://<your-host>
AI_FACTORY_AUTONOMY_MODE=pr_ready
AI_FACTORY_CODE_WORKER_PROVIDER=codex_cli
AI_FACTORY_CODE_WORKER_MODEL=gpt-5.4
AI_FACTORY_INTAKE_PAUSED=false
```

Optional command gates:

```sh
AI_FACTORY_GATE_LINT_COMMAND="uv run ruff check ."
AI_FACTORY_GATE_TYPECHECK_COMMAND="uv run mypy src"
AI_FACTORY_GATE_UNIT_COMMAND="uv run python -m pytest -q"
AI_FACTORY_GATE_CONTRACT_COMMAND="uv run python scripts/validate_contracts.py"
AI_FACTORY_REQUIRED_GATE_KINDS=unit,contract
```

## Commands

Validate the host before starting services:

```sh
uv run auto-mindsdb-factory factory-doctor \
  --store-dir /var/lib/ai-factory \
  --repository ianu82/ai-factory \
  --repo-root /srv/ai-factory
```

Verify Linear stage states without creating missing states:

```sh
uv run auto-mindsdb-factory linear-ensure-stage-states \
  --store-dir /var/lib/ai-factory \
  --repo-root /srv/ai-factory \
  --verify-only
```

or using the production-facing alias:

```sh
uv run auto-mindsdb-factory linear-stage-setup \
  --store-dir /var/lib/ai-factory \
  --repo-root /srv/ai-factory \
  --verify-only
```

Run the webhook receiver:

```sh
uv run auto-mindsdb-factory linear-webhook-server \
  --store-dir /var/lib/ai-factory \
  --repo-root /srv/ai-factory \
  --host 127.0.0.1 \
  --port 8080
```

Run the production worker:

```sh
uv run auto-mindsdb-factory factory-worker \
  --store-dir /var/lib/ai-factory \
  --repo-root /srv/ai-factory \
  --repository ianu82/ai-factory
```

Check the deployed services:

```sh
sudo systemctl status caddy
sudo systemctl status ai-factory-webhook.service
sudo systemctl status ai-factory-worker.service
```

Run a deployed host doctor check:

```sh
sudo -u ai-factory bash -lc 'cd /srv/ai-factory && set -a; source /etc/ai-factory/factory.env; set +a; uv run auto-mindsdb-factory factory-doctor --store-dir /var/lib/ai-factory --repo-root /srv/ai-factory --repository ianu82/ai-factory'
```

Pause new intake without disturbing active runs:

```sh
AI_FACTORY_INTAKE_PAUSED=true
```

## Linear Webhook Setup

The current webhook was created in Linear's API settings UI because workspace-admin privileges are required to manage webhooks. The service API key used by the worker can read issues and write comments, but it cannot create or list webhooks.

Use these fields if the webhook must be recreated:

- Label: `AI Factory Lightsail`
- URL: `https://184-33-39-108.sslip.io/hooks/linear`
- Data change events: `Issues`
- Team selection: `software-factory`

After recreating the webhook, copy Linear's generated signing secret into `/etc/ai-factory/factory.env` as `LINEAR_WEBHOOK_SECRET`, then restart `ai-factory-webhook.service`.

```sh
sudo systemctl restart ai-factory-webhook.service
```

Do not commit the signing secret, Linear API key, GitHub token, or OpenAI API key to the repository.

## Systemd Sketch

```ini
[Unit]
Description=AI Factory Linear Webhook
After=network-online.target

[Service]
User=ai-factory
WorkingDirectory=/srv/ai-factory
EnvironmentFile=/etc/ai-factory/factory.env
ExecStart=/usr/bin/env uv run auto-mindsdb-factory linear-webhook-server --store-dir /var/lib/ai-factory --repo-root /srv/ai-factory --host 127.0.0.1 --port 8080
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```ini
[Unit]
Description=AI Factory Worker
After=network-online.target

[Service]
User=ai-factory
WorkingDirectory=/srv/ai-factory
EnvironmentFile=/etc/ai-factory/factory.env
ExecStart=/usr/bin/env uv run auto-mindsdb-factory factory-worker --store-dir /var/lib/ai-factory --repo-root /srv/ai-factory --repository ianu82/ai-factory
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Production Boundaries

- The code worker receives sanitized factory artifacts and a writable worktree. It does not receive Linear, GitHub, AWS, or OpenAI API keys through the environment.
- The orchestrator owns commits, pushes, PR creation, Linear comments, and stage movement.
- Stage 5 only treats command-backed checks as real evidence. LLM quality, latency, cost, staging, and monitoring checks are `deferred` or `not_configured` until real connectors exist.
- Stage 7-9 file-backed artifacts are simulation-only and must not satisfy production gates.
