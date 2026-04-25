# Lightsail Production Runbook

This runbook describes the first production deployment shape for the AI Factory PR-ready runtime.

## Runtime Shape

Production v1 runs on one persistent Lightsail instance with one active worker and one filesystem run store.

- `linear-webhook-server` receives signed Linear Issue webhooks at `/hooks/linear`, verifies and persists accepted trigger events, and returns quickly.
- `factory-worker` drains persisted Linear triggers, advances factory runs, invokes the code worker, runs gates, syncs Linear, and stops at human merge/deploy.
- `AI_FACTORY_AUTONOMY_MODE=pr_ready` is the production default. It disables automatic merge, staging, production monitoring, and feedback movement unless those stages are backed by real external evidence later.

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

Pause new intake without disturbing active runs:

```sh
AI_FACTORY_INTAKE_PAUSED=true
```

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
