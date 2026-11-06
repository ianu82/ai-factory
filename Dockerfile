FROM node@sha256:4b150b48b4116e2a22f2c39cc27025fbf4f153e2078fd3b14d159fdb5c7279da AS node
FROM ghcr.io/astral-sh/uv:0.12.5@sha256:e85be844203885286c60ffad8a858d48afb6c5a5c237ca0e67f12e74b8f174b1 AS uv
FROM python@sha256:4f26056fb6ace322265b99c8441e7adae065a56720432e4e722ca921217a68e3

RUN apt-get update && apt-get install -y --no-install-recommends gh && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home factory
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules/npm /opt/npm
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY . .
ARG RAILWAY_GIT_COMMIT_SHA
RUN test -n "$RAILWAY_GIT_COMMIT_SHA" \
    && git init -q && git remote add origin https://github.com/ianu82/ai-factory.git \
    && git fetch --depth=1 origin "$RAILWAY_GIT_COMMIT_SHA" && git reset --mixed FETCH_HEAD \
    && git diff --exit-code HEAD -- . \
    && git config --system --add safe.directory /app \
    && uv sync --locked --no-install-project \
    && .venv/bin/python scripts/install_anton.py .venv/bin/python \
    && node /opt/npm/bin/npm-cli.js ci --prefix src/auto_mindsdb_factory/railway_bridge --ignore-scripts --no-audit --no-fund \
    && node /opt/npm/bin/npm-cli.js ci --prefix src/auto_mindsdb_factory/codex_bridge --ignore-scripts --no-audit --no-fund
ENV PATH="/app/.venv/bin:${PATH}" PYTHONPATH=/app/src HOME=/home/factory PYTHONUNBUFFERED=1
# Root only initializes ownership of the newly mounted volume. start.py drops to
# factory before Git, credential setup, polling or remote execution.
CMD ["python", "deploy/railway/start.py"]
