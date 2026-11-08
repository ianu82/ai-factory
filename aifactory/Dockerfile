# AI Factory v2: web app, API, orchestrator and job sandboxes in one service.
FROM node:22-bookworm-slim AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npx vite build --outDir /web-dist --emptyOutDir

FROM node:22-bookworm-slim AS node

FROM python:3.13-slim-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ripgrep procps ca-certificates curl build-essential \
 && rm -rf /var/lib/apt/lists/*
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
 && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
 && ln -s ../lib/node_modules/corepack/dist/corepack.js /usr/local/bin/corepack \
 && node --version && npm --version
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /usr/local/bin/uv
# Codex harness: the official CLI, pinned.
RUN npm install -g @openai/codex@0.167.0 --no-audit --no-fund && codex --version
WORKDIR /app
# Anton harness: the reviewed SDK commit with hash-locked dependencies.
COPY anton-requirements.txt /tmp/anton-requirements.txt
RUN uv pip install --system --no-deps --require-hashes --only-binary=:all: -r /tmp/anton-requirements.txt \
 && uv pip install --system --no-deps --no-build-isolation \
      "anton-agent @ git+https://github.com/mindsdb/anton@537cc1fb11849e9d152870738af22ca55c1d59d8" \
 && python -c "import anton.core.session"
# Runtime QA: headless Chromium for screenshots, shared read-only with job sandboxes.
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN uv pip install --system "playwright==1.64.1" \
 && python -m playwright install --with-deps chromium \
 && chmod -R a+rX /ms-playwright
# VM bridge: Railway sandbox SDK, used for verification and runtime QA in isolated VMs.
COPY vmbridge/package.json vmbridge/package-lock.json ./vmbridge/
RUN npm ci --prefix vmbridge --no-audit --no-fund --ignore-scripts
COPY vmbridge/sidecar.mjs ./vmbridge/
COPY pyproject.toml ./
COPY src ./src
COPY --from=web /web-dist ./src/aifactory/web
RUN uv pip install --system . && uv pip check --system || true
ENV AIFACTORY_DATA_DIR=/data/aifactory \
    AIFACTORY_ENV=production \
    PYTHONUNBUFFERED=1 \
    PATH="/usr/local/bin:/usr/bin:/bin:/usr/local/sbin:/usr/sbin:/sbin"
EXPOSE 8000
CMD ["aifactory", "serve"]
