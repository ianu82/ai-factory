// Long-lived bridge between the Factory (Python) and Railway sandbox VMs.
// Requests and responses are JSON lines on stdin/stdout. No Factory credentials enter a VM:
// the Railway token stays in this process and VMs are created with an empty environment.
import readline from 'node:readline';
import {randomUUID} from 'node:crypto';
import {DEFAULT_RAILWAY_GRAPHQL_ENDPOINT, ExecNotStartedError, Sandbox} from 'railway';
import WebSocket from 'ws';
import {previewCapabilities, createPreview, PrivateTunnel} from './preview.mjs';

// Separate ownership from verifier boxes: a verifier/checkpoint id can never be
// promoted to a preview. Currently empty because this provider cannot deny egress.
const previews = new Map();
const tunnels = new Map();
function preview(id) {
  const sandbox = previews.get(id);
  if (!sandbox) throw new Error('Preview VM missing or isolation unproven; rebuild required');
  return sandbox;
}

const HANDSHAKE_TIMEOUT_MS = 30_000;
// The railway SDK retries a connection that failed before the command started once (connectWithRetry), so a
// start can take two handshakes. A spawn is acknowledged without a session name only after both would have
// failed, plus a margin for minting the second token.
const SDK_CONNECT_ATTEMPTS = 2;
const SPAWN_START_WAIT_MS = SDK_CONNECT_ATTEMPTS * HANDSHAKE_TIMEOUT_MS + 15_000;

class ExecWebSocket extends WebSocket {
  constructor(address, protocols, options) {
    if (protocols && typeof protocols === 'object' && !Array.isArray(protocols)) { options = protocols; protocols = undefined; }
    super(address, protocols, {...options, handshakeTimeout: HANDSHAKE_TIMEOUT_MS});
  }
}

const client = {
  token: process.env.RAILWAY_API_TOKEN,
  authType: process.env.RAILWAY_TOKEN_TYPE || 'project-token',
  environmentId: process.env.RAILWAY_ENVIRONMENT_ID,
  webSocketImpl: ExecWebSocket,
};
const boxes = new Map();

// Direct GraphQL for the orphan sweep: the SDK's list includes destroyed sandboxes and hides its cursor, and an
// orphan may not answer a connect, so it is destroyed by id. Same endpoint and auth headers as the SDK.
async function graphql(query, variables) {
  const auth = client.authType === 'project-token' ? {'project-access-token': client.token}
    : {Authorization: `Bearer ${client.token}`};
  const res = await fetch(process.env.RAILWAY_GRAPHQL_ENDPOINT || DEFAULT_RAILWAY_GRAPHQL_ENDPOINT, {
    method: 'POST', headers: {'content-type': 'application/json', ...auth}, body: JSON.stringify({query, variables})});
  const body = await res.json().catch(() => ({}));
  if (!res.ok || body.errors) throw new Error(`Railway GraphQL ${res.status}: ${JSON.stringify(body.errors || '').slice(0, 300)}`);
  return body.data;
}
const procs = new Map();
const send = (message) => process.stdout.write(JSON.stringify(message) + '\n');

async function box(id) {
  if (!boxes.has(id)) boxes.set(id, await Sandbox.connect(id, client));
  return boxes.get(id);
}

function template(spec) {
  let t = Sandbox.template();
  if (spec.packages?.length) t = t.withPackages(...spec.packages);
  if (spec.env) t = t.withEnv(spec.env);
  for (const step of spec.run || []) t = t.run(step);
  if (spec.workdir) t = t.workdir(spec.workdir);
  return t;
}

const ops = {
  async previewCapabilities() { return previewCapabilities(); },
  async previewCreate() { return createPreview(); },
  async previewReconnect(a) {
    const sandbox = preview(a.id);
    await sandbox.refresh();
    if (sandbox.status !== 'RUNNING') throw new Error('Preview VM no longer running; rebuild required');
    return {id: sandbox.id};
  },
  async previewOpen(a) {
    if (tunnels.size >= 32) throw new Error('Preview transport capacity reached');
    const sandbox = preview(a.id);
    const token = randomUUID();
    tunnels.set(token, {id: a.id, tunnel: new PrivateTunnel(sandbox, a.port)});
    return {tunnel: token};
  },
  async previewWrite(a) {
    const entry = tunnels.get(a.tunnel);
    if (!entry || entry.id !== a.id) throw new Error('Preview tunnel missing; never replay a request');
    await entry.tunnel.write(a.base64);
    return {};
  },
  async previewRead(a) {
    const entry = tunnels.get(a.tunnel);
    if (!entry || entry.id !== a.id) throw new Error('Preview tunnel missing; never replay a request');
    return entry.tunnel.read();
  },
  async previewClose(a) {
    const entry = tunnels.get(a.tunnel);
    if (entry && entry.id === a.id) {
      tunnels.delete(a.tunnel);
      await entry.tunnel.close();
    }
    return {};
  },
  async previewDestroy(a) {
    const sandbox = preview(a.id);
    for (const [token, entry] of tunnels) {
      if (entry.id === a.id) {
        tunnels.delete(token);
        await entry.tunnel.close().catch(() => {});
      }
    }
    await sandbox.destroy();
    previews.delete(a.id);
    boxes.delete(a.id);
    return {destroyed: true};
  },
  async ping() { return {ok: true, configured: Boolean(client.token && client.environmentId)}; },
  async sandboxes() {
    const items = [];
    let after = null;
    for (let page = 0; page < 20; page++) {
      const data = await graphql('query($e: String!, $after: String) { sandboxes(environmentId: $e, active: true, '
        + 'first: 100, after: $after) { edges { node { id status createdAt } } pageInfo { hasNextPage endCursor } } }',
        {e: client.environmentId, after});
      items.push(...data.sandboxes.edges.map((edge) => edge.node));
      if (!data.sandboxes.pageInfo.hasNextPage) break;
      after = data.sandboxes.pageInfo.endCursor;
    }
    return {items};
  },
  async forget(a) {
    await graphql('mutation($e: String!, $id: String!) { sandboxDestroy(environmentId: $e, id: $id) { id status } }',
      {e: client.environmentId, id: a.id});
    boxes.delete(a.id);
    return {destroyed: true};
  },
  async create(a) {
    const options = {...client, resources: a.resources, idleTimeoutMinutes: a.idleTimeoutMinutes ?? 20,
      networkIsolation: a.networkIsolation || 'ISOLATED', env: {}, domains: []};
    const sandbox = a.checkpoint ? await Sandbox.create(a.checkpoint, options)
      : a.template ? await Sandbox.create(template(a.template), options) : await Sandbox.create(options);
    boxes.set(sandbox.id, sandbox);
    return {id: sandbox.id, region: sandbox.region ?? null};
  },
  async checkpoint(a) {
    const sandbox = await box(a.id);
    const info = await sandbox.checkpoint(a.name);
    return {name: info?.name ?? a.name};
  },
  async checkpoints() {
    const items = await Sandbox.checkpoints(client);
    return {items: items.map((c) => ({id: c.id, key: c.key, createdAt: c.createdAt}))};
  },
  async deleteCheckpoint(a) {
    await Sandbox.deleteCheckpoint(a.id, client);
    return {deleted: true};
  },
  async exec(a) {
    const sandbox = await box(a.id);
    const result = await sandbox.exec(a.command, {timeoutSec: a.timeoutSec, cwd: a.cwd, env: a.env,
      maxOutputBytes: a.maxOutputBytes ?? 2_000_000});
    return {exitCode: result.exitCode, stdout: result.stdout, stderr: result.stderr,
      truncated: result.truncated, timedOut: result.timedOut};
  },
  async spawn(a) {
    const sandbox = await box(a.id);
    const proc = randomUUID();
    const handle = sandbox.exec(a.command, {cwd: a.cwd, env: a.env, captureOutput: false});
    // Reply only once the command has started (its durable session is named), has already ended, or the
    // handshake timeout plus a margin has passed. A failure to start then rejects this call, so the caller can
    // retry an exec that never ran (ExecNotStartedError) instead of seeing a process that exits before it began.
    const ended = Promise.resolve(handle).then(() => 'ended');
    // A start failure rejects the session name with ExecNotStartedError: surface it so the caller retries.
    // Any other rejection (e.g. no durable session assigned) says nothing about the start, so keep waiting for
    // the handle or the timer rather than risk a second copy of a command that did start.
    const started = Promise.resolve(handle.sessionName).then(() => 'started', (error) => {
      if (error instanceof ExecNotStartedError || error?.name === 'ExecNotStartedError') throw error;
      return new Promise(() => {});
    });
    let timer;
    const waited = new Promise((resolve) => { timer = setTimeout(() => resolve('unknown'), SPAWN_START_WAIT_MS); });
    try {
      await Promise.race([started, ended, waited]);
    } finally {
      clearTimeout(timer);
    }
    procs.set(proc, handle);
    Promise.resolve(handle).then(
      (r) => send({event: 'exit', proc, exitCode: r?.exitCode ?? null}),
      (e) => send({event: 'exit', proc, error: String(e?.message || e)}),
    ).finally(() => procs.delete(proc));
    return {proc};
  },
  async kill(a) {
    const handle = procs.get(a.proc);
    if (handle) { try { await handle.kill('KILL'); } catch { /* already gone */ } }
    return {};
  },
  async write(a) {
    const sandbox = await box(a.id);
    await sandbox.files.write(a.path, Buffer.from(a.base64, 'base64'), a.mode ? {mode: a.mode} : undefined);
    return {};
  },
  async read(a) {
    const sandbox = await box(a.id);
    const options = {format: 'bytes'};
    if (a.length) { options.length = a.length; options.fromEnd = Boolean(a.fromEnd); }
    const bytes = await sandbox.files.read(a.path, options);
    return {base64: Buffer.from(bytes).toString('base64')};
  },
  async destroy(a) {
    try { const sandbox = await box(a.id); await sandbox.destroy(); } finally { boxes.delete(a.id); }
    return {destroyed: true};
  },
};

setInterval(() => { for (const sandbox of boxes.values()) sandbox.heartbeat().catch(() => {}); }, 60_000).unref();

readline.createInterface({input: process.stdin}).on('line', async (line) => {
  let request;
  try { request = JSON.parse(line); } catch { return; }
  try {
    const op = ops[request.op];
    if (!op) throw new Error(`unknown operation ${request.op}`);
    send({id: request.id, ok: true, result: await op(request.args || {})});
  } catch (error) {
    send({id: request.id, ok: false, error: `${error?.name || 'Error'}: ${String(error?.message || error).slice(0, 2000)}`});
  }
});
process.stdin.on('end', async () => {
  await Promise.allSettled([...boxes.values()].map((s) => s.destroy()));
  process.exit(0);
});
