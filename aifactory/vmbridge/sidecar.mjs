// Long-lived bridge between the Factory (Python) and Railway sandbox VMs.
// Requests and responses are JSON lines on stdin/stdout. No Factory credentials enter a VM:
// the Railway token stays in this process and VMs are created with an empty environment.
import readline from 'node:readline';
import {randomUUID} from 'node:crypto';
import {Sandbox} from 'railway';
import WebSocket from 'ws';

const HANDSHAKE_TIMEOUT_MS = 30_000;
// A spawn is acknowledged as started without a session name only after any handshake would have failed.
const SPAWN_START_WAIT_MS = HANDSHAKE_TIMEOUT_MS + 15_000;

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
  async ping() { return {ok: true, configured: Boolean(client.token && client.environmentId)}; },
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
    const started = Promise.resolve(handle.sessionName).then(() => 'started', () => 'unknown');
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
