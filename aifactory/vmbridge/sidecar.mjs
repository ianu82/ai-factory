// Long-lived bridge between the Factory (Python) and Railway sandbox VMs.
// Requests and responses are JSON lines on stdin/stdout. No Factory credentials enter a VM:
// the Railway token stays in this process and VMs are created with an empty environment.
import readline from 'node:readline';
import {randomUUID} from 'node:crypto';
import {Sandbox} from 'railway';
import WebSocket from 'ws';

class ExecWebSocket extends WebSocket {
  constructor(address, protocols, options) {
    if (protocols && typeof protocols === 'object' && !Array.isArray(protocols)) { options = protocols; protocols = undefined; }
    super(address, protocols, {...options, handshakeTimeout: 30000});
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
    const sandbox = a.template ? await Sandbox.create(template(a.template), options) : await Sandbox.create(options);
    boxes.set(sandbox.id, sandbox);
    return {id: sandbox.id, region: sandbox.region ?? null};
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
