// Controller-only SDK bridge. No controller secrets enter the VM's environment.
import {readFile} from 'node:fs/promises';
import {Sandbox, SandboxNotFoundError} from 'railway';

const MAX_MESSAGE_BYTES = 100 * 1024 * 1024;
const options = {
  token: process.env.RAILWAY_API_TOKEN,
  authType: 'project-token',
  environmentId: process.env.RAILWAY_ENVIRONMENT_ID,
};

async function checkedExec(sandbox, command, input, timeoutSec, maxOutputBytes = 65536) {
  const handle = sandbox.exec(command, {
    ephemeral: true, stdin: input !== undefined, timeoutSec, maxOutputBytes,
    signal: AbortSignal.timeout((timeoutSec + 30) * 1000),
  });
  if (input !== undefined) {
    await handle.stdin.write(input);
    await handle.stdin.end();
  }
  const result = await handle;
  if (result.exitCode !== 0 || result.timedOut || result.truncated) {
    throw new Error('Remote execution was incomplete');
  }
  return result.stdout;
}

async function dispatch(request) {
  if (request.action === 'create') {
    const sandbox = await Sandbox.create({
      ...options, resources: {cpu: 4, memoryGB: 8}, idleTimeoutMinutes: 5,
      networkIsolation: 'ISOLATED', region: 'us-west2', env: {}, domains: [],
    });
    try {
      const directory = '/opt/ai-factory/auto_mindsdb_factory';
      await sandbox.files.mkdir(directory);
      await sandbox.files.write(`${directory}/__init__.py`, '');
      for (const name of ['docker_runner', 'product_workspace', 'source_transfer', 'railway_remote']) {
        const code = await readFile(new URL(`../${name}.py`, import.meta.url));
        await sandbox.files.write(`${directory}/${name}.py`, code, {mode: 0o644});
      }
      await checkedExec(sandbox,
        'set -eu; id -u factory >/dev/null 2>&1 || useradd -m factory; ' +
        'getent group docker >/dev/null || groupadd --system docker; ' +
        'chgrp docker /var/run/docker.sock && chmod 660 /var/run/docker.sock && ' +
        'usermod -aG docker factory && install -d -o factory -g factory /work && ' +
        'runuser -u factory -- docker info >/dev/null && python3 --version',
        undefined, 90);
      return {id: sandbox.id};
    } catch (error) {
      await sandbox.destroy();
      throw error;
    }
  }
  let sandbox;
  try {
    sandbox = await Sandbox.connect(request.id, options);
  } catch (error) {
    if (request.action === 'destroy' && error instanceof SandboxNotFoundError) return {destroyed: true};
    throw error;
  }
  if (request.action === 'destroy') {
    await sandbox.destroy();
    return {destroyed: true};
  }
  if (request.action === 'heartbeat') {
    await sandbox.heartbeat();
    return {alive: true};
  }
  if (request.action !== 'request') throw new Error('Unknown bridge operation');
  const output = await checkedExec(sandbox,
    'cd /opt/ai-factory && runuser -u factory -- python3 -m auto_mindsdb_factory.railway_remote',
    JSON.stringify(request.request), request.timeout, MAX_MESSAGE_BYTES);
  return JSON.parse(output);
}

try {
  const chunks = [];
  let length = 0;
  for await (const chunk of process.stdin) {
    length += chunk.length;
    if (length > MAX_MESSAGE_BYTES) throw new Error('Bridge input exceeds limit');
    chunks.push(chunk);
  }
  const response = await dispatch(JSON.parse(Buffer.concat(chunks)));
  process.stdout.write(JSON.stringify(response));
} catch (error) {
  process.stdout.write(JSON.stringify({error: error.constructor.name}));
  process.exitCode = 1;
}
