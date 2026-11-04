import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import test from 'node:test';
import {Sandbox, ExecInterruptedError, ExecNotStartedError} from 'railway';
import {WebSocketServer} from 'ws';
import {clientOptions} from './client.mjs';

async function fixture(t, mode) {
  const server = createServer();
  const sockets = new WebSocketServer({noServer: true});
  const observed = {attempts: 0, commands: 0};
  server.on('upgrade', (request, socket, head) => {
    observed.attempts++;
    if (mode === 'reject' || (mode === 'recover' && observed.attempts === 1)) {
      socket.end('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
      return;
    }
    sockets.handleUpgrade(request, socket, head, connection => {
      connection.on('message', (bytes, binary) => {
        if (binary) return;
        const frame = JSON.parse(bytes.toString());
        if (frame.type === 'exec_hello') {
          connection.send(JSON.stringify({type: 'exec_capabilities', data: {
            version: 2, stdin_chunk_bytes: 16384, stdin_chunks: 64,
          }}));
        }
        if (frame.type === 'init_exec') {
          observed.commands++;
          if (mode === 'interrupt') connection.terminate();
        }
        if (frame.type === 'stdin_close' && mode === 'recover') {
          connection.send(Buffer.concat([Buffer.from([1]), Buffer.from('confirmed result\n')]));
          connection.send(JSON.stringify({type: 'exit', data: {exit_code: 0}}));
        }
      });
    });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    for (const connection of sockets.clients) connection.terminate();
    server.closeAllConnections();
    await new Promise(resolve => sockets.close(resolve));
    await new Promise(resolve => server.close(resolve));
  });
  const sandbox = await Sandbox.connect('test-sandbox', {
    ...clientOptions({RAILWAY_API_TOKEN: 'test-only', RAILWAY_ENVIRONMENT_ID: 'test-environment'}),
    tcpProxyWsEndpoint: `ws://127.0.0.1:${server.address().port}/ws/exec`,
    fetch: async () => new Response(JSON.stringify({data: {
      sandbox: {id: 'test-sandbox', environmentId: 'test-environment', status: 'RUNNING',
        networkIsolation: 'ISOLATED', domains: []},
      generateShellToken: 'test-only-shell-token',
    }}), {status: 200, headers: {'content-type': 'application/json'}}),
  });
  return {sandbox, observed};
}

test('rejected handshakes fail before execution without crashing Node', async t => {
  const {sandbox, observed} = await fixture(t, 'reject');
  await assert.rejects(sandbox.exec('must-not-run', {
    ephemeral: true, signal: AbortSignal.timeout(5000),
  }), ExecNotStartedError);
  assert.deepEqual(observed, {attempts: 2, commands: 0});
});

test('a transient rejected handshake recovers and executes exactly once', async t => {
  const {sandbox, observed} = await fixture(t, 'recover');
  const handle = sandbox.exec('one-command', {
    ephemeral: true, stdin: true, timeoutSec: 2, signal: AbortSignal.timeout(5000),
  });
  await handle.stdin.write('request data');
  await handle.stdin.end();
  const result = await handle;
  assert.equal(result.exitCode, 0);
  assert.equal(result.stdout, 'confirmed result\n');
  assert.equal(result.truncated, false);
  assert.equal(result.timedOut, false);
  assert.deepEqual(observed, {attempts: 2, commands: 1});
});

test('a disconnect after command dispatch is not retried', async t => {
  const {sandbox, observed} = await fixture(t, 'interrupt');
  await assert.rejects(sandbox.exec('outcome-unknown', {
    ephemeral: true, signal: AbortSignal.timeout(5000),
  }), ExecInterruptedError);
  assert.deepEqual(observed, {attempts: 1, commands: 1});
});
