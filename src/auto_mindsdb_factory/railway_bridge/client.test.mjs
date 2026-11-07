import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import test from 'node:test';
import {Sandbox, ExecInterruptedError, ExecNotStartedError} from 'railway';
import {WebSocketServer} from 'ws';
import {clientOptions} from './client.mjs';
import {execObservations} from './exec-observations.mjs';

async function fixture(t, mode, observations) {
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
    ...(observations ? {webSocketImpl: observations.WebSocketImpl} : {}),
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

test('metadata observation preserves prestart rejection and bounded HTTP facts', async t => {
  const observations = execObservations();
  const {sandbox, observed} = await fixture(t, 'reject', observations);
  await assert.rejects(sandbox.exec('private-command-must-not-run', {
    ephemeral: true, signal: AbortSignal.timeout(5000),
  }), ExecNotStartedError);
  assert.deepEqual(observed, {attempts: 2, commands: 0});
  const facts = observations.snapshot();
  assert.equal(facts.connections, 2);
  assert.equal(facts.http_rejections, 2);
  assert.equal(facts.last_http_status, 503);
  assert.equal(facts.init_send_attempts, 0);
  assert.equal(facts.observation_failed, false);
  assert.doesNotMatch(JSON.stringify(facts), /private-command|test-only|shell-token|127\.0\.0\.1/);
});

test('metadata observation preserves recovery, input and one command dispatch', async t => {
  const observations = execObservations();
  const {sandbox, observed} = await fixture(t, 'recover', observations);
  const handle = sandbox.exec('private-command', {
    ephemeral: true, stdin: true, timeoutSec: 2, signal: AbortSignal.timeout(5000),
  });
  await handle.stdin.write('private-input');
  await handle.stdin.end();
  assert.equal((await handle).stdout, 'confirmed result\n');
  assert.deepEqual(observed, {attempts: 2, commands: 1});
  const facts = observations.snapshot();
  assert.equal(facts.http_rejections, 1);
  assert.equal(facts.capabilities_frames, 1);
  assert.equal(facts.init_send_attempts, 1);
  assert.equal(facts.init_send_returned, 1);
  assert.doesNotMatch(JSON.stringify(facts), /private-command|private-input|test-only|shell-token|confirmed result/);
  facts.connections = 123;
  assert.equal(observations.snapshot().connections, 2);
});

test('metadata observation never adds retry after command dispatch', async t => {
  const observations = execObservations();
  const {sandbox, observed} = await fixture(t, 'interrupt', observations);
  await assert.rejects(sandbox.exec('private-uncertain-command', {
    ephemeral: true, signal: AbortSignal.timeout(5000),
  }), ExecInterruptedError);
  assert.deepEqual(observed, {attempts: 1, commands: 1});
  assert.equal(observations.snapshot().init_send_attempts, 1);
  assert.equal(observations.snapshot().http_rejections, 0);
});
