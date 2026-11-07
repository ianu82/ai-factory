import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import test from 'node:test';
import {Sandbox, ExecInterruptedError, ExecNotStartedError, ExecControlUnsupportedError} from 'railway';
import {WebSocketServer} from 'ws';
import {clientOptions} from './client.mjs';
import {execObservations, execFailureObservations} from './exec-observations.mjs';
import {checkedExec, execBudget} from './exec.mjs';
import {bridgeProgress} from './progress.mjs';

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
      // Release a failed deadline regression promptly, without an SDK retry.
      const fallback = mode === 'stalled' ? setTimeout(() => connection.close(1008), 600) : undefined;
      connection.on('close', () => clearTimeout(fallback));
      connection.on('message', (bytes, binary) => {
        if (binary) return;
        const frame = JSON.parse(bytes.toString());
        if (frame.type === 'exec_hello') {
          if (mode === 'stalled') return;
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

test('partial progress precedes a real stalled SDK handshake timeout without exposing payloads', async t => {
  const lines = [];
  let report = () => {};
  const observations = execObservations(() => report());
  report = bridgeProgress('a'.repeat(32), observations.snapshot, line => lines.push(line));
  const {sandbox} = await fixture(t, 'stalled', observations);
  report('exec');
  await assert.rejects(checkedExec(sandbox, 'private-command', 'private-input', 2, 65536, 80),
    error => error.name === 'TimeoutError');
  const frames = lines.map(line => JSON.parse(line.slice('FACTORY_BRIDGE_PROGRESS '.length)));
  assert.ok(frames.some(f => f.exec_observations.hello_send_attempts === 1));
  assert.ok(frames.every(f => f.stage === 'exec' && f.exec_observations.init_send_attempts === 0));
  for (let i = 0; i < 100; i++) report('failed');
  assert.equal(lines.length, 64);
  assert.ok(!lines.join('').includes('private-'));
  bridgeProgress(undefined, observations.snapshot, () => assert.fail('legacy output changed'))();
  bridgeProgress('b'.repeat(32), observations.snapshot, () => { throw new Error('private'); })();
});

test('rejected handshakes fail before execution without crashing Node', async t => {
  const {sandbox, observed} = await fixture(t, 'reject');
  await assert.rejects(sandbox.exec('must-not-run', {
    ephemeral: true, signal: AbortSignal.timeout(5000),
  }), ExecNotStartedError);
  assert.deepEqual(observed, {attempts: 2, commands: 0});
});

for (const input of [undefined, 'private-input']) {
  test(`controller budget stops a stalled handshake with stdin=${input !== undefined}`, async t => {
    const {sandbox, observed} = await fixture(t, 'stalled');
    const started = performance.now();
    await assert.rejects(checkedExec(sandbox, 'must-not-run', input, 2, 65536, 80),
      error => error.name === 'TimeoutError');
    assert.ok(performance.now() - started < 500);
    assert.deepEqual(observed, {attempts: 1, commands: 0});
  });
}

test('invalid controller budgets fail before SDK admission', async () => {
  let calls = 0;
  const sandbox = {exec() { calls++; }};
  for (const budget of [0, -1, 1.5, NaN, Infinity, '80', null, true]) {
    assert.throws(() => execBudget(budget), TypeError);
    await assert.rejects(checkedExec(sandbox, 'must-not-run', undefined, 2, 65536, budget), TypeError);
  }
  assert.equal(calls, 0);
});

test('control-plane lookup consumes the budget and cannot admit a late exec', async t => {
  let now = 100;
  t.mock.method(performance, 'now', () => now);
  const remaining = execBudget(80);
  now = 125;
  assert.equal(remaining(), 55);
  now = 181;
  await assert.rejects(checkedExec({exec() { assert.fail('expired exec dispatched'); }},
    'must-not-run', undefined, 2, 65536, remaining()), TypeError);
  assert.equal(execBudget(undefined)(), undefined);
});

test('optional budget preserves defaults and cannot extend the existing signal limit', async t => {
  const signalMs = [];
  const timeout = AbortSignal.timeout;
  t.mock.method(AbortSignal, 'timeout', ms => { signalMs.push(ms); return timeout(ms); });
  const options = [];
  const sandbox = {exec(command, supplied) {
    options.push(supplied);
    return {exitCode: 0, timedOut: false, truncated: false, stdout: 'result'};
  }};
  for (const budget of [undefined, 1000000, 80]) {
    assert.equal(await checkedExec(sandbox, 'command', undefined, 2, 17, budget), 'result');
  }
  assert.deepEqual(signalMs, [32000, 32000, 80]);
  assert.ok(options.every(o => o.ephemeral && !o.stdin && o.timeoutSec === 2 && o.maxOutputBytes === 17));
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
  }), error => {
    assert.equal(error.constructor, ExecNotStartedError);
    assert.equal(execFailureObservations(error, observations).exec_observations.last_http_status, 503);
    return true;
  });
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

test('failure metadata is absent for unrelated, derived or unavailable observations', () => {
  const observations = execObservations();
  class Derived extends ExecNotStartedError {}
  for (const error of [new Error('private-text'), new Derived('private-text'), null]) {
    assert.deepEqual(execFailureObservations(error, observations), {});
  }
  const error = new ExecNotStartedError('private-text');
  assert.deepEqual(execFailureObservations(error, {snapshot() { throw new Error('private-text'); }}), {});
  for (const ErrorType of [ExecNotStartedError, ExecInterruptedError, ExecControlUnsupportedError]) {
    const facts = execFailureObservations(new ErrorType('private-text'), observations);
    assert.deepEqual(facts.exec_observations, observations.snapshot());
    assert.doesNotMatch(JSON.stringify(facts), /private-text/);
  }
});
