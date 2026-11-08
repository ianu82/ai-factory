// Run with: node --experimental-strip-types --test src/live.test.ts
import { test } from "node:test";
import assert from "node:assert/strict";
import { BACKOFF_MS, startLive, type LiveDeps } from "./live.ts";

class FakeSource {
  readyState = 0;
  onopen: ((e: unknown) => void) | null = null;
  onerror: ((e: unknown) => void) | null = null;
  onmessage: ((e: { data: string }) => void) | null = null;
  closed = false;
  open() { this.readyState = 1; this.onopen?.({}); }
  dropAndRetry() { this.readyState = 0; this.onerror?.({}); }      // network blip: the browser retries itself
  failForGood() { this.readyState = 2; this.onerror?.({}); }       // e.g. 502 during a deploy: the browser gives up
  send(data: unknown) { this.onmessage?.({ data: JSON.stringify(data) }); }
  close() { this.closed = true; this.readyState = 2; }
}

function harness() {
  const sources: FakeSource[] = [];
  const states: string[] = [];
  const data: unknown[] = [];
  let resyncs = 0;
  let clock = 0;
  let timers: { at: number; fn: () => void; id: number }[] = [];
  let nextId = 1;
  const listeners: Record<string, (() => void)[]> = { online: [], focus: [], visible: [] };
  const deps: LiveDeps = {
    open: () => { const s = new FakeSource(); sources.push(s); return s; },
    setTimeout: (fn, ms) => { const id = nextId++; timers.push({ at: clock + ms, fn, id }); return id; },
    clearTimeout: (id) => { timers = timers.filter((t) => t.id !== id); },
    listen: (type, fn) => { listeners[type].push(fn); return () => { listeners[type] = listeners[type].filter((f) => f !== fn); }; },
    onState: (s) => states.push(s),
    onData: (d) => data.push(d),
    onResync: () => { resyncs += 1; },
  };
  const advance = (ms: number) => {
    clock += ms;
    for (;;) {
      const due = timers.filter((t) => t.at <= clock).sort((a, b) => a.at - b.at)[0];
      if (!due) break;
      timers = timers.filter((t) => t !== due);
      due.fn();
    }
  };
  const fire = (type: string) => listeners[type].forEach((fn) => fn());
  return { deps, sources, states, data, advance, fire, timers: () => timers, resyncs: () => resyncs, listeners };
}

test("a stream the browser gave up on is reconnected with backoff and then resynced", () => {
  const h = harness();
  const stop = startLive("/stream", h.deps);
  h.sources[0].open();
  assert.equal(h.states.at(-1), "live");

  h.sources[0].failForGood();                       // deploy: reconnect answered 502
  assert.equal(h.states.at(-1), "offline");
  assert.equal(h.sources.length, 1);
  h.advance(BACKOFF_MS[0]);
  assert.equal(h.sources.length, 2);                // retried after 1s
  h.sources[1].failForGood();                       // still restarting
  h.advance(BACKOFF_MS[1] - 1);
  assert.equal(h.sources.length, 2);
  h.advance(1);
  assert.equal(h.sources.length, 3);                // then after 2s
  h.sources[2].open();                              // the new server is up
  assert.equal(h.states.at(-1), "live");
  assert.equal(h.resyncs(), 1);                     // catch up on missed events, once
  h.sources[2].send({ type: "changed", entity: "decision" });
  assert.deepEqual(h.data, [{ type: "changed", entity: "decision" }]);
  stop();
});

test("a blip the browser retries by itself is left to the browser, then resynced", () => {
  const h = harness();
  startLive("/stream", h.deps);
  h.sources[0].open();
  assert.equal(h.resyncs(), 0);
  h.sources[0].dropAndRetry();
  assert.equal(h.states.at(-1), "connecting");
  h.fire("focus");
  h.advance(60_000);
  assert.equal(h.sources.length, 1);                // no extra connection: the browser handles it
  h.sources[0].open();                              // browser reconnected by itself
  assert.equal(h.states.at(-1), "live");
  assert.equal(h.resyncs(), 1);                     // events sent during the blip are caught up
});

test("focus or the network coming back reconnects at once", () => {
  const h = harness();
  startLive("/stream", h.deps);
  h.sources[0].open();
  h.sources[0].failForGood();
  h.fire("focus");
  assert.equal(h.sources.length, 2);                // no waiting for the backoff timer
  assert.equal(h.timers().length, 0);
  h.sources[1].failForGood();
  h.fire("online");
  assert.equal(h.sources.length, 3);
  h.fire("focus");                                  // the new attempt is still connecting: leave it be
  assert.equal(h.sources.length, 3);
  h.sources[2].open();
  h.fire("focus");
  assert.equal(h.sources.length, 3);                // healthy: focus is a no-op
});

test("backoff is capped and stopping clears timers and listeners", () => {
  const h = harness();
  const stop = startLive("/stream", h.deps);
  for (let i = 0; i < 8; i++) {
    h.sources.at(-1)!.failForGood();
    h.advance(BACKOFF_MS[Math.min(i, BACKOFF_MS.length - 1)]);
  }
  assert.equal(h.sources.length, 9);
  h.sources.at(-1)!.failForGood();
  stop();
  assert.equal(h.timers().length, 0);
  assert.ok(Object.values(h.listeners).every((l) => l.length === 0));
  assert.ok(h.sources.at(-1)!.closed);
});
