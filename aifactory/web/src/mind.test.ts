import { test } from "node:test";
import assert from "node:assert/strict";
import { MindController, canConfirm, formatDuration, mindCost, mindLink, mindPrivateKey, mindRouteContext, mindSuggestions, readMindSSE, stepLabel, validMindBudget, type MindTransport } from "./mind.ts";
import type { MindCard, MindEvent, MindHistory } from "./api.ts";

const card: MindCard = { id: "c1", revision: 1, kind: "create_outcome", workspace_id: "target", title: "Create outcome", fields: { title: "Original", brief: "A brief" }, status: "proposed" };
const history = (): MindHistory => ({ thread: { id: "t1", title: "Delivery", workspace_id: "billing", cost_usd: null, cost_known: false }, messages: [{ id: "a1", role: "assistant", content: "Proposed", cards: [{ ...card }] }], status: { model: "claude-opus-5-5-exact-catalog-id", available: true, budget_state: "ok" } });
const done = { type: "done", message: { id: "a2", role: "assistant", content: "Completed 🧠", cards: [card] }, thread: history().thread, status: history().status } as MindEvent;
function response(text: string, size = 1, cancel?: () => void) {
  const bytes = new TextEncoder().encode(text);
  let position = 0;
  return new Response(new ReadableStream({ pull(controller) { if (position >= bytes.length) controller.close(); else { controller.enqueue(bytes.slice(position, position + size)); position += size; } }, cancel }));
}
for (const ending of ["\n", "\r", "\r\n"]) {
  test(`SSE decodes every byte boundary, UTF-8, comments and multiline JSON (${JSON.stringify(ending)})`, async () => {
    const events: MindEvent[] = [];
    const text = [": heartbeat", "event: text_delta", 'data: {"text":', 'data: "🧠 café"}', "", `data: ${JSON.stringify(done)}`, "", ""].join(ending);
    await readMindSSE(response(text), new AbortController().signal, (e) => events.push(e));
    assert.deepEqual(events, [{ type: "text_delta", text: "🧠 café" }, done]);
  });
}
test("SSE accepts terminal event at EOF, ignores trailing frames and cancels reader", async () => {
  let cancelled = false;
  const received: MindEvent[] = [];
  await readMindSSE(response(`data: ${JSON.stringify(done)}`), new AbortController().signal, (e) => received.push(e));
  await readMindSSE(response(`data: ${JSON.stringify(done)}\n\ndata: invalid\n\n`, 1, () => { cancelled = true; }), new AbortController().signal, () => {});
  assert.equal(received.length, 1);
  assert.equal(cancelled, true);
});
test("SSE reads past an error frame to done, then reports the error", async () => {
  const received: MindEvent[] = [];
  const body = 'event: error\ndata: {"message":"Budget stopped"}\n\n' + `data: ${JSON.stringify(done)}\n\n`;
  await assert.rejects(readMindSSE(response(body), new AbortController().signal, (e) => received.push(e)), /Budget stopped/);
  assert.deepEqual(received.map((e) => e.type), ["error", "done"]);
});
test("SSE reports error, malformed JSON, missing terminal, HTTP failure and missing body", async () => {
  for (const [body, error] of [["", /before completion/], ['data: {"type":"text_delta","text":"partial"}\n\n', /before completion/], ["data: invalid\n\n", /Invalid/], ['event: error\ndata: {"message":"Budget stopped"}\n\n', /Budget stopped/]] as const) {
    await assert.rejects(readMindSSE(response(body), new AbortController().signal, () => {}), error);
  }
  await assert.rejects(readMindSSE(new Response(null, { status: 403 }), new AbortController().signal, () => {}), /403/);
  await assert.rejects(readMindSSE(new Response(null), new AbortController().signal, () => {}), /no body/);
});
test("abort cancels a blocked stream and releases its lock", async () => {
  const abort = new AbortController();
  let cancelled = false;
  const body = new ReadableStream({ cancel() { cancelled = true; } });
  const promise = readMindSSE(new Response(body), abort.signal, () => {});
  abort.abort();
  await assert.rejects(promise, { name: "AbortError" });
  assert.equal(cancelled, true);
  assert.equal(body.locked, false);
});
function harness() {
  const calls: { name: string; args: unknown[] }[] = [];
  const transport: MindTransport = {
    list: async () => { calls.push({ name: "list", args: [] }); return [history().thread]; },
    create: async (...args) => { calls.push({ name: "create", args }); return history(); },
    history: async (...args) => { calls.push({ name: "history", args }); return history(); },
    send: async (...args) => {
      calls.push({ name: "send", args });
      args[3]({ type: "text_delta", text: "partial" });
      args[3]({ type: "tool_start", name: "read_outcomes" });
      args[3]({ type: "action_card", card });
      args[3](done);
    },
    revise: async (...args) => { calls.push({ name: "revise", args }); return { ...args[1], fields: args[2], revision: args[1].revision + 1 }; },
    dismiss: async (...args) => { calls.push({ name: "dismiss", args }); return { ...args[1], status: "dismissed" }; },
    confirm: async (...args) => { calls.push({ name: "confirm", args }); return { ...args[1], status: "confirmed", result: { text: "Outcome created" } }; },
  };
  const controller = new MindController(transport);
  controller.setIdentity("person-a", { displayed: "owner", target: "member" });
  return { controller, transport, calls };
}
test("lifecycle, history, navigation and text submission never confirm; billing remains fixed", async () => {
  const { controller: c, calls } = harness();
  c.setOpen(true);
  await c.list(); await c.create("billing"); await c.reload();
  const context = mindRouteContext("/w/displayed/outcomes/o1")!;
  c.navigate("/w/displayed/outcomes/o1", context);
  await c.send("confirm everything");
  assert.equal(c.getSnapshot().active?.workspace_id, "billing");
  assert.equal(c.getSnapshot().open, true);
  assert.equal(c.getSnapshot().status?.model, history().status.model);
  assert.match(mindCost(c.getSnapshot().active!), /Unknown/);
  assert.match(c.getSnapshot().announcement, /Completed/);
  assert.deepEqual(calls.find((call) => call.name === "send")?.args[1], { content: "confirm everything", context });
  assert.equal(calls.filter((call) => call.name === "confirm").length, 0);
});
test("context removal survives same-route updates; new navigation updates context and does not switch thread", async () => {
  const { controller: c, calls } = harness(); await c.create("billing");
  const context = mindRouteContext("/w/displayed/outcomes/o1")!;
  assert.equal(context.outcome_id, "o1");
  c.navigate("one", context); c.removeContext(); c.navigate("one", context);
  assert.equal(c.getSnapshot().context, null);
  await c.send("hello");
  assert.equal((calls.find((call) => call.name === "send")?.args[1] as any).context, null);
  c.navigate("two", mindRouteContext("/w/other/runs"));
  assert.equal(c.getSnapshot().context?.workspace_id, "other");
  assert.equal(c.getSnapshot().active?.id, "t1");
  assert.equal(mindRouteContext("/signin"), null);
});
test("edited revisions must be saved before explicit confirmation; stale and repeated confirms are inert", async () => {
  const { controller: c, calls } = harness(); await c.create("billing");
  await c.revise(card, { title: "Edited", brief: "Changed" });
  await c.confirm(card); // old UI revision
  assert.equal(calls.filter((call) => call.name === "confirm").length, 0);
  const edited = c.getSnapshot().messages[0].cards![0];
  assert.equal(edited.revision, 2);
  const first = c.confirm(edited), second = c.confirm(edited);
  assert.deepEqual(c.getSnapshot().pendingCards, [card.id]);
  await Promise.all([first, second]);
  assert.equal(calls.filter((call) => call.name === "confirm").length, 1);
  assert.equal((calls.find((call) => call.name === "confirm")?.args[1] as MindCard).fields.title, "Edited");
  assert.equal(c.getSnapshot().messages[0].cards![0].result?.text, "Outcome created");
  await c.confirm(edited);
  assert.equal(calls.filter((call) => call.name === "confirm").length, 1);
});
test("target workspace role gates confirmation, never the displayed owner role", async () => {
  for (const role of ["viewer", undefined] as const) {
    const { controller: c, calls } = harness();
    c.setIdentity("person-a", { displayed: "owner", ...(role ? { target: role } : {}) });
    await c.create("billing"); await c.confirm(card);
    assert.equal(calls.filter((call) => call.name === "confirm").length, 0);
    assert.equal(canConfirm(card, { target: "viewer" }), false);
  }
  for (const role of ["member", "admin", "owner"] as const) assert.equal(canConfirm(card, { target: role }), true);
});
test("dismiss is inert, unavailable/budget/pending accounting all stop text submission", async () => {
  const { controller: c, calls, transport } = harness(); await c.create("billing");
  await c.dismiss(card); await c.confirm(card);
  assert.equal(c.getSnapshot().messages[0].cards![0].status, "dismissed");
  assert.equal(calls.some((call) => call.name === "confirm"), false);
  for (const status of [{ ...history().status, available: false, reason: "missing_key" }, ...["pending_accounting", "budget_exceeded"].map((budget_state) => ({ ...history().status, budget_state }))]) {
    transport.history = async () => ({ ...history(), status } as MindHistory);
    await c.reload(); await c.send("hello");
    assert.equal(c.getSnapshot().status, status);
  }
  assert.equal(calls.some((call) => call.name === "send"), false);
});
test("private identity reset aborts requests and ignores late responses; caches have user keys", async () => {
  const { controller: c, transport } = harness(); await c.create("billing");
  c.setOpen(true); c.setContext(mindRouteContext("/w/billing/runs"));
  let resolve!: (value: MindHistory) => void;
  let signal!: AbortSignal;
  transport.history = async (_id, s) => { signal = s; return new Promise((r) => { resolve = r; }); };
  const pending = c.reload();
  c.setIdentity(null);
  assert.equal(signal.aborted, true);
  resolve(history()); await pending;
  assert.equal(c.getSnapshot().messages.length, 0);
  assert.equal(c.getSnapshot().active, null);
  assert.equal(c.getSnapshot().context, null);
  assert.equal(c.getSnapshot().open, false);
  c.setIdentity("person-b");
  assert.equal(c.getSnapshot().threads.length, 0);
  assert.notDeepEqual(mindPrivateKey("person-a"), mindPrivateKey("person-b"));
});
test("failed confirms release pending state and errors are visible; retry requires another explicit click", async () => {
  const { controller: c, transport, calls } = harness(); await c.create("billing");
  transport.confirm = async () => { throw new Error("Permission changed"); };
  await c.confirm(card);
  assert.match(c.getSnapshot().error, /Permission changed/);
  assert.deepEqual(c.getSnapshot().pendingCards, []);
  await c.send("try again");
  assert.equal(calls.some((call) => call.name === "confirm"), false);
});
test("a thread list that recovers on reload clears its earlier error", async () => {
  const { controller: c, transport } = harness();
  const list = transport.list;
  transport.list = async () => { throw new Error("Could not load threads"); };
  await c.list();
  assert.match(c.getSnapshot().error, /Could not load threads/);
  transport.list = list;
  await c.reload();
  assert.equal(c.getSnapshot().error, "");
  assert.equal(c.getSnapshot().threads.length, 1);
});
test("a thread list that completes later keeps a newer error from another action", async () => {
  const { controller: c, transport } = harness();
  await c.create("billing");
  transport.list = async () => { throw new Error("Could not load threads"); };
  await c.list();
  let release = () => {};
  transport.list = () => new Promise((resolve) => { release = () => resolve([history().thread]); });
  const pending = c.list();
  transport.confirm = async () => { throw new Error("Permission changed"); };
  await c.confirm(card);
  assert.match(c.getSnapshot().error, /Permission changed/);
  release();
  await pending;
  assert.match(c.getSnapshot().error, /Permission changed/);
  assert.equal(c.getSnapshot().threads.length, 1);
});
test("an older thread list that finishes after a newer one failed leaves the newer failure", async () => {
  const { controller: c, transport } = harness();
  let release = () => {};
  transport.list = () => new Promise((resolve) => { release = () => resolve([history().thread]); });
  const older = c.list();
  transport.list = async () => { throw new Error("Could not load threads"); };
  await c.list();
  assert.match(c.getSnapshot().error, /Could not load threads/);
  release();
  await older;
  assert.match(c.getSnapshot().error, /Could not load threads/);
  assert.equal(c.getSnapshot().threads.length, 0);
});
test("budget validation allows zero/nonnegative finite values, rejects empty/negative/nonfinite", () => {
  for (const value of ["0", "10", "0.001", "100.50"]) assert.equal(validMindBudget(value), true);
  for (const value of ["", " ", "-1", "Infinity", "NaN", "1e999", "bad"]) assert.equal(validMindBudget(value), false);
});
test("safe citations and deterministic screen-specific suggestions", () => {
  assert.equal(mindLink("/w/a/outcomes/b"), "internal");
  assert.equal(mindLink("https://github.com/org/repo/pull/1"), "external");
  for (const url of ["javascript:alert(1)", "data:text/html,hi", "//evil.example", "/w/a/outcomes\\evil", "/auth/logout"]) assert.equal(mindLink(url), null);
  for (const page of ["outcomes", "outcomes/1", "runs", "pulls", "signals", "settings", "onboarding", "inbox", "decisions"]) {
    const context = mindRouteContext(`/w/a/${page}`);
    const asks = mindSuggestions(context);
    assert.ok(asks.length >= 2 && asks.length <= 3);
    assert.deepEqual(asks, mindSuggestions(context));
    assert.ok(asks.every((ask) => !ask.startsWith("/")));
  }
});

test("stale history cannot clear a newer navigation's loading state or leak content", async () => {
  const { controller: c, transport } = harness();
  const pending: ((h: MindHistory) => void)[] = [];
  transport.history = async () => new Promise((resolve) => pending.push(resolve));
  const first = c.select("old");
  const second = c.select("new");
  pending[0](history()); await first;
  assert.equal(c.getSnapshot().busy, true);
  assert.equal(c.getSnapshot().active, null);
  pending[1]({ ...history(), thread: { ...history().thread, id: "new" } }); await second;
  assert.equal(c.getSnapshot().active?.id, "new");
  assert.equal(c.getSnapshot().busy, false);
});
test("token/tool progress is visual only; completion announces once; terminal budget errors stop future sends", async () => {
  const { controller: c, transport } = harness(); await c.create("billing");
  const announcements: string[] = [];
  const stop = c.subscribe(() => { const value = c.getSnapshot().announcement; if (value && announcements.at(-1) !== value) announcements.push(value); });
  transport.send = async (_thread, _body, _signal, receive) => {
    receive({ type: "text_delta", text: "Some" });
    receive({ type: "text_delta", text: " more" });
    receive({ type: "tool_start", name: "read_runs" });
    assert.equal(c.getSnapshot().announcement, "");
    assert.equal(c.getSnapshot().draft, "Some more");
    assert.match(c.getSnapshot().progress, /read runs/);
    receive(done);
  };
  await c.send("question");
  assert.equal(announcements.length, 1);
  transport.send = async (_thread, _body, _signal, receive) => {
    receive({ type: "error", message: "Accounting pending", reason: "pending_accounting" });
    throw new Error("Accounting pending");
  };
  await c.send("next");
  assert.equal(c.getSnapshot().status?.budget_state, "pending_accounting");
  assert.equal(c.getSnapshot().busy, false);
  stop();
});

test("identical consecutive completions each get a new live-region identity, never tokens", async () => {
  const { controller: c, transport } = harness(); await c.create("billing");
  transport.send = async (_thread, _body, _signal, receive) => {
    const before = c.getSnapshot().announcementId;
    receive({ type: "text_delta", text: "partial" });
    receive({ type: "tool_start", name: "read_runs" });
    assert.equal(c.getSnapshot().announcementId, before);
    receive(done);
  };
  await c.send("first");
  const first = c.getSnapshot();
  await c.send("second");
  assert.equal(c.getSnapshot().announcement, first.announcement);
  assert.equal(c.getSnapshot().announcementId, first.announcementId + 1);
  c.setIdentity(null);
  assert.equal(c.getSnapshot().announcement, "");
  assert.equal(c.getSnapshot().announcementId, 0);
});

test("Mind's floating controls stay below shared modals", async () => {
  const { readFile } = await import("node:fs/promises");
  const mindStyles = await readFile(new URL("./components/mind/Mind.css", import.meta.url), "utf8");
  const sharedStyles = await readFile(new URL("./styles.css", import.meta.url), "utf8");
  const layer = (styles: string, selector: string) => {
    const rule = styles.split(`${selector} {`)[1]?.split("}")[0];
    const value = rule?.match(/z-index:\s*(\d+)/)?.[1];
    assert.ok(value, `${selector} must declare a stacking layer`);
    return Number(value);
  };
  assert.ok(layer(mindStyles, ".mind-jump") < layer(sharedStyles, ".scrim"), "a modal owning focus must not be covered by Mind");
});

test("a turn records the steps Mind took and how long it worked, and labels tools in plain words", async () => {
  const { controller: c, transport } = harness(); await c.create("billing");
  transport.send = async (_thread, _body, _signal, receive) => {
    receive({ type: "tool_start", name: "open_decisions" });
    assert.deepEqual(c.getSnapshot().steps, [{ name: "open_decisions", done: false }]);
    assert.equal(c.getSnapshot().progress, "Checking open decisions");
    receive({ type: "tool_result", name: "open_decisions", citations: [{ label: "Retry?", url: "/w/a/inbox" }] });
    receive({ type: "tool_start", name: "outcome_detail" });
    receive({ type: "tool_result", name: "outcome_detail" });
    receive(done);
  };
  await c.send("What needs me?");
  const reply = c.getSnapshot().messages.at(-1)!;
  assert.deepEqual(reply.steps, [{ name: "open_decisions", done: true, citations: 1 }, { name: "outcome_detail", done: true, citations: 0 }]);
  assert.equal(typeof reply.durationMs, "number");
  assert.equal(stepLabel("propose_retry_task", false), "Preparing a retry for your approval");
  assert.equal(stepLabel("brand_new_tool", true), "Used brand new tool");
  assert.equal(stepLabel("propose_rename_thing", true), "Prepared rename thing for your approval");
  assert.equal(formatDuration(12_400), "12s");
  assert.equal(formatDuration(125_000), "2m 5s");
  assert.equal(formatDuration(null), null);
});

test("Mind pages are trusted in-app links", () => {
  assert.equal(mindLink("/w/ws1/mind/t1"), "internal");
  assert.equal(mindLink("/w/ws1/mind"), "internal");
});

test("mobile focus trap yields Escape, Tab, focus and restoration to another modal", async () => {
  const { containModalFocus } = await import("./modalFocus.ts");
  const listeners = new Map<string, (event: any) => void>();
  const doc: any = {
    activeElement: null,
    addEventListener: (name: string, fn: (event: any) => void) => listeners.set(name, fn),
    removeEventListener: (name: string) => listeners.delete(name),
  };
  const control = () => ({ tabIndex: 0, isConnected: true, closest: () => null, focus() { doc.activeElement = this; } });
  const trigger = control(), composer = control(), otherDialogControl = control();
  const dialog: any = { ownerDocument: doc, querySelectorAll: () => [composer], contains: (node: any) => node === composer || node === dialog };
  let otherDialog = false, closed = 0;
  trigger.focus();
  let cleanup = containModalFocus(dialog, () => { closed++; }, () => !otherDialog);
  assert.equal(doc.activeElement, composer);
  otherDialog = true;
  otherDialogControl.focus();
  const unexpected = () => assert.fail("Mind must not consume another dialog's keyboard events");
  for (const key of ["Escape", "Tab"]) listeners.get("keydown")!({ key, preventDefault: unexpected, stopPropagation: unexpected });
  listeners.get("focusin")!({ target: otherDialogControl });
  assert.equal(closed, 0);
  assert.equal(doc.activeElement, otherDialogControl);
  cleanup();
  assert.equal(doc.activeElement, otherDialogControl); // no restoration underneath another modal
  assert.equal(listeners.size, 0);
  otherDialog = false;
  trigger.focus();
  cleanup = containModalFocus(dialog, () => { closed++; }, () => !otherDialog);
  listeners.get("keydown")!({ key: "Escape", defaultPrevented: true, preventDefault: unexpected, stopPropagation: unexpected });
  assert.equal(closed, 0);
  listeners.get("keydown")!({ key: "Escape", preventDefault() {}, stopPropagation() {} });
  assert.equal(closed, 1);
  cleanup();
  assert.equal(doc.activeElement, trigger);
});

test("tall message pane follows new own messages, streaming tokens and progress near the bottom", async () => {
  const { MindScroll } = await import("./mindScroll.ts");
  const scroll = new MindScroll();
  const pane = { scrollTop: 1600, scrollHeight: 2000, clientHeight: 400 };
  scroll.onScroll(pane);
  for (const height of [2100, 2140, 2200, 2400]) {
    pane.scrollHeight = height;
    assert.equal(scroll.onOutput(pane), false);
    assert.equal(pane.scrollTop, height - pane.clientHeight);
  }
  pane.scrollTop -= 40;
  scroll.onScroll(pane);
  pane.scrollHeight += 80;
  scroll.onOutput(pane);
  assert.equal(pane.scrollTop, pane.scrollHeight - pane.clientHeight);
});

test("streaming preserves an intentional scroll-up and New reply jumps to latest output", async () => {
  const { MindScroll } = await import("./mindScroll.ts");
  const scroll = new MindScroll();
  const pane = { scrollTop: 500, scrollHeight: 2000, clientHeight: 400 };
  scroll.onScroll(pane);
  for (const height of [2050, 2200, 2400]) {
    pane.scrollHeight = height;
    assert.equal(scroll.onOutput(pane), true);
    assert.equal(pane.scrollTop, 500);
  }
  scroll.jump(pane);
  assert.equal(pane.scrollTop, 2000);
  pane.scrollHeight += 100;
  assert.equal(scroll.onOutput(pane), false);
  assert.equal(pane.scrollTop, 2100);
  pane.scrollTop = 0;
  scroll.onScroll(pane);
  assert.equal(scroll.onOutput(pane), true);
  pane.scrollTop = pane.scrollHeight - pane.clientHeight;
  assert.equal(scroll.onScroll(pane), false, "manually returning to bottom clears the button");
});

test("HTTP fixtures follow T1 thread/message/SSE and T3 edited action contracts", async () => {
  const { createMindHttpTransport } = await import("./mindHttp.ts");
  const calls: { path: string; init: any }[] = [];
  const wireCard = { id: "action", revision: 1, kind: "send_guidance", workspace: { id: "target", name: "Target" }, object: { label: "Outcome", href: "/w/target/outcomes/one" }, fields: { text: "Guide" }, effect: "Send guidance to Outcome in Target.", status: "pending" };
  const wireThread = { id: "thread", title: "Delivery", billing_workspace_id: "billing", model_id: "claude-opus-5-5", cost: { usd: null, known: false } };
  const wireHistory = { thread: wireThread, messages: [{ id: "reply", role: "assistant", text: "Complete", citations: [wireCard.object], action_cards: [wireCard], status: "complete" }] };
  const request: any = async (path: string, init: any = {}) => {
    calls.push({ path, init });
    if (path === "/api/mind/threads") return init.method === "POST" ? wireThread : [wireThread];
    if (path.startsWith("/api/mind/status?")) return { status: "ready", model_id: wireThread.model_id };
    if (path.includes("/actions/")) {
      assert.equal(init.json.expected_revision, 1);
      return { ...wireCard, revision: 2, fields: { text: init.json.text ?? "Guide" }, status: path.endsWith("/confirm") ? "succeeded" : init.json.dismiss ? "dismissed" : "pending", ...(path.endsWith("/confirm") ? { result: { ok: true, message_id: "created" } } : {}) };
    }
    return wireHistory;
  };
  const responseRequest: any = async (path: string, init: any) => {
    calls.push({ path, init });
    const envelope = (type: string, data: unknown) => `data: ${JSON.stringify({ seq: 1, thread_id: "thread", message_id: "reply", type, data })}\r\n\r\n`;
    return response(envelope("message_started", {}) + envelope("tool_started", { name: "read_outcomes" }) + envelope("tool_finished", { name: "read_outcomes" }) + envelope("action_card", { card: wireCard }) + envelope("token", { text: "Complete 🧠" }) + envelope("usage", { thread_cost: wireThread.cost, daily_spend: null, daily_limit: 10 }) + envelope("done", { status: "complete" }));
  };
  const t = createMindHttpTransport(request, responseRequest, readMindSSE);
  const signal = new AbortController().signal;
  assert.equal((await t.list(signal))[0].workspace_id, "billing");
  const h = await t.create("billing", signal);
  assert.deepEqual(calls.find((c) => c.init.method === "POST")?.init.json, { billing_workspace_id: "billing" });
  assert.equal(h.status.model, "claude-opus-5-5");
  assert.equal(h.messages[0].cards![0].workspace_id, "target");
  assert.match(mindCost(h.thread), /Unknown/);
  const events: MindEvent[] = [];
  await t.send("thread", { content: "Question", context: null }, signal, (event) => events.push(event));
  const sent = calls.find((c) => c.path.endsWith("/messages"))!;
  assert.equal(sent.init.json.text, "Question");
  assert.equal(typeof sent.init.json.client_message_id, "string");
  assert.equal(sent.init.json.context, null);
  assert.equal(events.find((e) => e.type === "text_delta")?.text, "Complete 🧠");
  const completed = events.at(-1);
  assert.equal(completed?.type, "done");
  if (completed?.type === "done") assert.equal(completed.status?.budget_state, "pending_accounting");
  assert.equal(calls.some((c) => c.path.endsWith("/confirm")), false);
  const action = h.messages[0].cards![0];
  await t.revise("thread", action, { text: "Edited" }, signal);
  assert.deepEqual(calls.at(-1), { path: "/api/mind/threads/thread/actions/action", init: { method: "PATCH", json: { expected_revision: 1, text: "Edited" }, signal } });
  await t.dismiss("thread", action, signal);
  assert.deepEqual(calls.at(-1)?.init.json, { expected_revision: 1, dismiss: true });
  const confirmed = await t.confirm("thread", action, signal);
  assert.equal(confirmed.status, "confirmed");
  assert.match(confirmed.result!.text, /Completed: Send guidance/);
});

test("a failed turn reloads its saved reply without another paid readiness check, and keeps its budget failure", async () => {
  const { createMindHttpTransport } = await import("./mindHttp.ts");
  const wireThread = { id: "thread", title: "Delivery", billing_workspace_id: "billing", model_id: "opus-5-5", cost: { usd: 0.4, known: true } };
  const paths: string[] = [];
  const request: any = async (path: string) => {
    paths.push(path);
    if (path.startsWith("/api/mind/status?")) return { status: "ready", model_id: "opus-5-5" };
    return { thread: wireThread, messages: [{ id: "reply", role: "assistant", text: "Partial", citations: [], action_cards: [], status: "failed" }] };
  };
  const envelope = (type: string, data: unknown) => `data: ${JSON.stringify({ seq: 1, thread_id: "thread", message_id: "reply", type, data })}\n\n`;
  const stream = envelope("token", { text: "Partial" }) + envelope("error", { code: "budget_exceeded", message: "This reply hit its spending limit." })
    + envelope("usage", { thread_cost: wireThread.cost, daily_spend: 1, daily_limit: 10 }) + envelope("done", { status: "failed" });
  const t = createMindHttpTransport(request, (async () => response(stream)) as any, readMindSSE);
  const events: MindEvent[] = [];
  await assert.rejects(t.send("thread", { content: "Question", context: null }, new AbortController().signal, (e) => events.push(e)), /spending limit/);
  assert.equal(paths.some((p) => p.startsWith("/api/mind/status?")), false);
  const completed = events.at(-1);
  assert.equal(completed?.type, "done");
  if (completed?.type === "done") {
    assert.equal(completed.status, undefined);
    assert.equal(completed.budget, "budget_exceeded");
    assert.equal(completed.message.content, "Partial");
  }
  // The controller keeps the blocked state through the reload instead of re-enabling the composer.
  const { controller: c, transport } = harness();
  await c.select("thread");
  transport.send = async (_thread, _body, _signal, receive) => {
    for (const event of events) receive(event);
    throw new Error("This reply hit its spending limit.");
  };
  await c.send("Question");
  assert.equal(c.state.status?.budget_state, "budget_exceeded");
  assert.equal(c.state.messages.at(-1)?.content, "Partial");
});

test("wire unavailable, accounting and budget codes map to explicit blocked states", async () => {
  const { mindStatus, mindBudget, mindUsage } = await import("./mindHttp.ts");
  const status = mindStatus({ status: "unavailable", model_id: null, reason: "Missing saved key" });
  assert.equal(status.available, false);
  assert.equal(status.reason, "Missing saved key");
  assert.equal(mindBudget("budget_exceeded"), "budget_exceeded");
  assert.equal(mindBudget("accounting_busy"), "pending_accounting");
  assert.equal(mindBudget("accounting_pending"), "pending_accounting");
  for (const [daily_spend, daily_limit, state] of [[0, 0, "budget_exceeded"], [10, 10, "budget_exceeded"], [null, 10, "pending_accounting"], [1, 10, "ok"]] as const) {
    assert.equal(mindUsage(status, { daily_spend, daily_limit, thread_cost: { usd: null, known: false } }).budget_state, state);
  }
});

test("a new conversation opens only when its first message can be sent", async () => {
  const { controller: c, transport } = harness();
  transport.create = async () => ({ ...history(), messages: [], status: { model: null, available: false, reason: "Add a key", budget_state: "ok" } });
  let sent = false, created = 0;
  transport.send = async () => { sent = true; };
  assert.equal(await c.start("billing", "Hello"), null);
  assert.equal(sent, false);
  // With a status check, an unavailable Mind creates no conversation at all.
  transport.status = async () => ({ model: null, available: false, reason: "Add a key", budget_state: "ok" });
  const create = transport.create;
  transport.create = async (...args) => { created++; return create(...args); };
  assert.equal(await c.start("billing", "Hello"), null);
  assert.equal(created, 0);
  transport.status = async () => history().status;
  transport.create = async () => ({ ...history(), messages: [] });
  assert.equal(await c.start("billing", "Hello"), "t1");
});

test("a failed readiness check ends as unavailable with its reason, never checking forever", async () => {
  const { controller: c, transport } = harness();
  transport.status = async () => { throw new Error("offline"); };
  await c.checkStatus("billing");
  const status = c.getSnapshot().status!;
  assert.equal(status.available, false);
  assert.match(status.reason || "", /offline/);
  transport.status = async () => history().status;
  await c.checkStatus("billing");
  assert.equal(c.getSnapshot().status?.available, true);
});
