import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import ts from "typescript";

// No DOM/test dependency: compile the real helper modules using the existing compiler.
const moduleUrl = (source: string) => `data:text/javascript;base64,${Buffer.from(ts.transpile(source, { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext })).toString("base64")}`;
const apiUrl = moduleUrl(await readFile(new URL("./api.ts", import.meta.url), "utf8"));
const helper = await import(moduleUrl((await readFile(new URL("./attachments.ts", import.meta.url), "utf8")).replace('"./api"', JSON.stringify(apiUrl))));
const { AttachmentQueue, attachmentLimit, artifactBlob, safeDownloadName, MAX_FILE_BYTES, MAX_TOTAL_BYTES } = helper;
const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
const file = (name = "test.txt") => new File(["original bytes"], name, { type: "text/plain" });
const artifact = (id: string) => ({ id, name: "test.txt", data: { mime_type: "text/plain" }, content: btoa("original bytes") });

test("count, per-file and aggregate budgets include pending files and release on removal", () => {
  assert.match(attachmentLimit(Array(10).fill({ size: 1 }), { size: 1 }), /10 attachments/);
  assert.match(attachmentLimit([], { size: MAX_FILE_BYTES + 1 }), /10 MiB/);
  assert.match(attachmentLimit([{ size: MAX_TOTAL_BYTES - 1 }], { size: 2 }), /25 MiB/);
  assert.equal(attachmentLimit([{ size: MAX_FILE_BYTES }], { size: MAX_FILE_BYTES }), undefined);
  const queue = new AttachmentQueue("ws", () => {}, async () => {}, () => new Promise(() => {}), () => new Promise(() => {}));
  queue.add(Array.from({ length: 11 }, () => file()));
  assert.equal(queue.items.length, 10);
  assert.equal(queue.notices.length, 1);
  const id = queue.items[0].id;
  queue.remove(id);
  queue.add([file()]);
  assert.equal(queue.items.length, 10);
  assert.ok(!queue.items.some((item: any) => item.id === id));
  queue.dispose();
});

test("server errors remain authoritative, retry retains ID, and pending/failed items block submit", async () => {
  let attempts = 0;
  const deleted: string[] = [];
  const queue = new AttachmentQueue("ws", () => {}, async (path: string) => { deleted.push(path); }, async (_path: string, _body: any, _signal: any, progress: any) => {
    progress(50, 100);
    assert.equal(queue.items[0].progress, 50);
    if (++attempts === 1) throw new Error("Staged attachment quota exceeded (413)");
    return artifact("saved");
  }, async () => "YQ==");
  queue.add([file()]);
  const id = queue.items[0].id;
  assert.equal(queue.blocked, true);
  await tick();
  assert.equal(queue.items[0].state, "error");
  assert.match(queue.items[0].error, /quota/);
  assert.equal(queue.blocked, true);
  queue.retry(id);
  await tick();
  assert.equal(queue.items[0].id, id);
  assert.equal(queue.blocked, false);
  assert.deepEqual(queue.ids, ["saved"]);
  queue.finishSubmission(true);
  queue.dispose();
  assert.deepEqual(deleted, []);
});

test("removed/cancelled uploads cannot reappear; late successes are deleted", async () => {
  const deleted: string[] = [];
  let finish: any;
  let signal: AbortSignal;
  const queue = new AttachmentQueue("ws", () => {}, async (path: string) => { deleted.push(path); }, (_path: string, _json: any, s: AbortSignal) => {
    signal = s; return new Promise((resolve) => { finish = resolve; });
  }, async () => "YQ==");
  queue.add([file()]);
  await tick();
  queue.remove(queue.items[0].id);
  assert.equal(signal!.aborted, true);
  finish(artifact("late"));
  await tick();
  assert.deepEqual(queue.ids, []);
  assert.deepEqual(deleted, ["/api/workspaces/ws/attachments/late"]);
});

test("unmount during outcome POST defers deletion until result, never deleting linked files", async () => {
  for (const success of [true, false]) {
    const deleted: string[] = [];
    const queue = new AttachmentQueue("ws", () => {}, async (path: string) => { deleted.push(path); }, async () => artifact("saved"), async () => "YQ==");
    queue.add([file()]); await tick();
    queue.submitting = true;
    queue.dispose();
    assert.equal(deleted.length, 0);
    queue.finishSubmission(success);
    assert.equal(deleted.length, success ? 0 : 1);
  }
});

test("document decoding preserves original bytes and MIME; download names are safe", async () => {
  const blob = artifactBlob(artifact("file"));
  assert.equal(blob.type, "text/plain");
  assert.equal(await blob.text(), "original bytes");
  assert.equal(safeDownloadName("../../unsafe\\file<script>.txt"), "file_script_.txt");
});

test("XHR reports real progress, same-origin credentials, 413 details and cancellation", async () => {
  const { uploadJson, ApiError } = await import(apiUrl);
  const original = globalThis.XMLHttpRequest;
  let xhr: any;
  class FakeXHR {
    upload: any = {}; status = 413; responseText = '{"detail":"Authoritative quota error"}';
    onload: any; onabort: any; withCredentials = false;
    open(method: string, path: string) { assert.equal(method, "POST"); assert.equal(path, "/attachments"); }
    setRequestHeader(name: string, value: string) { assert.equal(name, "Content-Type"); assert.equal(value, "application/json"); }
    send(body: string) { assert.equal(body, '{"content":"YQ=="}'); }
    abort() { this.onabort(); }
  }
  globalThis.XMLHttpRequest = class extends FakeXHR { constructor() { super(); xhr = this; } } as any;
  try {
    const controller = new AbortController();
    const progress: number[] = [];
    const result = uploadJson("/attachments", { content: "YQ==" }, controller.signal, (loaded: number) => progress.push(loaded));
    assert.equal(xhr.withCredentials, true);
    xhr.upload.onprogress({ lengthComputable: true, loaded: 50, total: 100 });
    assert.deepEqual(progress, [50]);
    xhr.onload();
    await assert.rejects(result, (err: any) => err instanceof ApiError && err.status === 413 && err.message === "Authoritative quota error");
    const cancelled = uploadJson("/attachments", { content: "YQ==" }, controller.signal, () => {});
    controller.abort();
    await assert.rejects(cancelled, { name: "AbortError" });
  } finally { globalThis.XMLHttpRequest = original; }
});

test("modal moves focus inside, wraps both ways, recovers escaped focus and restores trigger", async () => {
  const { containModalFocus } = await import(moduleUrl(await readFile(new URL("./modalFocus.ts", import.meta.url), "utf8")));
  const listeners = new Map<string, any>();
  const doc: any = { activeElement: null, addEventListener: (key: string, fn: any) => listeners.set(key, fn), removeEventListener: (key: string) => listeners.delete(key) };
  const control = () => ({ tabIndex: 0, isConnected: true, closest: () => null, focus() { doc.activeElement = this; } });
  const trigger = control(), close = control(), download = control();
  trigger.focus();
  const dialog: any = { ownerDocument: doc, querySelectorAll: () => [close, download], contains: (item: any) => item === close || item === download || item === dialog, focus() { doc.activeElement = this; } };
  let closed = false;
  const cleanup = containModalFocus(dialog, () => { closed = true; });
  assert.equal(doc.activeElement, close); // Regression: opening via Enter left focus on thumbnail.
  let prevented = false;
  const key = (shiftKey: boolean) => { prevented = false; listeners.get("keydown")({ key: "Tab", shiftKey, preventDefault() { prevented = true; } }); };
  key(true); assert.equal(doc.activeElement, download); assert.ok(prevented);
  key(false); assert.equal(doc.activeElement, close); assert.ok(prevented);
  trigger.focus(); key(true); assert.equal(doc.activeElement, download);
  trigger.focus(); listeners.get("focusin")({ target: trigger }); assert.equal(doc.activeElement, close);
  listeners.get("keydown")({ key: "Escape", preventDefault() {}, stopPropagation() {} }); assert.ok(closed);
  cleanup(); assert.equal(doc.activeElement, trigger); assert.equal(listeners.size, 0);
});
