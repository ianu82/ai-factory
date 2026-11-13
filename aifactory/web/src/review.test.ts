import test from "node:test";
import assert from "node:assert/strict";
import { reviewState, type ReviewEvidence } from "./api.ts";

test("legacy tasks and completed reviews have no active review label", () => {
  assert.equal(reviewState(), null);
  assert.equal(reviewState({ rounds: 2 }), null);
});

test("review states use durable evidence independently of attempts", () => {
  assert.equal(reviewState({ active_round: 1, rounds: 0 }), "Addressing review comments");
  assert.equal(reviewState({ pending: [{ id: "comment:1" }] } as ReviewEvidence), "Waiting to address review comments");
  assert.equal(reviewState({ findings: { one: { status: "disputed" } } } as ReviewEvidence), "Review disputed");
  assert.equal(reviewState({ wait_deadlines: { head: "2999-01-01T00:00:00Z" } }, "head"), "Waiting for reviews");
  assert.equal(reviewState({ wait_deadlines: { old: "2999-01-01T00:00:00Z", head: "2000-01-01T00:00:00Z" } }, "head"), null);
});

test("current delivery evidence uses its own review round and deadline", () => {
  assert.equal(reviewState({ rounds: 1, round: { n: 2, head: "head", findings: [] } }), "Addressing review comments");
  assert.equal(reviewState({ answered: { one: { action: "disputed", note: "Not applicable" } } }), "Review disputed");
  const waiting = { head: "head", deadline: "2999-01-01T00:00:00Z" };
  assert.equal(reviewState(waiting, "head"), "Waiting for reviews");
  assert.equal(reviewState(waiting, "other"), null);
  assert.equal(reviewState({ ...waiting, result: { head: "head", state: "reviewed" } }, "head"), null);
});

// Transpile TSX with the existing compiler; no browser/test dependency required.
const { readFile } = await import("node:fs/promises");
const ts = await import("typescript");
const { renderToStaticMarkup } = await import("react-dom/server");
const { createElement } = await import("react");
const moduleUrl = (source: string) => `data:text/javascript;base64,${Buffer.from(ts.transpile(source, {
  target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext, jsx: ts.JsxEmit.ReactJSX,
})).toString("base64")}`;
const apiUrl = new URL("./api.ts", import.meta.url).href;
const policyUrl = moduleUrl((await readFile(new URL("./reviewPolicy.ts", import.meta.url), "utf8")).replace('"./api"', JSON.stringify(apiUrl)));
const { saveReviewPolicy } = await import(policyUrl);
const wsUrl = moduleUrl('export const useWs = () => ({ ws: { id: "workspace" } });');
const uiUrl = moduleUrl('export const useToast = () => () => {}; export const Empty = () => null; export const Skeleton = () => null;');
async function component(path: string) {
  const source = ts.transpile(await readFile(new URL(path, import.meta.url), "utf8"), {
    target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext, jsx: ts.JsxEmit.ReactJSX,
  })
    .replace(/from "([^"]+)"/g, (_, name: string) => `from ${JSON.stringify(
      name === "../App" ? wsUrl : name === "../ui" ? uiUrl : name === "../api" ? apiUrl :
      name === "../format" ? new URL("./format.ts", import.meta.url).href : import.meta.resolve(name)
    )}`);
  return import(moduleUrl(source));
}

const { ReviewControls } = await component("./components/ReviewControls.tsx");
const { PullRowView } = await component("./pages/PullRequests.tsx");
const { MemoryRouter } = await import("react-router-dom");

test("review controls require an explicit server capability and admin role to save", async () => {
  const policy = { address: "changes_requested", reviewers: "bots", ignore: ["noise[bot]"], wait_seconds: 0, max_rounds: 4 };
  const calls: any[] = [];
  const request = async (path: string, init: unknown) => { calls.push({ path, init }); };
  for (const meta of [{}, { capabilities: {} }, { capabilities: { review_gating: false } }]) {
    const html = renderToStaticMarkup(createElement(ReviewControls, { value: policy, supported: false, admin: true, onSave: () => {} }));
    assert.match(html, /Review-driven revisions are coming/);
    assert.doesNotMatch(html, /<input|<select|<form|Save review policy/);
    await saveReviewPolicy(meta, true, "ws", policy, undefined, request);
    await saveReviewPolicy(meta, true, "ws", policy, "repo", request);
  }
  const meta = { capabilities: { review_gating: true } };
  await saveReviewPolicy(meta, false, "ws", policy, undefined, request);
  assert.equal(calls.length, 0);
  const html = renderToStaticMarkup(createElement(ReviewControls, { value: policy, supported: true, admin: true, onSave: () => {} }));
  assert.match(html, /Save review policy/);
  assert.match(html, /value="changes_requested" selected/);
  assert.match(html, /value="noise\[bot\]"/);
  await saveReviewPolicy(meta, true, "ws", policy, undefined, request);
  assert.deepEqual(calls[0], { path: "/api/workspaces/ws", init: { method: "PATCH", json: { settings: { review: policy } } } });
  const override = { ...policy, address: null, ignore: [] };
  await saveReviewPolicy(meta, true, "ws", override, "repo", request);
  const { address, ...expected } = override;
  assert.deepEqual(calls[1].init.json, { config: { review: expected, review_inherit: ["address"] } });
  const readonly = renderToStaticMarkup(createElement(ReviewControls, { value: policy, supported: true, admin: false, onSave: () => {} }));
  assert.doesNotMatch(readonly, /Save review policy/);
  assert.equal((readonly.match(/disabled=""/g) || []).length, 5);
});

test("PR rows use the persisted current head for waiting state and link to the task and PR", () => {
  const row = { repo: "owner/app", number: 7, title: "Review work", url: "https://github.com/owner/app/pull/7",
    author: { login: "", avatar: "" }, group: "checks", reason: "Waiting for recorded checks", detail: {},
    updated_at: new Date().toISOString(), factory: { task_id: "task", task_key: "T5", outcome_id: "outcome",
      outcome_number: 42, head_sha: "head", review: { rounds: 2, wait_deadlines: { head: "2999-01-01T00:00:00Z" } } } };
  const html = renderToStaticMarkup(createElement(MemoryRouter, null, createElement(PullRowView, { r: row })));
  assert.match(html, /Waiting for reviews/);
  assert.match(html, /review rounds 2/);
  assert.match(html, /href="https:\/\/github.com\/owner\/app\/pull\/7"/);
  assert.match(html, /href="\/w\/workspace\/outcomes\/outcome#task-task"/);
  const pending = { ...row, group: "attention", reason: "Review comments to address (3)" };
  assert.match(renderToStaticMarkup(createElement(MemoryRouter, null, createElement(PullRowView, { r: pending }))), /Review comments to address \(3\)/);
});

test("an uncached outcome task link focuses the card only after its query loads", async () => {
  // Exercise the real components with hook/DOM stand-ins, without adding a DOM dependency.
  const state: any = { loading: true, effects: [], scrolls: [], focus: [] };
  (globalThis as any).__taskLinkTest = state;
  const hooks = moduleUrl(`
    const state = globalThis.__taskLinkTest;
    export const useState = (initial) => [initial, () => {}];
    export const useRef = () => ({ current: {
      scrollIntoView: (options) => state.scrolls.push(options),
      focus: (options) => state.focus.push(options),
    } });
    export const useEffect = (effect) => state.effects.push(effect);
    export const useLocation = () => ({ hash: "#task-target" });
    export const useQueryClient = () => ({});
    export const useQuery = () => ({ isLoading: state.loading, data: state.data });
  `);
  const stubs = moduleUrl(`
    export const useWs = () => ({ ws: { id: "workspace", role: "viewer" } });
    export const useToast = () => () => {};
    export const Skeleton = () => null;
    export const Prose = () => null;
    export const Status = () => null;
    export const GateTrack = () => null;
    export const Mark = () => null;
    export const DiffView = () => null;
    export const DecisionCard = () => null;
    export const TaskCard = () => null;
    export const LinearIssueLink = () => null;
    export const OutcomeAttachments = () => null;
  `);
  const load = async (path: string) => {
    const source = ts.transpile(await readFile(new URL(path, import.meta.url), "utf8"), {
      target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext, jsx: ts.JsxEmit.ReactJSX,
    }).replace(/from "([^"]+)"/g, (_, name: string) => `from ${JSON.stringify(
      ["react", "react-router-dom", "@tanstack/react-query"].includes(name) ? hooks :
      name === "../api" ? apiUrl : name === "../format" ? new URL("./format.ts", import.meta.url).href :
      name === "../outcomePriority" ? new URL("./outcomePriority.ts", import.meta.url).href :
      name.startsWith(".") ? stubs : import.meta.resolve(name)
    )}`);
    return import(moduleUrl(source));
  };
  try {
    const { OutcomeDetail } = await load("./components/OutcomeDetail.tsx");
    const { TaskCard } = await load("./components/TaskCard.tsx");
    const task = (id: string) => ({ id, key: "T5", title: id, status: "pending", evidence: {}, depends_on: [], attempt: 1, cost_usd: 0 });
    const cards = (node: any): any[] => {
      if (!node || typeof node !== "object") return [];
      if (Array.isArray(node)) return node.flatMap(cards);
      return node.props?.task ? [node] : cards(node.props?.children);
    };
    assert.equal(cards(OutcomeDetail({ outcomeId: "outcome" })).length, 0);
    assert.equal(state.effects.length, 0);
    state.loading = false;
    state.data = { outcome: { id: "outcome", status: "running", cost_usd: 0, progress: { total: 2, done: 0 }, acceptance: [] },
      tasks: [task("other"), task("target")], runs: [], decisions: [], plans: [], messages: [], events: [], repositories: [] };
    const loaded = cards(OutcomeDetail({ outcomeId: "outcome" }));
    assert.equal(loaded.length, 2);
    assert.equal(loaded[0].props.focused, false);
    assert.equal(loaded[1].props.focused, true);
    for (const card of loaded) {
      const tree = TaskCard(card.props);
      assert.equal(tree.props.id, `task-${card.props.task.id}`);
      assert.equal(tree.props.tabIndex, -1);
    }
    state.effects.forEach((effect: () => void) => effect());
    assert.deepEqual(state.scrolls, [{ block: "center" }]);
    assert.deepEqual(state.focus, [{ preventScroll: true }]);
  } finally {
    delete (globalThis as any).__taskLinkTest;
  }
});

test("handed-over PR rows show the draft icon only when GitHub reports a draft", () => {
  for (const draft of [true, false]) {
    const row = { repo: "owner/app", number: 1, title: "Finish by hand", url: "https://github.com/owner/app/pull/1",
      author: { login: "", avatar: "" }, draft, group: "handed_over", reason: draft ? "Draft" : "Handed over",
      detail: {}, updated_at: new Date().toISOString(), factory: null };
    const html = renderToStaticMarkup(createElement(MemoryRouter, null, createElement(PullRowView, { r: row })));
    if (draft) {
      assert.match(html, /aria-label="Handed over draft"/);
      assert.match(html, /git-pull-request-draft/);
    } else {
      assert.match(html, /aria-label="Handed over"/);
      assert.doesNotMatch(html, /git-pull-request-draft/);
    }
    assert.doesNotMatch(html, /Ready to merge|Waiting on.*checks/);
  }
});

test("rebased workspace review form preserves main's fields behind the capability gate", async () => {
  const state: any = { supported: false, admin: true, calls: [], submitted: 0 };
  (globalThis as any).__workspaceReviewTest = state;
  // Exercise the actual Settings form without exporting page-internal components or adding a DOM dependency.
  const settings = await readFile(new URL("./pages/Settings.tsx", import.meta.url), "utf8");
  const form = settings.slice(settings.indexOf("type ReviewForm ="), settings.indexOf("function Policies()"));
  const source = `
    import { REVIEW_DEFAULTS } from ${JSON.stringify(new URL("./rules.ts", import.meta.url).href)};
    const state = globalThis.__workspaceReviewTest;
    const useWs = () => ({ ws: { role: state.admin ? "owner" : "viewer", settings: { review: {
      address: "unresolved", reviewers: "all", ignore: [], wait_seconds: 600, max_rounds: 3,
      trust: "anyone", block_on: "all", on_timeout: "ask", request: "@reviewer review",
    } } }, meta: { capabilities: state.supported === undefined ? undefined : { review_gating: state.supported } } });
    const useMemo = (fn) => fn();
    const useEffect = () => {};
    const useState = (initial) => [typeof initial === "object" ? { ...initial, wait_seconds: "42" } : initial, () => {}];
    const useSaveSettings = () => async (patch) => state.calls.push(patch);
    ${form.replace("function ReviewPolicySection()", "export function ReviewPolicySection()")}
  `;
  try {
    const compiled = ts.transpile(source, {
      target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext, jsx: ts.JsxEmit.ReactJSX,
    }).replace('from "react/jsx-runtime"', `from ${JSON.stringify(import.meta.resolve("react/jsx-runtime"))}`);
    const { ReviewPolicySection } = await import(moduleUrl(compiled));
    const findForm = (node: any): any => {
      if (!node || typeof node !== "object") return undefined;
      if (node.type === "form") return node;
      const children = Array.isArray(node.props?.children) ? node.props.children : [node.props?.children];
      return children.map(findForm).find(Boolean);
    };
    for (const supported of [undefined, false]) {
      state.supported = supported;
      const view = ReviewPolicySection();
      assert.equal(findForm(view), undefined);
      assert.match(renderToStaticMarkup(view), /Review-driven revisions are coming/);
      assert.doesNotMatch(renderToStaticMarkup(view), /<input|<select|<form/);
    }
    state.supported = true;
    const view = ReviewPolicySection();
    const html = renderToStaticMarkup(view);
    for (const label of ["Trusted reviewers", "Findings that block a merge", "When no review arrives", "Comment that asks for a review", "Whose reviews count", "Reviewers to ignore"]) {
      assert.ok(html.includes(label), label);
    }
    assert.match(html, /value="anyone" selected/);
    assert.match(html, /value="@reviewer review"/);
    await findForm(view).props.onSubmit({ preventDefault() { state.submitted++; } });
    assert.deepEqual(state.calls, [{ review: { wait_seconds: 42 } }]); // No hidden defaults overwrite main's fields.
    state.admin = false;
    const readonly = ReviewPolicySection();
    assert.doesNotMatch(renderToStaticMarkup(readonly), /Save review<\/button>/);
    await findForm(readonly).props.onSubmit({ preventDefault() {} });
    assert.equal(state.calls.length, 1);
  } finally {
    delete (globalThis as any).__workspaceReviewTest;
  }
});
