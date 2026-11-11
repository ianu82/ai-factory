/**
 * A static, signed-out view of Mind for visual review on any deployment (served as /mind-fixture.html).
 * Every byte of data is invented here and the transport never touches the network: it plays back a scripted
 * conversation, so the real page components can be looked at without an account or a model.
 */
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter, NavLink, Route, Routes, useParams } from "react-router-dom";
import { Activity, GitPullRequest, Inbox as InboxIcon, Library, Plus, Radio, Target } from "lucide-react";
import type { Me, MindCard, MindEvent, MindHistory, MindMessage, MindStatus, MindThread } from "./api";
import type { MindTransport } from "./mind";
import { MindProvider } from "./components/mind/MindProvider";
import { MindNav } from "./components/mind/MindNav";
import { MindConversation, MindHome } from "./pages/Mind";
import "./styles.css";

const params = new URLSearchParams(window.location.search);
const view = params.get("view") || "thread";
const ws = "demo";
const ready: MindStatus = { model: "opus-5-5", available: true, budget_state: "ok" };
const status: MindStatus = view === "unavailable"
  ? { model: null, available: false, reason: "Add a saved MindsHub Inference key in workspace Settings.", budget_state: "ok" }
  : ready;

const threads: MindThread[] = [
  { id: "t1", title: "Why is the billing export stuck?", workspace_id: ws, cost_usd: 0.1842, cost_known: true, updated_at: new Date().toISOString() },
  { id: "t2", title: "What needs my attention today?", workspace_id: ws, cost_usd: 0.0611, cost_known: true, updated_at: new Date(Date.now() - 86_400_000).toISOString() },
  { id: "t3", title: "Which pull requests are waiting on me?", workspace_id: ws, cost_usd: 0.0377, cost_known: true, updated_at: new Date(Date.now() - 3 * 86_400_000).toISOString() },
];

const retry: MindCard = {
  id: "a1", revision: 1, kind: "retry_task", workspace_id: ws, title: "Retry T2 on #12 Billing export",
  effect: "Resolve the open unblock decision as retry and requeue this task. Work may run again and incur execution costs.",
  fields: { text: "Rebase onto main first: #481 changed the export schema. Then rerun the Postgres tests." }, status: "proposed",
};
const guidance: MindCard = {
  id: "a2", revision: 2, kind: "send_guidance", workspace_id: ws, title: "Guidance on #14 Usage alerts",
  fields: { text: "Keep the alert threshold per workspace, not per user." }, status: "confirmed",
  result: { text: "Completed: Post the exact text above as your guidance on this outcome.", citations: [{ label: "#14 Usage alerts", url: `/w/${ws}/outcomes/o14` }] },
};

const answer = `**#12 Billing export** is stuck on **T2**, and it isn't a code problem.

T2's last three attempts failed the same Postgres test (\`test_export_rows_match_invoice\`) after **#481** merged and changed the export schema underneath it. The branch was cut before #481, so every retry rebuilds on the old schema.

What would unblock it:

1. Rebase T2 onto main so it builds on the new schema.
2. Rerun the Postgres suite. The SQLite run already passes.
3. If it still fails, the assertion compares column order, which #481 changed on purpose. The test should compare by name.

| Task | State | Attempts | Last result |
| --- | --- | --- | --- |
| T1 Export job | merged | 1 | verified |
| T2 Invoice rows | blocked | 3 of 3 | Postgres test failed |
| T3 CSV download | waiting on T2 | 0 | — |

I've prepared the retry below with that instruction attached. Nothing runs until you approve it.`;

const conversation = (): MindMessage[] => [
  { id: "u1", role: "user", content: "Why is the billing export stuck?" },
  { id: "m1", role: "assistant", content: answer, durationMs: 14_000,
    steps: [{ name: "open_decisions", done: true, citations: 2 }, { name: "outcome_detail", done: true, citations: 3 }, { name: "runs", done: true, citations: 3 }, { name: "propose_retry_task", done: true }],
    citations: [{ label: "#12 Billing export", url: `/w/${ws}/outcomes/o12` }, { label: "T2 Invoice rows", url: `/w/${ws}/outcomes/o12?task=t2` }, { label: "PR #481", url: "https://github.com/example/factory/pull/481" }],
    cards: [{ ...retry }] },
  { id: "u2", role: "user", content: "And the usage alerts outcome, did my guidance land?" },
  { id: "m2", role: "assistant", content: "Yes. Your guidance is on **#14 Usage alerts**, and T1 picked it up in its current attempt: the threshold is now stored per workspace.", durationMs: 6_000,
    steps: [{ name: "outcome_detail", done: true, citations: 1 }], citations: [{ label: "#14 Usage alerts", url: `/w/${ws}/outcomes/o14` }], cards: [{ ...guidance }] },
];

function history(id: string): MindHistory {
  const thread = threads.find((t) => t.id === id) || { ...threads[0], id, title: "New conversation" };
  return { thread, messages: id === "t1" ? conversation() : [], status };
}

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
const transport: MindTransport = {
  list: async () => threads,
  status: async () => status,
  create: async () => { const t = { ...threads[0], id: `n${Date.now()}`, title: "New conversation" }; threads.unshift(t); return { thread: t, messages: [], status }; },
  history: async (id) => history(id),
  send: async (thread, body, signal, receive) => {
    const t = threads.find((x) => x.id === thread);
    if (t && t.title === "New conversation") t.title = body.content.slice(0, 60);
    const steps = ["overview", "open_decisions", "list_outcomes"];
    for (const name of steps) {
      if (signal.aborted) return;
      receive({ type: "tool_start", name }); await wait(700);
      receive({ type: "tool_result", name, citations: [{ label: "#12 Billing export", url: `/w/${ws}/outcomes/o12` }] }); await wait(250);
    }
    const text = "Three things need you today:\n\n- **#12 Billing export** is blocked on a retry decision (T2).\n- **PR #482** has passed CI and is waiting for your merge approval.\n- **#16 Signup flow** finished planning and needs the plan approved.\n\nWant me to prepare the retry for #12 first?";
    for (const word of text.split(/(?<= )/)) { if (signal.aborted) return; receive({ type: "text_delta", text: word }); await wait(28); }
    const message: MindMessage = { id: `m${Date.now()}`, role: "assistant", content: text, citations: [{ label: "#12 Billing export", url: `/w/${ws}/outcomes/o12` }, { label: "PR #482", url: "https://github.com/example/factory/pull/482" }] };
    receive({ type: "done", message, thread: t || threads[0], status } as MindEvent);
  },
  revise: async (_t, card, fields) => ({ ...card, revision: card.revision + 1, fields }),
  dismiss: async (_t, card) => ({ ...card, revision: card.revision + 1, status: "dismissed" }),
  confirm: async (_t, card) => { await wait(600); return { ...card, status: "confirmed", result: { text: "Done. T2 is queued again with your instruction.", citations: [{ label: "T2 Invoice rows", url: `/w/${ws}/outcomes/o12?task=t2` }] } }; },
};

const me = { user: { id: "u-fixture", email: null, name: "Fixture" }, organizations: [],
  workspaces: [{ id: ws, name: "AI Factory", slug: "ai-factory", description: "", org_id: "org", org_name: "Example Org", role: "owner", paused: false, settings: {} }] } as unknown as Me;

function Thread() { const { threadId = "t1" } = useParams(); return <MindConversation workspaceId={ws} threadId={threadId} />; }

function Shell() {
  const base = `/w/${ws}`;
  return <div className="shell">
    <aside className="sidebar" aria-label="Main">
      <div className="brand"><img src="/minds-icon.svg" alt="" /><span className="word">FACTORY</span></div>
      <button className="ws-switch"><span className="ws-mark">A</span><span style={{ minWidth: 0 }}><div className="name">AI Factory</div><div className="org">Example Org</div></span></button>
      <button className="capture"><Plus size={15} /> New outcome <kbd>C</kbd></button>
      <MindNav workspaceId={ws} />
      <NavLink to={`${base}/inbox`} className="nav-item"><InboxIcon /> Inbox</NavLink>
      <NavLink to={`${base}/outcomes`} className="nav-item"><Target /> Outcomes</NavLink>
      <NavLink to={`${base}/pulls`} className="nav-item"><GitPullRequest /> Pull requests</NavLink>
      <NavLink to={`${base}/runs`} className="nav-item"><Activity /> Runs</NavLink>
      <NavLink to={`${base}/signals`} className="nav-item"><Radio /> Signals</NavLink>
      <NavLink to={`${base}/decisions`} className="nav-item"><Library /> Decisions</NavLink>
    </aside>
    <div className="main">
      <Routes>
        <Route path="/w/:wsId/mind" element={<MindHome workspaceId={ws} />} />
        <Route path="/w/:wsId/mind/:threadId" element={<Thread />} />
        <Route path="*" element={<div className="empty"><h3>Not part of this fixture</h3><p>Only the Mind pages are wired here.</p></div>} />
      </Routes>
    </div>
  </div>;
}

const start = view === "home" || view === "unavailable" ? `/w/${ws}/mind` : `/w/${ws}/mind/t1`;
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <MemoryRouter initialEntries={[start]}>
      <MindProvider me={me} transport={transport}><Shell /></MindProvider>
    </MemoryRouter>
  </StrictMode>,
);
