import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { ArrowDown, Check, Plus } from "lucide-react";
import type { MindCitation, MindMessage, MindStatus } from "../api";
import { formatDuration, mindCost, stepLabel } from "../mind";
import { MindScroll } from "../mindScroll";
import { useMind } from "../components/mind/MindProvider";
import { AssistantTurn, LiveTurn, SourceLink, UserTurn, citationKey } from "../components/mind/MindTurns";
import { MindComposer, type ComposerHandle } from "../components/mind/MindComposer";
import "../components/mind/Mind.css";

const STARTERS = [
  "What needs my attention today?",
  "What shipped since yesterday, and what is stuck?",
  "Which pull requests are waiting on me, and why?",
  "Why did the latest failed task fail, and what would fix it?",
];

function blockedReason(status: MindStatus | null): string | null {
  if (!status) return null;
  if (!status.available) return status.reason || "Mind can't answer right now.";
  if (status.budget_state === "budget_exceeded") return "Today's Mind budget is used up. It resets at midnight UTC.";
  if (status.budget_state === "pending_accounting") return "Mind is settling the cost of the last reply. Try again in a moment.";
  return null;
}

function Notice({ status, workspaceId }: { status: MindStatus | null; workspaceId: string }) {
  const reason = blockedReason(status);
  if (!reason) return null;
  const settings = /key|budget|setting/i.test(reason);
  return <div className="mind-notice" role="status">
    <span>{reason}</span>
    {settings && <Link to={`/w/${workspaceId}/settings`} className="btn sm ghost">Open Settings</Link>}
  </div>;
}

function when(value?: string): string {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const days = Math.floor((Date.now() - date.getTime()) / 86_400_000);
  if (days < 1) return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  if (days < 7) return date.toLocaleDateString([], { weekday: "short" });
  return date.toLocaleDateString([], { day: "numeric", month: "short" });
}

/** My Mind: start a conversation, or pick up a recent one. */
export function MindHome({ workspaceId }: { workspaceId: string }) {
  const { controller, state } = useMind();
  const navigate = useNavigate();
  const composer = useRef<ComposerHandle>(null);
  const [starting, setStarting] = useState(false);
  useEffect(() => {
    if (!state.userId) return;
    controller.leave();
    void controller.checkStatus(workspaceId);
    void controller.list();
  }, [controller, state.userId, workspaceId]);
  const reason = blockedReason(state.status);
  const start = async (text: string) => {
    setStarting(true);
    const id = await controller.start(workspaceId, text);
    setStarting(false);
    if (id) navigate(`/w/${workspaceId}/mind/${id}`);
  };
  const recent = state.threads.slice(0, 12);
  return <div className="mind-home">
    <div className="mind-home-column">
      <h1>My Mind</h1>
      <p className="mind-home-lede">Ask what is happening across your workspaces, or ask Mind to step in. Anything it would change waits for your approval.</p>
      <Notice status={state.status} workspaceId={workspaceId} />
      <MindComposer ref={composer} large autoFocus onSend={start} disabled={!!reason || starting} disabledReason={starting ? "Starting…" : reason || undefined}
        placeholder="Ask about an outcome, a pull request or a failure, or ask Mind to fix something…" />
      <div className="mind-starters">
        {STARTERS.map((s) => <button key={s} type="button" className="mind-starter" disabled={!!reason || starting} onClick={() => void start(s)}>{s}</button>)}
      </div>
      {recent.length > 0 && <section className="mind-recent" aria-label="Recent conversations">
        <h2>Recent</h2>
        <ul>{recent.map((t) => <li key={t.id}>
          <Link to={`/w/${t.workspace_id}/mind/${t.id}`}>
            <span className="title">{t.title || "Untitled conversation"}</span>
            <span className="when faint">{when(t.updated_at)}</span>
          </Link>
        </li>)}</ul>
      </section>}
    </div>
  </div>;
}

/** A full-page Mind conversation, with what Mind did and touched alongside it. */
export function MindConversation({ workspaceId, threadId }: { workspaceId: string; threadId: string }) {
  const { controller, state, roles, workspaces } = useMind();
  const navigate = useNavigate();
  const pane = useRef<HTMLDivElement>(null);
  const scroll = useRef(new MindScroll());
  const [unseen, setUnseen] = useState(false);
  useEffect(() => {
    if (state.userId && state.active?.id !== threadId) void controller.select(threadId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [controller, state.userId, threadId]);
  useEffect(() => { scroll.current = new MindScroll(); setUnseen(false); }, [threadId]);
  useLayoutEffect(() => { if (pane.current) setUnseen(scroll.current.onOutput(pane.current)); }, [state.messages, state.draft, state.steps]);
  const loading = state.active?.id !== threadId;
  const reason = blockedReason(state.status);
  const actions = { confirm: (c: Parameters<typeof controller.confirm>[0]) => void controller.confirm(c),
                    dismiss: (c: Parameters<typeof controller.dismiss>[0]) => void controller.dismiss(c),
                    revise: (c: Parameters<typeof controller.revise>[0], f: Record<string, string>) => void controller.revise(c, f) };
  const previousUser = (index: number) => [...state.messages.slice(0, index)].reverse().find((m) => m.role === "user");
  const title = state.active?.title || "New conversation";
  return <div className="mind-page">
    <div className="mind-main">
      <header className="mind-head">
        <nav className="mind-crumbs" aria-label="Breadcrumb"><Link to={`/w/${workspaceId}/mind`}>My Mind</Link><span aria-hidden>/</span><span className="current" title={title}>{loading ? "…" : title}</span></nav>
        <button type="button" className="btn sm ghost" onClick={() => navigate(`/w/${workspaceId}/mind`)}><Plus /> New conversation</button>
      </header>
      <div className="mind-scroll" ref={pane} role="log" aria-label="Conversation with Mind" aria-busy={state.busy}
        onScroll={(e) => setUnseen(scroll.current.onScroll(e.currentTarget))}>
        <div className="mind-column">
          {loading ? <div className="mind-loading muted">Loading conversation…</div> : <>
            {state.messages.map((m, i) => m.role === "user" ? <UserTurn key={m.id} message={m} />
              : <AssistantTurn key={m.id} message={m} roles={roles} pending={state.pendingCards} busy={state.busy} actions={actions}
                  onRetry={m.failed ? () => { const prompt = previousUser(i); if (prompt) void controller.send(prompt.content); } : undefined} />)}
            {state.busy && <LiveTurn draft={state.draft} steps={state.steps} startedAt={state.startedAt} />}
            {state.error && !state.busy && <div className="mind-error" role="alert">{state.error}</div>}
          </>}
        </div>
      </div>
      {unseen && <button type="button" className="mind-jump" onClick={() => { if (pane.current) { scroll.current.jump(pane.current); setUnseen(false); } }}><ArrowDown /> New reply</button>}
      <div className="mind-bottom">
        <div className="mind-column">
          <Notice status={state.status} workspaceId={workspaceId} />
          <MindComposer onSend={(text) => void controller.send(text)} onStop={() => controller.cancel()} running={state.busy}
            disabled={loading || !!reason} disabledReason={reason || undefined} placeholder="Reply to Mind…" autoFocus />
        </div>
      </div>
      <div className="sr" aria-live="polite">{state.announcement}</div>
    </div>
    <MindRail messages={state.messages} busy={state.busy} steps={state.steps} startedAt={state.startedAt}
      workspace={workspaces.find((w) => w.id === state.active?.workspace_id)?.name} cost={state.active ? mindCost(state.active) : null} />
  </div>;
}

/** Alongside the conversation: this turn's progress, everything the conversation touched, and its cost. */
export function MindRail({ messages, busy, steps, startedAt, workspace, cost }: {
  messages: MindMessage[]; busy: boolean; steps: MindMessage["steps"]; startedAt: number | null; workspace?: string; cost: string | null;
}) {
  const last = [...messages].reverse().find((m) => m.role === "assistant" && (m.steps?.length || m.content));
  const shown = busy ? steps || [] : last?.steps || [];
  const linked = useMemo(() => {
    const all: MindCitation[] = messages.flatMap((m) => [...(m.citations || []), ...(m.cards || []).flatMap((c) => c.result?.citations || [])]);
    return all.filter((c, i) => all.findIndex((d) => citationKey(d) === citationKey(c)) === i).slice(-12).reverse();
  }, [messages]);
  return <aside className="mind-rail" aria-label="About this conversation">
    <section className="mind-card">
      <h3>Progress</h3>
      {shown.length ? <ol className="mind-progress">{shown.map((s, i) => <li key={i} className={s.done ? "done" : "running"}>
        {s.done ? <Check aria-hidden /> : <span className="dot active" aria-hidden />}{stepLabel(s.name, s.done)}
      </li>)}</ol> : <p className="faint">{busy ? "Thinking…" : "Steps Mind takes for a reply appear here."}</p>}
      {!busy && last?.durationMs != null && <p className="faint num mind-card-foot">Last reply took {formatDuration(last.durationMs)}</p>}
      {busy && startedAt && <p className="faint mind-card-foot">Working…</p>}
    </section>
    <section className="mind-card">
      <h3>In this conversation</h3>
      {linked.length ? <ul className="mind-linked">{linked.map((c) => <li key={citationKey(c)}><SourceLink citation={c} /></li>)}</ul>
        : <p className="faint">Outcomes, tasks and pull requests Mind looks at appear here.</p>}
    </section>
    <section className="mind-card">
      <h3>Conversation</h3>
      <dl className="mind-facts">
        {workspace && <><dt>Workspace</dt><dd>{workspace}</dd></>}
        {cost && <><dt>Cost so far</dt><dd className="num">{cost}</dd></>}
      </dl>
    </section>
  </aside>;
}
