import { useState } from "react";
import { Link } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowUp, Ban, ChevronDown, ChevronRight, Compass } from "lucide-react";
import { api, type Conversation, type ConversationItem, type ConversationStep, type OutcomeDetail as Detail, type Task } from "../api";
import { useWs } from "../App";
import { clock, DECISION_KIND, money, TASK_STATUS } from "../format";
import { Dot, Prose, Skeleton, Status, useToast } from "../ui";
import { DecisionCard } from "./DecisionCard";
import { OutcomeDetail } from "./OutcomeDetail";

const FINISHED = ["completed", "cancelled", "partially_completed"];
const RESOLUTION: Record<string, string> = {
  answer: "Answered", approve: "Approved", reject: "Changes requested", replan: "Sent back to the planner",
  retry: "Retried", acknowledge: "Acknowledged", abandon: "Abandoned", take_over: "Taken over", cancel: "Cancelled",
  dismiss: "Dismissed", fix: "Fix opened", revert: "Reverted",
};

/** An outcome as a conversation: the Lead narrates, decisions sit in the thread, people message the Lead, and an
 * inspector keeps progress and tasks in view. "Full activity" opens the classic detail. */
export function OutcomeThread({ outcomeId, onBack }: { outcomeId: string; onBack: () => void }) {
  const { ws } = useWs();
  const [full, setFull] = useState(false);
  const detail = useQuery({
    queryKey: ["outcome", ws.id, outcomeId],
    queryFn: () => api<Detail>(`/api/workspaces/${ws.id}/outcomes/${outcomeId}`),
    refetchInterval: (query) => (FINISHED.includes(query.state.data?.outcome.status || "") ? false : 15_000),
  });
  const active = !FINISHED.includes(detail.data?.outcome.status || "");
  const convo = useQuery({
    queryKey: ["conversation", ws.id, outcomeId],
    queryFn: () => api<Conversation>(`/api/workspaces/${ws.id}/outcomes/${outcomeId}/conversation`),
    refetchInterval: active ? 10_000 : false,
  });
  if (detail.isLoading) return <div className="othread"><Skeleton rows={8} /></div>;
  if (detail.error || !detail.data) {
    return <div className="empty"><h3>Could not load this outcome</h3><p>{(detail.error as Error)?.message}</p></div>;
  }
  if (full) {
    return <div className="othread-full"><OutcomeDetail outcomeId={outcomeId} onClose={() => setFull(false)} /></div>;
  }
  const { outcome, tasks, plans } = detail.data;
  const plan = [...plans].reverse().find((p) => p.status !== "superseded") || plans[plans.length - 1];
  return (
    <div className="othread">
      <Header detail={detail.data} active={active} onBack={onBack} />
      <div className="othread-body">
        <section className="othread-main" aria-label="Conversation">
          {convo.isLoading ? <Skeleton rows={6} /> : convo.error || !convo.data ? (
            <div className="empty"><p>{(convo.error as Error)?.message || "Could not load the conversation."}</p></div>
          ) : <Thread items={convo.data.items} />}
          {active && ws.role !== "viewer" && <Composer outcomeId={outcome.id} number={outcome.number} tasks={tasks} />}
        </section>
        <aside className="othread-rail" aria-label="Outcome details">
          <section className="ocard">
            <h2>Progress</h2>
            {outcome.progress.total > 0 && (
              <div className="obar" role="img" aria-label={`${outcome.progress.done} of ${outcome.progress.total} tasks delivered`}>
                <span style={{ width: `${(100 * outcome.progress.done) / outcome.progress.total}%` }} />
              </div>
            )}
            <dl className="ofacts">
              <dt>Delivered</dt><dd className="num">{outcome.progress.done} of {outcome.progress.total || "—"}</dd>
              <dt>Cost so far</dt><dd className="num">{money(outcome.cost_usd, outcome.cost_known)}</dd>
              <dt>Lead</dt><dd>{convo.data?.lead.model ? `${convo.data.lead.model}${convo.data.lead.running ? " · working" : convo.data.lead.last_active ? ` · active ${clock(convo.data.lead.last_active)}` : ""}` : "Joins at its first update"}</dd>
            </dl>
          </section>
          {tasks.length > 0 && (
            <section className="ocard">
              <h2>Tasks</h2>
              <ul className="otasks">{tasks.map((t) => <TaskLine key={t.id} t={t} />)}</ul>
            </section>
          )}
          <section className="ocard olinks">
            {plan && <button type="button" onClick={() => setFull(true)}><b>Plan v{plan.version}</b><span>{plan.status === "approved" ? "approved" : plan.status}</span><ChevronRight size={14} aria-hidden="true" /></button>}
            <button type="button" onClick={() => setFull(true)}><b>Full activity</b><span>every agent step</span><ChevronRight size={14} aria-hidden="true" /></button>
          </section>
        </aside>
      </div>
    </div>
  );
}

function Header({ detail, active, onBack }: { detail: Detail; active: boolean; onBack: () => void }) {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const { outcome, repositories } = detail;
  const cancel = async () => {
    if (!confirm(`Cancel #${outcome.number}? Running sessions stop and nothing further is merged.`)) return;
    try {
      await api(`/api/workspaces/${ws.id}/outcomes/${outcome.id}/cancel`, { method: "POST" });
      qc.invalidateQueries({ queryKey: ["outcome", ws.id] });
      qc.invalidateQueries({ queryKey: ["outcomes", ws.id] });
      toast("Cancelled");
    } catch (err) {
      toast((err as Error).message, true);
    }
  };
  return (
    <header className="othread-head">
      <div className="crumbs">
        <nav aria-label="Breadcrumb"><button type="button" className="linkish" onClick={onBack}>Outcomes</button><span aria-hidden="true"> / </span><span className="ref">#{outcome.number}</span></nav>
        <span className="faint">{repositories.map((r) => r.full_name).join(", ")}</span>
        <span style={{ flex: 1 }} />
        {active && ws.role !== "viewer" && <button className="btn sm ghost danger" onClick={cancel}><Ban size={14} /> Cancel outcome</button>}
      </div>
      <h1>{outcome.title}</h1>
      <div className="facts">
        <Status status={outcome.status} />
        {outcome.progress.total > 0 && <span><b className="num">{outcome.progress.done} of {outcome.progress.total}</b> tasks delivered</span>}
        <span>Cost <b className="num">{money(outcome.cost_usd, outcome.cost_known)}</b></span>
      </div>
    </header>
  );
}

function TaskLine({ t }: { t: Task }) {
  const status = TASK_STATUS[t.status] || { label: t.status, tone: "idle" as const };
  const pr = t.pr_number ? `PR #${t.pr_number}${t.merge_sha ? " merged" : ""}` : t.status === "pending" ? "Waits for earlier tasks" : "";
  return (
    <li>
      <span className="ref">{t.key}</span>
      <span className="otask-title">{t.title}</span>
      <span className="otask-state"><Dot tone={status.tone} /> {status.label}{pr && <span className="faint"> · {pr}</span>}</span>
    </li>
  );
}

function dayOf(iso: string): string {
  const date = new Date(iso);
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);
  if (date.toDateString() === today.toDateString()) return "Today";
  if (date.toDateString() === yesterday.toDateString()) return "Yesterday";
  return date.toLocaleDateString(undefined, { weekday: "short", day: "numeric", month: "short",
    ...(date.getFullYear() !== today.getFullYear() ? { year: "numeric" } : {}) });
}

function Thread({ items }: { items: ConversationItem[] }) {
  let day = "";
  return (
    <ol className="othread-list">
      {items.map((item, i) => {
        const label = dayOf(item.at);
        const separator = label !== day ? <li key={`day-${i}`} className="oday" aria-label={label}><span>{label}</span></li> : null;
        day = label;
        return [separator, <li key={`${item.kind}-${i}`} className={`oitem ${item.kind}`}><Item item={item} /></li>];
      })}
    </ol>
  );
}

function Item({ item }: { item: ConversationItem }) {
  switch (item.kind) {
    case "request":
      return <Request item={item} />;
    case "lead":
      return (
        <article className="olead">
          <span className="olead-mark" aria-hidden="true"><Compass size={16} /></span>
          <div>
            <div className="by"><b>Lead</b><span>· {clock(item.at)}</span>{item.decision_id && <span className="faint">· on a decision below</span>}</div>
            <Prose text={item.text} />
            <Steps steps={item.steps} omitted={item.steps_omitted} />
          </div>
        </article>
      );
    case "lead_guidance":
      return <div className="onote"><div className="by">Lead · {clock(item.at)} · to {item.task_key ? `${item.task_key}'s agents` : "the agents"}{item.delivered ? ", delivered" : ""}</div><Prose text={item.text} /></div>;
    case "person":
      return (
        <div className="operson">
          <div className="by">{item.author}{item.operator ? " (operator)" : ""} · {clock(item.at)}{item.task_key ? ` · about ${item.task_key}` : ""}</div>
          <Prose text={item.text} />
        </div>
      );
    case "factory":
      return <div className="onote"><div className="by">Factory · {clock(item.at)}</div><Prose text={item.text} /></div>;
    case "decision":
      return <DecisionItem item={item} />;
    case "working":
      return (
        <div className="oworking">
          <div className="by"><span className="opulse" aria-hidden="true" />Working · {item.steps[item.steps.length - 1]?.who}: {item.steps[item.steps.length - 1]?.text}</div>
          <Steps steps={item.steps} omitted={item.steps_omitted} />
        </div>
      );
    default:
      return null;
  }
}

function Request({ item }: { item: Extract<ConversationItem, { kind: "request" }> }) {
  const [open, setOpen] = useState(false);
  const long = item.brief.length > 420;
  return (
    <div className="orequest">
      <div className="by">Request · from {item.from}{item.by ? ` · ${item.by}` : ""} · {clock(item.at)}</div>
      <div className={long && !open ? "clamped" : ""}><Prose text={item.brief || item.title} /></div>
      {open && item.acceptance.length > 0 && <ul>{item.acceptance.map((a) => <li key={a}>{a}</li>)}</ul>}
      {(long || item.acceptance.length > 0) && (
        <button type="button" className="linkish" onClick={() => setOpen(!open)}>{open ? "Show less" : "Read the full request"}</button>
      )}
    </div>
  );
}

function DecisionItem({ item }: { item: Extract<ConversationItem, { kind: "decision" }> }) {
  const d = item.decision;
  if (d.status === "open") {
    return <div className="oneeds"><div className="by oneeds-tag">Needs you{d.task_key ? ` · ${d.task_key}` : ""}</div><DecisionCard d={d} /></div>;
  }
  const what = `${DECISION_KIND[d.kind] || d.kind}${d.pr_number ? ` · PR #${d.pr_number}` : d.task_key ? ` · ${d.task_key}` : ""}`;
  return (
    <div className="oresolved">
      <span><b>{what}</b> <span className="faint">asked {clock(d.created_at)}</span></span>
      <span className="faint">{RESOLUTION[d.resolution] || d.resolution}{d.resolved_by_name ? ` by ${d.resolved_by_name}` : ""}{d.resolved_at ? ` · ${clock(d.resolved_at)}` : ""}</span>
      {d.comment && <Prose text={d.comment} className="ocomment" />}
    </div>
  );
}

function Steps({ steps, omitted }: { steps: ConversationStep[]; omitted: number }) {
  const [open, setOpen] = useState(false);
  if (steps.length === 0) return null;
  const count = steps.length + omitted;
  return (
    <div className="osteps">
      <button type="button" className="linkish" aria-expanded={open} onClick={() => setOpen(!open)}>
        {count} step{count === 1 ? "" : "s"} {open ? <ChevronDown size={13} aria-hidden="true" /> : <ChevronRight size={13} aria-hidden="true" />}
      </button>
      {open && (
        <ol>
          {omitted > 0 && <li className="faint">{omitted} earlier step{omitted === 1 ? "" : "s"} in Full activity</li>}
          {steps.map((s, i) => (
            <li key={i} className={s.level}>
              <span className="time num">{clock(s.at)}</span><b>{s.who}</b><span>{s.text}{s.cost_usd != null && <span className="faint"> · {money(s.cost_usd)}</span>}</span>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}

function Composer({ outcomeId, number, tasks }: { outcomeId: string; number: number; tasks: Task[] }) {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const send = async () => {
    const body = text.trim();
    if (!body) return;
    // "@T4 …" talks about one task: the message goes to that task's agents as well as the Lead.
    const mention = body.match(/(?:^|\s)@(T\d+)\b/i);
    const task = mention ? tasks.find((t) => t.key.toLowerCase() === mention[1].toLowerCase()) : undefined;
    setBusy(true);
    try {
      await api(`/api/workspaces/${ws.id}/outcomes/${outcomeId}/messages`, { method: "POST", json: { body, task_id: task?.id ?? null } });
      setText("");
      qc.invalidateQueries({ queryKey: ["conversation", ws.id, outcomeId] });
      qc.invalidateQueries({ queryKey: ["outcome", ws.id, outcomeId] });
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(false);
    }
  };
  return (
    <form className="ocomposer" onSubmit={(e) => { e.preventDefault(); send(); }}>
      <label htmlFor="lead-message" className="sr-only">Message the Lead</label>
      <textarea id="lead-message" rows={2} value={text} placeholder={`Message the Lead about #${number}… mention @T1 to talk about one task`}
        onChange={(e) => setText(e.target.value)} onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } }} />
      <div className="row">
        <span className="faint">The Lead acts within this workspace's autonomy and asks you for the rest. Your agents see your message too.</span>
        <span style={{ flex: 1 }} />
        <button type="submit" className="osend" aria-label="Send" disabled={busy || !text.trim()}><ArrowUp size={15} /></button>
      </div>
    </form>
  );
}

export function OutcomeLink({ id, number }: { id: string; number: number }) {
  const { ws } = useWs();
  return <Link className="ref" to={`/w/${ws.id}/outcomes/${id}`}>#{number}</Link>;
}
