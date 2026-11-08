import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Ban, X } from "lucide-react";
import { api, type FactoryEvent, type OutcomeDetail as Detail } from "../api";
import { useWs } from "../App";
import { ago, clock, HARNESS_LABEL, money } from "../format";
import { Prose, Skeleton, Status, useToast } from "../ui";
import { DecisionCard } from "./DecisionCard";
import { TaskCard } from "./TaskCard";

const QUIET = new Set(["agent_thought", "agent_tool"]);

export function OutcomeDetail({ outcomeId, onClose, focusDecisionId }: { outcomeId: string; onClose: () => void; focusDecisionId?: string }) {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const q = useQuery({
    queryKey: ["outcome", ws.id, outcomeId],
    queryFn: () => api<Detail>(`/api/workspaces/${ws.id}/outcomes/${outcomeId}`),
    refetchInterval: (query) => (["completed", "cancelled"].includes(query.state.data?.outcome.status || "") ? false : 15_000),
  });
  const [showAll, setShowAll] = useState(false);
  if (q.isLoading) return <Skeleton rows={8} />;
  if (q.error || !q.data) return <div className="empty"><h3>Could not load this outcome</h3><p>{(q.error as Error)?.message}</p></div>;
  const { outcome, tasks, runs, decisions, plans, messages, events, repositories } = q.data;
  const open = decisions.filter((d) => d.status === "open")
    .sort((a, b) => Number(b.id === focusDecisionId) - Number(a.id === focusDecisionId));  // the one you came for first
  const plan = [...plans].reverse().find((p) => p.status !== "superseded") || plans[plans.length - 1];
  const active = !["completed", "cancelled", "partially_completed"].includes(outcome.status);
  const timeline = (showAll ? events : events.filter((e) => !QUIET.has(e.kind))).slice(-160);

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
    <div>
      <header className="p-head">
        <div className="crumbs">
          <span className="ref">#{outcome.number}</span>
          <span>{repositories.map((r) => r.full_name).join(", ")}</span>
          <span style={{ flex: 1 }} />
          {active && ws.role !== "viewer" && (
            <button className="btn sm ghost danger" onClick={cancel}><Ban size={14} /> Cancel</button>
          )}
          <button className="icon-btn" onClick={onClose} aria-label="Close panel"><X /></button>
        </div>
        <h2>{outcome.title}</h2>
        <div className="facts">
          <Status status={outcome.status} />
          {outcome.progress.total > 0 && <span><b className="num">{outcome.progress.done}/{outcome.progress.total}</b> tasks delivered</span>}
          <span>Cost <b className="num">{money(outcome.cost_usd, outcome.cost_known)}</b></span>
          {outcome.harness && <span>Worker <b>{HARNESS_LABEL[outcome.harness]}</b></span>}
          <span>{outcome.created_by_name ? `${outcome.created_by_name} · ` : outcome.source !== "user" ? `from ${outcome.source} · ` : ""}{ago(outcome.created_at)}</span>
        </div>
      </header>

      {open.length > 0 && (
        <section className="p-sec" aria-label="Needs you">
          {open.map((d) => <DecisionCard key={d.id} d={d} focused={d.id === focusDecisionId} />)}
        </section>
      )}

      {outcome.status === "blocked" && outcome.blocked_reason && open.length === 0 && (
        <section className="p-sec"><div style={{ color: "var(--bad)" }}><Prose text={outcome.blocked_reason} /></div></section>
      )}

      <section className="p-sec">
        <h3>Tasks {tasks.length > 0 && <span className="r faint num">{outcome.progress.done} of {tasks.length} delivered</span>}</h3>
        {tasks.length === 0 ? (
          <div className="muted" style={{ fontSize: 13 }}>
            {outcome.status === "planning" ? "The planner is reading the repositories and drafting tasks…" : "No tasks yet."}
          </div>
        ) : (
          tasks.map((t) => <TaskCard key={t.id} task={t} runs={runs.filter((r) => r.task_id === t.id)} defaultOpen={tasks.length === 1} />)
        )}
      </section>

      <section className="p-sec">
        <h3>Request</h3>
        {outcome.brief ? <Prose text={outcome.brief} /> : <span className="faint">No description</span>}
        {outcome.acceptance.length > 0 && (
          <ul style={{ margin: "8px 0 0", paddingLeft: 18, color: "var(--body)" }}>
            {outcome.acceptance.map((a) => <li key={a}>{a}</li>)}
          </ul>
        )}
      </section>

      {plan && (
        <section className="p-sec">
          <h3>Plan v{plan.version} <span className="r faint">{plan.status === "approved" ? (plan.approved_by ? "approved by a team member" : "approved by policy") : plan.status}</span></h3>
          <Prose text={plan.summary} />
          {(plan.content.risks || []).length > 0 && (
            <ul style={{ margin: "8px 0 0", paddingLeft: 18, color: "var(--muted)", fontSize: 12.5 }}>
              {(plan.content.risks || []).map((r: string) => <li key={r}>{r}</li>)}
            </ul>
          )}
        </section>
      )}

      <Conversation outcomeId={outcome.id} messages={messages} active={active} />

      <section className="p-sec">
        <h3>
          Activity
          <span className="r"><label className="check" style={{ fontSize: 12 }}><input type="checkbox" checked={showAll} onChange={(e) => setShowAll(e.target.checked)} /> agent detail</label></span>
        </h3>
        <ol className="timeline">
          {timeline.map((e) => <TimelineItem key={e.id} e={e} />)}
        </ol>
      </section>
    </div>
  );
}

function TimelineItem({ e }: { e: FactoryEvent }) {
  const cmd = e.kind === "agent_command";
  const url = e.data?.url as string | undefined;
  return (
    <li>
      <span className="time" title={new Date(e.created_at).toLocaleString()}>{clock(e.created_at)}</span>
      <span className={`tick ${e.level}`} />
      <span className={`msg ${cmd ? "cmd" : ""}`}>
        {cmd && "$ "}{e.message}
        {url && <> · <a className="linkish" href={url} target="_blank" rel="noreferrer">open</a></>}
      </span>
    </li>
  );
}

function Conversation({ outcomeId, messages, active }: { outcomeId: string; messages: Detail["messages"]; active: boolean }) {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const send = async () => {
    if (!text.trim()) return;
    setBusy(true);
    try {
      await api(`/api/workspaces/${ws.id}/outcomes/${outcomeId}/messages`, { method: "POST", json: { body: text } });
      setText("");
      qc.invalidateQueries({ queryKey: ["outcome", ws.id, outcomeId] });
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(false);
    }
  };
  if (!active && messages.length === 0) return null;
  return (
    <section className="p-sec">
      <h3>Guidance</h3>
      <div className="convo">
        {messages.map((m) => (
          <div key={m.id} className="bubble">
            <div className="by"><b style={{ color: "var(--ink)", fontWeight: 550 }}>{m.author || "Factory"}</b><span>{ago(m.created_at)}</span>
              {m.author_kind === "user" && <span className="faint">{m.delivered ? "· delivered to the agent" : "· waiting for the next agent turn"}</span>}</div>
            <Prose text={m.body} className="body" />
          </div>
        ))}
      </div>
      {active && ws.role !== "viewer" && (
        <div className="composer">
          <textarea className="textarea" placeholder="Steer the work: constraints, hints, corrections… Delivered to the active agent and recorded."
            value={text} onChange={(e) => setText(e.target.value)} onKeyDown={(e) => { if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) send(); }} />
          <div className="row-end">
            <span className="faint">Optional — routine work doesn't need you. Interventions are recorded.</span>
            <span style={{ flex: 1 }} />
            <button className="btn sm" onClick={send} disabled={busy || !text.trim()}>Send</button>
          </div>
        </div>
      )}
    </section>
  );
}
