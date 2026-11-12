import { useRef, useState } from "react";
import { useLocation } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Ban, X } from "lucide-react";
import { api, type FactoryEvent, type OutcomeDetail as Detail } from "../api";
import { useWs } from "../App";
import { ago, clock, HARNESS_LABEL, money } from "../format";
import { Prose, Skeleton, Status, useToast } from "../ui";
import { DecisionCard } from "./DecisionCard";
import { TaskCard } from "./TaskCard";
import { LinearIssueLink } from "./LinearIssueLink";

import { OutcomeAttachments } from "./Attachments";
import { OUTCOME_PRIORITIES, priorityLabel } from "../outcomePriority";

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
  const [savingPriority, setSavingPriority] = useState(false);
  const priorityPending = useRef(false);
  const [guided, setGuided] = useState(true);
  const [showAll, setShowAll] = useState(false);
  const { hash } = useLocation();
  const targetDecisionId = hash.startsWith("#decision-") ? hash.slice("#decision-".length) : focusDecisionId;
  if (q.isLoading) return <Skeleton rows={8} />;
  if (q.error || !q.data) return <div className="empty"><h3>Could not load this outcome</h3><p>{(q.error as Error)?.message}</p></div>;
  const { outcome, tasks, runs, decisions, plans, messages, events, repositories } = q.data;
  const open = decisions.filter((d) => d.status === "open" || d.id === targetDecisionId)
    .sort((a, b) => Number(b.id === targetDecisionId) - Number(a.id === targetDecisionId));
  const plan = [...plans].reverse().find((p) => p.status !== "superseded") || plans[plans.length - 1];
  const active = !["completed", "cancelled", "partially_completed"].includes(outcome.status);
  const timeline = (showAll ? events : events.filter((e) => !QUIET.has(e.kind))).slice(-160);

  const changePriority = async (value: string) => {
    if (ws.role === "viewer" || priorityPending.current || Number(value) === outcome.priority) return;
    priorityPending.current = true;
    setSavingPriority(true);
    try {
      await api(`/api/workspaces/${ws.id}/outcomes/${outcomeId}`, { method: "PATCH", json: { priority: Number(value) } });
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["outcome", ws.id, outcomeId] }),
        qc.invalidateQueries({ queryKey: ["outcomes", ws.id] }),
      ]);
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      priorityPending.current = false;
      setSavingPriority(false);
    }
  };

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
          <LinearIssueLink issue={outcome.linear_issue} />
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
          {ws.role !== "viewer" ? (
            <label className="outcome-priority-control">
              Priority
              <select className="select" value={outcome.priority} disabled={savingPriority} aria-busy={savingPriority}
                onChange={(e) => changePriority(e.target.value)}>
                {OUTCOME_PRIORITIES.map((p) => <option key={p.value} value={p.value}>{p.label}</option>)}
              </select>
            </label>
          ) : <span>Priority <b>{priorityLabel(outcome.priority)}</b></span>}
          {outcome.progress.total > 0 && <span><b className="num">{outcome.progress.done}/{outcome.progress.total}</b> tasks delivered</span>}
          <span>Cost <b className="num">{money(outcome.cost_usd, outcome.cost_known)}</b></span>
          {outcome.harness && <span>Worker <b>{HARNESS_LABEL[outcome.harness]}</b></span>}
          <span>{outcome.created_by_name ? `${outcome.created_by_name} · ` : outcome.source !== "user" ? `from ${outcome.source} · ` : ""}{ago(outcome.created_at)}</span>
        </div>
      </header>

      {q.data.guidance && <section className="p-sec" aria-label="Guided outcome">
        <button className="btn sm" onClick={() => setGuided(!guided)}>{guided ? "Dismiss guide" : "Reopen guide"}</button>
        {guided && <>
          <h3>Your guided README outcome · {outcome.status}</h3>
          <p>Only the selected repository’s root README may change. Inspect the CONTRIBUTING section for accurate instructions and preservation of unrelated content.</p>
          <ol>
            <li><a href="#guided-plan">Planning & plan review</a>: Factory reads repository facts and proposes tasks. Check scope; approve only when the plan is README-only.</li>
            <li><a href="#guided-tasks">Building & checks</a>: the worker prepares a candidate. Open the task for its diff and command results; no action unless a decision asks you.</li>
            <li><a href="#guided-tasks">Independent verification</a>: a separate reviewer checks the criteria. Read its verdict and evidence, not just the summary.</li>
            <li><a href="#guided-tasks">Pull request & CI</a>: open the task’s PR link to inspect the exact README diff and GitHub checks.</li>
            <li><a href="#guided-decisions">Merge or approval</a>: review any open decision below. Approval policies still apply; this guide does not grant permission to merge.</li>
            <li><a href="#guided-activity">Smoke</a>: after merge, inspect smoke results in task evidence and activity. Completed means Factory finished its configured gates.</li>
          </ol>
        </>}
      </section>}

      <div id="guided-decisions" />
      {open.length > 0 && (
        <section className="p-sec" aria-label="Needs you">
          {open.map((d) => <DecisionCard key={d.id} d={d} focused={d.id === targetDecisionId} />)}
        </section>
      )}

      {outcome.status === "blocked" && outcome.blocked_reason && open.length === 0 && (
        <section className="p-sec"><div style={{ color: "var(--bad)" }}><Prose text={outcome.blocked_reason} /></div></section>
      )}

      <section className="p-sec">
        <h3 id="guided-tasks">Tasks {tasks.length > 0 && <span className="r faint num">{outcome.progress.done} of {tasks.length} delivered</span>}</h3>
        {tasks.length === 0 ? (
          <div className="muted" style={{ fontSize: 13 }}>
            {outcome.status !== "planning" ? "No tasks yet."
              : outcome.blocked_reason || "The planner is reading the repositories and drafting tasks…"}
          </div>
        ) : (
          tasks.map((t) => <TaskCard key={t.id} task={t} runs={runs.filter((r) => r.task_id === t.id)} defaultOpen={tasks.length === 1} />)
        )}
      </section>

      <section className="p-sec">
        <h3>Request</h3>
        <OutcomeAttachments workspaceId={ws.id} outcomeId={outcome.id} attachments={q.data.attachments || []} />
        {outcome.brief ? <Prose text={outcome.brief} /> : <span className="faint">No description</span>}
        {outcome.acceptance.length > 0 && (
          <ul style={{ margin: "8px 0 0", paddingLeft: 18, color: "var(--body)" }}>
            {outcome.acceptance.map((a) => <li key={a}>{a}</li>)}
          </ul>
        )}
      </section>

      {plan && (
        <section className="p-sec">
          <h3 id="guided-plan">Plan v{plan.version} <span className="r faint">{plan.status === "approved" ? (plan.approved_by ? "approved by a team member" : plan.content.review?.verdict === "approve" ? "approved by the plan reviewer" : "approved by policy") : plan.status}</span></h3>
          <Prose text={plan.summary} />
          {(plan.content.risks || []).length > 0 && (
            <ul style={{ margin: "8px 0 0", paddingLeft: 18, color: "var(--muted)", fontSize: 12.5 }}>
              {(plan.content.risks || []).map((r: string) => <li key={r}>{r}</li>)}
            </ul>
          )}
          {plan.content.review && (
            <div style={{ marginTop: 10, color: "var(--muted)", fontSize: 12.5 }}>
              <b style={{ color: "var(--ink)", fontWeight: 550 }}>Plan review{plan.content.review.model ? ` · ${plan.content.review.model}` : ""}</b>
              {plan.content.review.summary && <Prose text={plan.content.review.summary} />}
              {(plan.content.review.findings || []).length > 0 && (
                <ul style={{ margin: "4px 0 0", paddingLeft: 18 }}>
                  {(plan.content.review.findings || []).map((f, i) => <li key={i}>{f.severity}{f.task ? ` (${f.task})` : ""}: {f.description}</li>)}
                </ul>
              )}
            </div>
          )}
        </section>
      )}

      <Conversation outcomeId={outcome.id} messages={messages} active={active} />

      <section className="p-sec">
        <h3 id="guided-activity">
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
