import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, type Decision } from "../api";
import { useWs } from "../App";
import { ago, DECISION_KIND } from "../format";
import { Empty, Skeleton, useToast } from "../ui";

export function DecisionsPage() {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const q = useQuery({ queryKey: ["decisions", ws.id], queryFn: () => api<Decision[]>(`/api/workspaces/${ws.id}/decisions`) });
  const metrics = useQuery({ queryKey: ["stats", ws.id], queryFn: () => api<{ decision_judges: Record<string, { decided: number; sent_back: number; held: number; overruled: number; overrule_rate: number }> }>(`/api/workspaces/${ws.id}/stats`) });
  const corrected = () => {
    qc.invalidateQueries({ queryKey: ["decisions", ws.id] });
    qc.invalidateQueries({ queryKey: ["rulings", ws.id] });
    qc.invalidateQueries({ queryKey: ["stats", ws.id] });
  };
  const toggle = async (d: Decision) => {
    try {
      if (d.kind === "ruling" && d.reusable) await api(`/api/workspaces/${ws.id}/rulings/${d.id}`, { method: "DELETE" });
      else await api(`/api/workspaces/${ws.id}/decisions/${d.id}`, { method: "PATCH", json: { reusable: !d.reusable, scope: d.scope === "repository" ? "repository" : "workspace" } });
      qc.invalidateQueries({ queryKey: ["decisions", ws.id] });
    } catch (err) {
      toast((err as Error).message, true);
    }
  };
  // Rulings come from their own uncapped list, so an old rule is always shown (and can be retired).
  const rulings = useQuery({ queryKey: ["rulings", ws.id], queryFn: () => api<{ rulings: { id: string; title: string; text: string; scope: string; by: string; at: string | null; kind: string }[] }>(`/api/workspaces/${ws.id}/rulings`) });
  const listed = new Set((q.data || []).map((d) => d.id));
  const older = (rulings.data?.rulings || []).filter((r) => r.kind === "ruling" && !listed.has(r.id));
  const retire = async (id: string) => {
    try {
      await api(`/api/workspaces/${ws.id}/rulings/${id}`, { method: "DELETE" });
      corrected();
    } catch (err) { toast((err as Error).message, true); }
  };
  const precedents = (q.data || []).filter((d) => d.reusable && d.status !== "superseded");
  const history = (q.data || []).filter((d) => !d.reusable || d.status === "superseded");
  const admin = ws.role === "owner" || ws.role === "admin";
  return (
    <>
      <div className="page-head">
        <h1>Decisions</h1>
        <span className="muted hide-sm" style={{ fontSize: 12.5 }}>Who decided what, and which decisions the Factory follows next time</span>
      </div>
      <div className="page-body">
        {metrics.data?.decision_judges && <section style={{ padding: "16px 22px" }} aria-label="Decision judge metrics">
          <h2>Decision judges</h2>
          {[7, 30].map(days => {
            const m = metrics.data.decision_judges[`days_${days}`];
            return m && <p key={days}>Last {days} days: {m.decided} decided · {m.sent_back} sent back · {m.held} held · {m.overruled} overruled · {(m.overrule_rate * 100).toFixed(1)}% overrule rate</p>;
          })}
          <p className="muted">UTC cohorts by review completion. Overrule rate: corrected automatic verdicts from the cohort / decided and sent back verdicts. Suggest counts as held.</p>
        </section>}

        {ws.role !== "viewer" && <AddRule onAdded={corrected} />}
        {q.isLoading ? <Skeleton /> : !q.data?.length ? (
          <Empty title="No decisions yet">Approvals, answers and unblocks are recorded here with who made them. Tick "Remember for future work" to turn one into a standing precedent.</Empty>
        ) : (
          <>
            <div className="list-section">Team rules and precedents the Factory follows · {precedents.length}</div>
            {precedents.length === 0 && <div className="muted" style={{ padding: "12px 22px", fontSize: 12.5 }}>None yet.</div>}
            {older.length > 0 && <ul className="rows">{older.map((r) => (
              <li key={r.id} className="row">
                <span className="dot good" />
                <div style={{ minWidth: 0 }}><div className="title">{r.title}</div><div className="sub"><span>Team rule · {r.scope}</span></div></div>
                <div className="meta">
                  <span>{r.by || "—"} · {ago(r.at)}</span>
                  {admin && <button className="btn sm ghost" onClick={() => retire(r.id)}>Retire</button>}
                </div>
              </li>))}</ul>}
            <ul className="rows">{precedents.map((d) => <Row key={d.id} d={d} admin={admin} toggle={toggle} corrected={corrected} open={() => d.outcome_id && navigate(`/w/${ws.id}/outcomes/${d.outcome_id}`)} />)}</ul>
            <div className="list-section">History · {history.length}</div>
            {history.length === 0 && <div className="muted" style={{ padding: "12px 22px", fontSize: 12.5 }}>Other resolved decisions will appear here, with who made them.</div>}
            <ul className="rows">{history.map((d) => <Row key={d.id} d={d} admin={admin} toggle={toggle} corrected={corrected} open={() => d.outcome_id && navigate(`/w/${ws.id}/outcomes/${d.outcome_id}`)} />)}</ul>
          </>
        )}
      </div>
    </>
  );
}

function Row({ d, admin, toggle, open, corrected }: { d: Decision; admin: boolean; toggle: (d: Decision) => void; open: () => void; corrected: () => void }) {
  const { ws, me } = useWs();
  const toast = useToast();
  const [editing, setEditing] = useState(false);
  const [answer, setAnswer] = useState("");
  const [reason, setReason] = useState("");
  const [resolution, setResolution] = useState(d.resolution || d.options[0]?.value || "answer");
  const [saving, setSaving] = useState(false);
  const canOverrule = ws.role !== "viewer" && d.status === "resolved" && d.judge?.mode === "decide" && !!d.judge.finished_at && ["decide", "send_back"].includes(d.judge.effective_verdict || "") && !d.judge.correction_decision_id;
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    try {
      await api(`/api/workspaces/${ws.id}/decisions/${d.id}/overrule`, { method: "POST", json: { resolution, answer, reason } });
      setEditing(false);
      corrected();
    } catch (err) { toast((err as Error).message, true); }
    finally { setSaving(false); }
  };
  return (
    <li className="row" onClick={open}>
      <span className={`dot ${d.reusable ? "good" : "idle"}`} />
      <div style={{ minWidth: 0 }}>
        <div className="title">{d.title}</div>
        <div className="sub">
          <span>{DECISION_KIND[d.kind] || d.kind}: <b style={{ color: "var(--ink)", fontWeight: 550 }}>{d.resolution}</b></span>
          {d.comment && <span style={{ overflow: "hidden", textOverflow: "ellipsis" }}>“{d.comment}”</span>}
        </div>
        {d.judge && <div style={{ marginTop: 8 }}>
          <b>Decision judge · {d.judge.effective_verdict || d.judge.state}</b>
          <p>{d.judge.summary}</p>
          {d.judge.answer && <p>{d.status === "open" ? "Proposal: " : "Answer: "}{d.judge.answer}</p>}
          {d.judge.citations.map((c, i) => <p key={i}>Decision {c.decision_id}: “{c.passage}”</p>)}
          {!!d.judge.closest_decisions.length && <p>Closest decisions: {d.judge.closest_decisions.join(", ")}</p>}
        </div>}
        {d.correction && <p>Superseded by {d.correction.resolved_by_name} · {ago(d.correction.resolved_at)}: {d.correction.answer}<br />Reason: {d.correction.attribution.reason}</p>}
        {d.supersedes && <p>Correction of decision {d.supersedes}</p>}
        {editing && <form onSubmit={submit} onClick={e => e.stopPropagation()} style={{ display: "grid", gap: 8, marginTop: 8 }}>
          <label>Resolution <select value={resolution} onChange={e => setResolution(e.target.value)}>{d.options.filter(o => !["approve", "take_over"].includes(o.value)).map(o => <option key={o.value} value={o.value}>{o.label || o.value}</option>)}</select></label>
          <label>Corrected answer <textarea required maxLength={8000} value={answer} onChange={e => setAnswer(e.target.value)} /></label>
          <label>Reason <textarea required maxLength={8000} value={reason} onChange={e => setReason(e.target.value)} /></label>
          <button className="btn sm" disabled={saving || !answer.trim() || !reason.trim()}>Save correction</button>
          <button type="button" className="btn sm ghost" onClick={() => setEditing(false)}>Cancel</button>
        </form>}
      </div>
      <div className="meta">
        <span>{d.resolved_by_name || "—"} · {ago(d.resolved_at)}</span>
        {canOverrule && <button className="btn sm ghost" onClick={e => { e.stopPropagation(); setEditing(true); }}>Overrule</button>}
        {(admin || (d.kind === "ruling" && d.reusable && d.resolved_by === me.user.id)) && d.comment && d.status !== "superseded" && (
          <button className="btn sm ghost" onClick={(e) => { e.stopPropagation(); toggle(d); }}>{d.reusable ? "Retire" : "Make precedent"}</button>
        )}
      </div>
    </li>
  );
}

/** State a rule every agent in the workspace follows: planners, builders, verifiers and Leads are all given it. */
function AddRule({ onAdded }: { onAdded: () => void }) {
  const { ws } = useWs();
  const toast = useToast();
  const [text, setText] = useState("");
  const [saving, setSaving] = useState(false);
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    try {
      await api(`/api/workspaces/${ws.id}/rulings`, { method: "POST", json: { text, scope: "workspace" } });
      setText("");
      onAdded();
    } catch (err) { toast((err as Error).message, true); }
    finally { setSaving(false); }
  };
  return (
    <form className="add-rule" onSubmit={submit} style={{ display: "flex", gap: 8, padding: "12px 22px", alignItems: "flex-start" }}>
      <label htmlFor="new-rule" className="sr-only">New team rule</label>
      <textarea id="new-rule" rows={1} maxLength={4000} value={text} onChange={e => setText(e.target.value)} style={{ flex: 1 }}
        placeholder="Add a team rule every agent follows, e.g. “Build token-shaped test strings at runtime, never as literals”" />
      <button className="btn sm" disabled={saving || !text.trim()}>Add rule</button>
    </form>
  );
}
