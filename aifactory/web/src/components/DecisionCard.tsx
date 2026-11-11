import { useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { ExternalLink } from "lucide-react";
import { api, type Decision } from "../api";
import { useWs } from "../App";
import { ago, DECISION_KIND } from "../format";
import { Prose, useToast } from "../ui";

const NEEDS_TEXT = new Set(["answer", "replan", "reject"]);

export function DecisionCard({ d, showContext = false, onOpen, focused = false }: { d: Decision; showContext?: boolean; onOpen?: () => void; focused?: boolean }) {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const [comment, setComment] = useState("");
  const [remember, setRemember] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const textNeeded = d.options.some((o) => NEEDS_TEXT.has(o.value));
  const [noting, setNoting] = useState(d.kind === "question");
  const canAct = ws.role !== "viewer" && d.status === "open" && d.actionable !== false;
  const card = useRef<HTMLElement>(null);
  useEffect(() => {
    if (focused) {
      card.current?.scrollIntoView({ block: "center" });
      card.current?.focus({ preventScroll: true });
    }
  }, [focused]);

  const resolve = async (value: string) => {
    if (NEEDS_TEXT.has(value) && !comment.trim()) {
      toast("Add a short answer or instruction first.", true);
      return;
    }
    setBusy(value);
    try {
      await api(`/api/workspaces/${ws.id}/decisions/${d.id}/resolve`, {
        method: "POST",
        json: { resolution: value, comment, reusable: remember, scope: remember ? "workspace" : "item" },
      });
      toast("Recorded");
      qc.invalidateQueries({ queryKey: ["inbox", ws.id] });
      qc.invalidateQueries({ queryKey: ["outcome", ws.id] });
      qc.invalidateQueries({ queryKey: ["outcomes", ws.id] });
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(null);
    }
  };

  return (
    <article ref={card} id={`decision-${d.id}`} tabIndex={-1} className={`decide ${focused ? "focused" : ""}`} aria-label={d.title}>
      <span className="kind-tag">{DECISION_KIND[d.kind] || d.kind} · {ago(d.created_at)}</span>
      <h4>{d.title}</h4>
      {showContext && d.outcome_number != null && (
        <div className="muted" style={{ fontSize: 12.5, marginBottom: 6 }}>
          <button className="linkish" onClick={onOpen}>#{d.outcome_number} {d.outcome_title}{d.task_key ? ` · ${d.task_key}` : ""}</button>
        </div>
      )}
      {d.body && <Prose text={d.body} />}
      {d.pr_url && <a className="linkish" href={d.pr_url} target="_blank" rel="noreferrer" style={{ marginTop: 6 }}>Review pull request #{d.pr_number ?? d.pr_url.split("/").pop()} on GitHub <ExternalLink /></a>}
      {d.lead_note && <div className="lead-note" style={{ marginTop: 8 }}>
        <b>Lead{d.outcome_number != null ? ` of #${d.outcome_number}` : ""} · {ago(d.lead_note.created_at)}</b>
        <Prose text={d.lead_note.text} />
      </div>}
      {d.judge && <div className="muted" style={{ marginTop: 8 }}>
        <b>Decision judge · {d.judge.effective_verdict || d.judge.state}</b>
        <p>{d.judge.summary}</p>
        {d.judge.answer && <Prose text={d.judge.answer} />}
        {d.judge.citations.map((c, i) => <p key={i}>Decision {c.decision_id}: “{c.passage}”</p>)}
        {!!d.judge.closest_decisions.length && <p>Closest decisions: {d.judge.closest_decisions.join(", ")}</p>}
      </div>}
      {d.correction && <p>Corrected by {d.correction.resolved_by_name} · {ago(d.correction.resolved_at)}: {d.correction.answer}<br />Reason: {d.correction.attribution.reason}</p>}
      {d.supersedes && <p>Correction of decision {d.supersedes}</p>}
      {canAct && (
        <>
          {noting && (
            <textarea className="textarea" autoFocus={d.kind !== "question"} placeholder={textNeeded ? "Your answer or instructions — sent to the Factory and recorded" : "Note for the record"}
              value={comment} onChange={(e) => setComment(e.target.value)} aria-label="Comment" />
          )}
          <div className="opts">
            {d.options.map((o, i) => (
              <button key={o.value} className={`btn sm ${i === 0 ? "primary" : ""}`} disabled={!!busy}
                onClick={() => (NEEDS_TEXT.has(o.value) && !noting ? setNoting(true) : resolve(o.value))}>
                {busy === o.value ? "…" : o.label}
              </button>
            ))}
            {!noting && <button className="btn sm ghost" onClick={() => setNoting(true)}>Add note</button>}
            <span style={{ flex: 1 }} />
            {noting && (
              <label className="check" title="Future plans and builds in this workspace will follow this decision">
                <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} disabled={!comment.trim()} />
                Remember for future work
              </label>
            )}
          </div>
        </>
      )}
    </article>
  );
}
