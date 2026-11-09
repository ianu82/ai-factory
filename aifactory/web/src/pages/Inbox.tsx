import { useEffect, useRef, useState } from "react";
import { LifeBuoy, X } from "lucide-react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";
import { api, type Decision, type FactoryEvent } from "../api";
import { useWs } from "../App";
import { ago, DECISION_KIND } from "../format";
import { Dot, Empty, Skeleton, useToast } from "../ui";
import { DecisionCard } from "../components/DecisionCard";
import { OutcomeDetail } from "../components/OutcomeDetail";
import { SetupNotice, useSetupState } from "../components/Setup";
import { RecoveryGuide } from "../components/RecoveryGuide";

type InboxData = { decisions: Decision[]; since: string | null; changes: (FactoryEvent & { outcome_number?: number; outcome_title?: string })[] };

const NEEDS_TEXT = new Set(["answer", "replan", "reject"]);
const GROUPS: { key: string; label: string; kinds: string[] }[] = [
  { key: "merge", label: "Merge approvals", kinds: ["approve_merge", "approve_release"] },
  { key: "plan", label: "Plans to approve", kinds: ["approve_plan"] },
  { key: "question", label: "Questions", kinds: ["question"] },
  { key: "stuck", label: "Stuck", kinds: ["unblock", "budget"] },
];

/** One line that says why this needs a person: the useful part of the body, without boilerplate. */
function highlight(d: Decision): string {
  let text = d.body || "";
  const feedback = text.indexOf("Latest feedback:");
  if (feedback >= 0) text = text.slice(feedback + "Latest feedback:".length);
  text = text.replace(/https?:\/\/\S+/g, "").replace(/[`*_#>]/g, "").replace(/^\s*[-•]\s*/gm, "").replace(/\s+/g, " ").trim();
  return text.length > 180 ? `${text.slice(0, 177)}…` : text;
}

export function InboxPage() {
  const { ws } = useWs();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const toast = useToast();
  const setup = useSetupState();
  const { decisionId } = useParams();
  const [guide, setGuide] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const q = useQuery({ queryKey: ["inbox", ws.id], queryFn: () => api<InboxData>(`/api/workspaces/${ws.id}/inbox`) });
  const marked = useRef(false);
  useEffect(() => {
    // Leaving the inbox counts as having looked; capture "since" first so this visit still shows changes.
    return () => {
      if (!marked.current && q.data) {
        marked.current = true;
        api(`/api/workspaces/${ws.id}/seen`, { method: "POST" }).then(() => qc.invalidateQueries({ queryKey: ["inbox", ws.id] }));
      }
    };
  }, [ws.id, q.data, qc]);

  const [order, setOrderState] = useState<"priority" | "newest">(() => {
    try { return localStorage.getItem("factory:inbox:order") === "newest" ? "newest" : "priority"; } catch { return "priority"; }
  });
  const setOrder = (next: "priority" | "newest") => {
    setOrderState(next);
    try { localStorage.setItem("factory:inbox:order", next); } catch { /* per-viewer convenience only */ }
  };
  const decisions = [...(q.data?.decisions || [])].sort((a, b) => order === "newest"
    ? b.created_at.localeCompare(a.created_at)
    : (a.priority ?? 2) - (b.priority ?? 2) || a.created_at.localeCompare(b.created_at));
  const selected = decisions.find((d) => d.id === decisionId) || null;
  const select = (d: Decision) => navigate(`/w/${ws.id}/inbox/${d.id}`);
  const close = () => navigate(`/w/${ws.id}/inbox`);
  const openOutcome = (e: FactoryEvent) => e.outcome_id && navigate(`/w/${ws.id}/outcomes/${e.outcome_id}`);
  const canAct = ws.role !== "viewer";

  const quick = async (d: Decision, value: string) => {
    setBusy(`${d.id}:${value}`);
    try {
      await api(`/api/workspaces/${ws.id}/decisions/${d.id}/resolve`, { method: "POST", json: { resolution: value, comment: "" } });
      toast("Recorded");
      qc.invalidateQueries({ queryKey: ["inbox", ws.id] });
      qc.invalidateQueries({ queryKey: ["outcome", ws.id] });
      qc.invalidateQueries({ queryKey: ["outcomes", ws.id] });
      if (decisionId === d.id) close();
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(null);
    }
  };

  // Priority groups decisions by what they ask for; Newest is one chronological list.
  const byKind = GROUPS.map((g) => ({ ...g, rows: decisions.filter((d) => g.kinds.includes(d.kind)) }))
    .concat([{ key: "other", label: "Other", kinds: [], rows: decisions.filter((d) => !GROUPS.some((g) => g.kinds.includes(d.kind))) }]);
  const groups = (order === "newest" ? [{ key: "newest", label: "Newest first", kinds: [] as string[], rows: decisions }] : byKind)
    .filter((g) => g.rows.length);

  const onRowKey = (e: React.KeyboardEvent<HTMLLIElement>, d: Decision) => {
    if (e.key === "Enter") select(d);
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      const rows = Array.from(document.querySelectorAll<HTMLLIElement>(".ib-row"));
      const i = rows.indexOf(e.currentTarget);
      rows[Math.max(0, Math.min(rows.length - 1, i + (e.key === "ArrowDown" ? 1 : -1)))]?.focus();
    }
  };

  return (
    <>
      <div className="page-head">
        <h1>Inbox</h1>
        <span className="muted" style={{ fontSize: 12.5, whiteSpace: "nowrap" }}>{decisions.length ? `${decisions.length} need${decisions.length === 1 ? "s" : ""} you` : ""}</span>
        <span className="spacer" />
        <button className="btn sm ghost" onClick={() => setGuide(true)}><LifeBuoy /> How recovery works</button>
      </div>
      <div className="toolbar" role="group" aria-label="Sort decisions">
        <button className={`chip ${order === "priority" ? "on" : ""}`} aria-pressed={order === "priority"} onClick={() => setOrder("priority")}>Priority</button>
        <button className={`chip ${order === "newest" ? "on" : ""}`} aria-pressed={order === "newest"} onClick={() => setOrder("newest")}>Newest</button>
      </div>
      {guide && <RecoveryGuide onClose={() => setGuide(false)} />}
      <div className={`split ${selected ? "with-panel" : ""}`}>
        <div className="list-pane">
          {q.isLoading || setup.loading ? <Skeleton rows={4} /> : (
            <>
              {(!setup.ready || !setup.firstOutcome) && <SetupNotice />}
              {groups.map((g) => (
                <section key={g.key} aria-label={g.label}>
                  <div className="list-section">{g.label} · {g.rows.length}</div>
                  <ul className="rows">
                    {g.rows.map((d) => {
                      const primary = d.options[0];
                      const inline = canAct && primary && !NEEDS_TEXT.has(primary.value) ? primary : null;
                      return (
                        <li key={d.id} className={`row ib-row ${d.id === decisionId ? "selected" : ""}`} tabIndex={0} aria-selected={d.id === decisionId}
                          onClick={() => select(d)} onKeyDown={(e) => onRowKey(e, d)}>
                          <Dot tone={d.kind === "unblock" || d.kind === "budget" ? "bad" : "human"} />
                          <div style={{ minWidth: 0 }}>
                            <div className="title">
                              <span className="ib-kind">{DECISION_KIND[d.kind] || d.kind}</span>{d.title.replace(/^Approve merge: /, "").replace(/^Approve plan for #\d+: /, "")}
                            </div>
                            <div className="sub">
                              {d.outcome_number != null && <span className="ref">#{d.outcome_number}{d.task_key ? ` ${d.task_key}` : ""}</span>}
                              {d.pr_number != null && <span className="num">PR #{d.pr_number}</span>}
                              <span className="ib-why">{highlight(d) || d.outcome_title}</span>
                            </div>
                          </div>
                          <div className="meta">
                            {inline && (
                              <button className="btn sm primary ib-act" disabled={busy !== null}
                                onClick={(e) => { e.stopPropagation(); quick(d, inline.value); }}>
                                {busy === `${d.id}:${inline.value}` ? "…" : inline.label}
                              </button>
                            )}
                            {canAct && d.options.length > (inline ? 1 : 0) && (
                              <button className="btn sm ib-act wide" onClick={(e) => { e.stopPropagation(); select(d); }}>
                                {d.options.find((o) => o !== inline && NEEDS_TEXT.has(o.value))?.label || "More"}
                              </button>
                            )}
                            <span className="num" style={{ minWidth: 52 }}>{ago(d.created_at)}</span>
                          </div>
                        </li>
                      );
                    })}
                  </ul>
                </section>
              ))}
              {decisions.length === 0 && setup.ready && (
                <Empty title="Nothing needs you">
                  Running work stays in Outcomes. Plan approvals, questions, merges that need a person and anything that got stuck land here.
                </Empty>
              )}
              {(q.data?.changes.length || 0) > 0 && (
                <>
                  <div className="list-section" style={{ marginTop: 12 }}>Since you looked · {ago(q.data?.since)}</div>
                  <ul className="rows">
                    {q.data!.changes.slice(0, 15).map((e) => (
                      <li key={e.id} className="row" onClick={() => openOutcome(e)}>
                        <span className={`dot ${e.level === "success" ? "good" : e.level === "error" ? "bad" : "idle"}`} />
                        <div style={{ minWidth: 0 }}>
                          <div className="title" style={{ fontWeight: 500 }}>{e.message}</div>
                          {e.outcome_number != null && <div className="sub"><span className="ref">#{e.outcome_number}</span><span>{e.outcome_title}</span></div>}
                        </div>
                        <span className="meta">{ago(e.created_at)}</span>
                      </li>
                    ))}
                  </ul>
                </>
              )}
            </>
          )}
        </div>
        {selected && (
          <aside className="panel" aria-label="Decision detail">
            {selected.outcome_id ? (
              <OutcomeDetail key={selected.outcome_id} outcomeId={selected.outcome_id} onClose={close} focusDecisionId={selected.id} />
            ) : (
              <div>
                <header className="p-head">
                  <div className="crumbs"><span>{DECISION_KIND[selected.kind] || selected.kind}</span><span style={{ flex: 1 }} />
                    <button className="icon-btn" onClick={close} aria-label="Close panel"><X /></button></div>
                  <h2>{selected.title}</h2>
                </header>
                <section className="p-sec"><DecisionCard d={selected} /></section>
              </div>
            )}
          </aside>
        )}
      </div>
    </>
  );
}
