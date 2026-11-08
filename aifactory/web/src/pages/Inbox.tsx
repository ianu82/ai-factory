import { useEffect, useRef, useState } from "react";
import { LifeBuoy } from "lucide-react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, type Decision, type FactoryEvent } from "../api";
import { useWs } from "../App";
import { ago } from "../format";
import { Empty, Skeleton } from "../ui";
import { DecisionCard } from "../components/DecisionCard";
import { SetupChecklist, useSetupState } from "../components/Setup";
import { RecoveryGuide } from "../components/RecoveryGuide";

type InboxData = { decisions: Decision[]; since: string | null; changes: (FactoryEvent & { outcome_number?: number; outcome_title?: string })[] };

export function InboxPage() {
  const { ws } = useWs();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const setup = useSetupState();
  const [guide, setGuide] = useState(false);
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
  const decisions = [...(q.data?.decisions || [])].sort((a, b) => (a.priority ?? 2) - (b.priority ?? 2) || a.created_at.localeCompare(b.created_at));
  const open = (d: Decision | FactoryEvent) => d.outcome_id && navigate(`/w/${ws.id}/outcomes/${d.outcome_id}`);

  return (
    <>
      <div className="page-head">
        <h1>Inbox</h1>
        <span className="muted" style={{ fontSize: 12.5 }}>{decisions.length ? `${decisions.length} need${decisions.length === 1 ? "s" : ""} you` : ""}</span>
        <span className="spacer" />
        <button className="btn sm ghost" onClick={() => setGuide(true)}><LifeBuoy /> How recovery works</button>
      </div>
      {guide && <RecoveryGuide onClose={() => setGuide(false)} />}
      <div className="page-body">
        {q.isLoading || setup.loading ? <Skeleton rows={4} /> : (
          <>
            {!setup.ready && <SetupChecklist />}
            {decisions.length > 0 && (
              <div style={{ padding: "16px 22px 8px", display: "grid", gap: 10, maxWidth: 820 }}>
                {decisions.map((d) => <DecisionCard key={d.id} d={d} showContext onOpen={() => open(d)} />)}
              </div>
            )}
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
                    <li key={e.id} className="row" onClick={() => open(e)}>
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
    </>
  );
}
