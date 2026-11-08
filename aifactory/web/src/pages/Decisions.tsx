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
  const toggle = async (d: Decision) => {
    try {
      await api(`/api/workspaces/${ws.id}/decisions/${d.id}`, { method: "PATCH", json: { reusable: !d.reusable, scope: "workspace" } });
      qc.invalidateQueries({ queryKey: ["decisions", ws.id] });
    } catch (err) {
      toast((err as Error).message, true);
    }
  };
  const precedents = (q.data || []).filter((d) => d.reusable);
  const history = (q.data || []).filter((d) => !d.reusable);
  const admin = ws.role === "owner" || ws.role === "admin";
  return (
    <>
      <div className="page-head">
        <h1>Decisions</h1>
        <span className="muted hide-sm" style={{ fontSize: 12.5 }}>Who decided what, and which decisions the Factory follows next time</span>
      </div>
      <div className="page-body">
        {q.isLoading ? <Skeleton /> : !q.data?.length ? (
          <Empty title="No decisions yet">Approvals, answers and unblocks are recorded here with who made them. Tick "Remember for future work" to turn one into a standing precedent.</Empty>
        ) : (
          <>
            <div className="list-section">Precedents the Factory follows · {precedents.length}</div>
            {precedents.length === 0 && <div className="muted" style={{ padding: "12px 22px", fontSize: 12.5 }}>None yet.</div>}
            <ul className="rows">{precedents.map((d) => <Row key={d.id} d={d} admin={admin} toggle={toggle} open={() => d.outcome_id && navigate(`/w/${ws.id}/outcomes/${d.outcome_id}`)} />)}</ul>
            <div className="list-section">History</div>
            <ul className="rows">{history.map((d) => <Row key={d.id} d={d} admin={admin} toggle={toggle} open={() => d.outcome_id && navigate(`/w/${ws.id}/outcomes/${d.outcome_id}`)} />)}</ul>
          </>
        )}
      </div>
    </>
  );
}

function Row({ d, admin, toggle, open }: { d: Decision; admin: boolean; toggle: (d: Decision) => void; open: () => void }) {
  return (
    <li className="row" onClick={open}>
      <span className={`dot ${d.reusable ? "good" : "idle"}`} />
      <div style={{ minWidth: 0 }}>
        <div className="title">{d.title}</div>
        <div className="sub">
          <span>{DECISION_KIND[d.kind] || d.kind}: <b style={{ color: "var(--ink)", fontWeight: 550 }}>{d.resolution}</b></span>
          {d.comment && <span style={{ overflow: "hidden", textOverflow: "ellipsis" }}>“{d.comment}”</span>}
        </div>
      </div>
      <div className="meta">
        <span>{d.resolved_by_name || "—"} · {ago(d.resolved_at)}</span>
        {admin && d.comment && (
          <button className="btn sm ghost" onClick={(e) => { e.stopPropagation(); toggle(d); }}>{d.reusable ? "Retire" : "Make precedent"}</button>
        )}
      </div>
    </li>
  );
}
