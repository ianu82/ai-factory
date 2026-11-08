import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { ExternalLink, RefreshCw } from "lucide-react";
import { api, type Signal } from "../api";
import { useWs } from "../App";
import { ago } from "../format";
import { Dot, Empty, Skeleton, useToast } from "../ui";

const SOURCE: Record<string, string> = {
  github_issue: "GitHub issue", ci_failure: "CI failing on main", smoke_failure: "Post-merge smoke", release_failure: "Release check", feedback: "Feedback",
};

export function SignalsPage() {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const [busy, setBusy] = useState(false);
  const q = useQuery({ queryKey: ["signals", ws.id], queryFn: () => api<Signal[]>(`/api/workspaces/${ws.id}/signals`) });
  const sync = async () => {
    setBusy(true);
    try {
      const r = await api<{ created: number; updated: number }>(`/api/workspaces/${ws.id}/signals/sync`, { method: "POST" });
      toast(`${r.created} new, ${r.updated} updated`);
      qc.invalidateQueries({ queryKey: ["signals", ws.id] });
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(false);
    }
  };
  const act = async (s: Signal, action: "accept" | "ignore") => {
    try {
      const r = await api<Signal>(`/api/workspaces/${ws.id}/signals/${s.id}`, { method: "POST", json: { action } });
      qc.invalidateQueries({ queryKey: ["signals", ws.id] });
      if (action === "accept" && r.outcome_id) navigate(`/w/${ws.id}/outcomes/${r.outcome_id}`);
    } catch (err) {
      toast((err as Error).message, true);
    }
  };
  const fresh = (q.data || []).filter((s) => s.status === "new");
  const rest = (q.data || []).filter((s) => s.status !== "new");
  return (
    <>
      <div className="page-head">
        <h1>Signals</h1>
        <span className="muted hide-sm" style={{ fontSize: 12.5 }}>Product feedback that may become work. Nothing starts until someone accepts it.</span>
        <span className="spacer" />
        <button className="btn sm" onClick={sync} disabled={busy}><RefreshCw /> {busy ? "Checking…" : "Check GitHub"}</button>
      </div>
      <div className="page-body">
        {q.isLoading ? <Skeleton /> : !q.data?.length ? (
          <Empty title="No signals">
            Label a GitHub issue <code className="mono">factory</code>, or let failing CI on the default branch and post-merge smoke failures arrive here automatically.
          </Empty>
        ) : (
          <>
            {fresh.length > 0 && <div className="list-section">New · {fresh.length}</div>}
            <ul className="rows">{fresh.map((s) => <SignalRow key={s.id} s={s} act={act} canAct={ws.role !== "viewer"} />)}</ul>
            {rest.length > 0 && <div className="list-section">Handled</div>}
            <ul className="rows">{rest.map((s) => <SignalRow key={s.id} s={s} act={act} canAct={false} />)}</ul>
          </>
        )}
      </div>
    </>
  );
}

function SignalRow({ s, act, canAct }: { s: Signal; act: (s: Signal, a: "accept" | "ignore") => void; canAct: boolean }) {
  return (
    <li className="row" style={{ cursor: "default" }}>
      <Dot tone={s.status === "new" ? (s.source.includes("failure") ? "bad" : "human") : "idle"} />
      <div style={{ minWidth: 0 }}>
        <div className="title">{s.title}</div>
        <div className="sub">
          <span>{SOURCE[s.source] || s.source}</span>
          {s.count > 1 && <span className="num">seen {s.count}×</span>}
          <span>{ago(s.last_seen_at)}</span>
          {s.status !== "new" && <span>{s.status}</span>}
          {s.url && <a className="linkish" href={s.url} target="_blank" rel="noreferrer">source <ExternalLink /></a>}
        </div>
      </div>
      <div className="meta">
        {canAct && <><button className="btn sm ghost" onClick={() => act(s, "ignore")}>Ignore</button><button className="btn sm primary" onClick={() => act(s, "accept")}>Make it an outcome</button></>}
      </div>
    </li>
  );
}
