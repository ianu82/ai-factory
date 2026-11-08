import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, type Run } from "../api";
import { useWs } from "../App";
import { ago, duration, HARNESS_LABEL, money, tokens } from "../format";
import { Dot, Empty, Skeleton } from "../ui";

type Stats = {
  tasks: { total: number; completed: number; merged: number; first_attempt: number; smoke_failed: number };
  cost: { usd: number; known: boolean; per_completed_task: number };
  tokens: number;
  human: { decisions: number; open: number; interventions: number };
  harnesses: Record<string, { runs: number; succeeded: number }>;
};

const RUN_TONE: Record<string, "good" | "bad" | "active" | "idle"> = { succeeded: "good", failed: "bad", error: "bad", running: "active", cancelled: "idle" };

export function RunsPage() {
  const { ws } = useWs();
  const navigate = useNavigate();
  const runs = useQuery({ queryKey: ["runs", ws.id], queryFn: () => api<(Run & { outcome_number: number })[]>(`/api/workspaces/${ws.id}/runs?limit=200`), refetchInterval: 15_000 });
  const stats = useQuery({ queryKey: ["runs", ws.id, "stats"], queryFn: () => api<Stats>(`/api/workspaces/${ws.id}/stats`) });
  const s = stats.data;
  return (
    <>
      <div className="page-head"><h1>Runs</h1><span className="muted" style={{ fontSize: 12.5 }}>Every agent session, what it cost and how it ended</span></div>
      {s && s.tasks.total > 0 && (
        <div className="toolbar" style={{ gap: 22, fontSize: 12.5, color: "var(--muted)" }}>
          <span><b className="num" style={{ color: "var(--ink)" }}>{s.tasks.completed}</b> of {s.tasks.total} tasks delivered</span>
          <span><b className="num" style={{ color: "var(--ink)" }}>{s.tasks.merged ? Math.round((s.tasks.first_attempt / s.tasks.merged) * 100) : 0}%</b> merged on first attempt</span>
          <span><b className="num" style={{ color: "var(--ink)" }}>{money(s.cost.usd, s.cost.known)}</b> total · {money(s.cost.per_completed_task, s.cost.known)} per delivered task</span>
          <span><b className="num" style={{ color: "var(--ink)" }}>{s.human.decisions}</b> human decisions · {s.human.interventions} interventions</span>
          {s.tasks.smoke_failed > 0 && <span style={{ color: "var(--bad)" }}>{s.tasks.smoke_failed} smoke failures after merge</span>}
        </div>
      )}
      <div className="page-body">
        {runs.isLoading ? <Skeleton /> : !runs.data?.length ? (
          <Empty title="No runs yet">Planner, builder and verifier sessions appear here as outcomes are worked on.</Empty>
        ) : (
          <table className="table">
            <thead>
              <tr><th></th><th>Session</th><th>Outcome</th><th className="hide-sm">Model</th><th className="r">Turns</th><th className="r">Tokens</th><th className="r">Cost</th><th className="r hide-sm">Duration</th><th className="r">Started</th></tr>
            </thead>
            <tbody>
              {runs.data.map((r) => (
                <tr key={r.id} onClick={() => navigate(`/w/${ws.id}/outcomes/${r.outcome_id}`)} style={{ cursor: "pointer" }} title={r.error || undefined}>
                  <td style={{ width: 14 }}><Dot tone={RUN_TONE[r.status] || "idle"} title={r.status} /></td>
                  <td><b style={{ fontWeight: 550 }}>{r.role}</b> <span className="muted">· {HARNESS_LABEL[r.harness]}{r.attempt > 1 ? ` · attempt ${r.attempt}` : ""}</span>
                    {r.error && <div className="muted" style={{ fontSize: 12, maxWidth: 380, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.error}</div>}</td>
                  <td style={{ maxWidth: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    <span className="ref">#{r.outcome_number}{r.task_key ? ` ${r.task_key}` : ""}</span> {r.outcome_title}
                  </td>
                  <td className="mono hide-sm">{r.model}</td>
                  <td className="r num">{r.turns}</td>
                  <td className="r num">{tokens(r.input_tokens + r.output_tokens)}</td>
                  <td className="r num">{money(r.cost_usd, r.cost_known)}</td>
                  <td className="r num hide-sm">{duration(r.started_at, r.finished_at)}</td>
                  <td className="r num muted">{ago(r.started_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </>
  );
}
