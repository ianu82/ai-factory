import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";
import { GitPullRequest, Search } from "lucide-react";
import { api, type Outcome, type Repo } from "../api";
import { useWs } from "../App";
import { ago, money, OUTCOME_STATUS } from "../format";
import { Dot, Empty, GateTrack, Skeleton } from "../ui";
import type { Task } from "../api";
import { OutcomeDetail } from "../components/OutcomeDetail";
import { SetupChecklist, useSetupState } from "../components/Setup";

const FILTERS: { key: string; label: string; match: (o: Outcome) => boolean }[] = [
  { key: "active", label: "Active", match: (o) => ["planning", "running", "awaiting_approval", "needs_input", "attention", "blocked"].includes(o.status) },
  { key: "needs", label: "Needs you", match: (o) => o.open_decisions > 0 },
  { key: "done", label: "Delivered", match: (o) => ["completed", "partially_completed"].includes(o.status) },
  { key: "all", label: "All", match: () => true },
];

export function OutcomesPage() {
  const { ws, openCapture } = useWs();
  const { outcomeId } = useParams();
  const navigate = useNavigate();
  const [filter, setFilter] = useState("active");
  const [query, setQuery] = useState("");
  const outcomes = useQuery({
    queryKey: ["outcomes", ws.id],
    queryFn: () => api<Outcome[]>(`/api/workspaces/${ws.id}/outcomes`),
    refetchInterval: 20_000,
  });
  const repos = useQuery({ queryKey: ["repos", ws.id], queryFn: () => api<Repo[]>(`/api/workspaces/${ws.id}/repositories`) });
  const setup = useSetupState();
  const repoName = useMemo(() => new Map((repos.data || []).map((r) => [r.id, r.full_name.split("/")[1]])), [repos.data]);
  const all = outcomes.data || [];
  const counts = Object.fromEntries(FILTERS.map((f) => [f.key, all.filter(f.match).length]));
  const effective = filter === "active" && all.length > 0 && counts.active === 0 ? "all" : filter;
  const rows = all
    .filter(FILTERS.find((f) => f.key === effective)!.match)
    .filter((o) => !query || `${o.title} ${o.number}`.toLowerCase().includes(query.toLowerCase()))
    .sort((a, b) => (b.open_decisions > 0 ? 1 : 0) - (a.open_decisions > 0 ? 1 : 0) || a.priority - b.priority || b.updated_at.localeCompare(a.updated_at));

  const select = (id: string) => navigate(`/w/${ws.id}/outcomes/${id}`);
  const close = () => navigate(`/w/${ws.id}/outcomes`);

  if (!outcomes.isLoading && all.length === 0 && !setup.ready) {
    return (
      <>
        <div className="page-head"><h1>Outcomes</h1></div>
        <div className="page-body"><SetupChecklist /></div>
      </>
    );
  }

  return (
    <>
      <div className="page-head">
        <h1>Outcomes</h1>
        <span className="spacer" />
        <label className="search hide-sm">
          <Search />
          <input placeholder="Search outcomes" value={query} onChange={(e) => setQuery(e.target.value)} aria-label="Search outcomes" />
        </label>
      </div>
      <div className="toolbar" role="tablist" aria-label="Filter outcomes">
        {FILTERS.map((f) => (
          <button key={f.key} role="tab" aria-selected={effective === f.key} className={`chip ${effective === f.key ? "on" : ""}`} onClick={() => setFilter(f.key)}>
            {f.label} <span className="n">{counts[f.key]}</span>
          </button>
        ))}
      </div>
      <div className={`split ${outcomeId ? "with-panel" : ""}`}>
        <div className="list-pane">
          {outcomes.isLoading ? <Skeleton /> : rows.length === 0 ? (
            <Empty title={all.length ? "Nothing here" : "No outcomes yet"} action={<button className="btn primary" onClick={openCapture}>Describe an outcome</button>}>
              {all.length ? "Try another filter." : "Describe what you want delivered; the Factory plans it, builds it, verifies it and ships it."}
            </Empty>
          ) : (
            <ul className="rows">
              {rows.map((o) => <OutcomeRow key={o.id} o={o} selected={o.id === outcomeId} onSelect={() => select(o.id)} repos={o.repo_ids.map((r) => repoName.get(r) || "?")} />)}
            </ul>
          )}
        </div>
        {outcomeId && (
          <aside className="panel" aria-label="Outcome detail">
            <OutcomeDetail key={outcomeId} outcomeId={outcomeId} onClose={close} />
          </aside>
        )}
      </div>
    </>
  );
}

function OutcomeRow({ o, selected, onSelect, repos }: { o: Outcome; selected: boolean; onSelect: () => void; repos: string[] }) {
  const s = OUTCOME_STATUS[o.status] || { label: o.status, tone: "idle" as const };
  return (
    <li className={`row ${selected ? "selected" : ""}`} onClick={onSelect} tabIndex={0} aria-selected={selected}
      onKeyDown={(e) => e.key === "Enter" && onSelect()}>
      <Dot tone={s.tone} title={s.label} />
      <div style={{ minWidth: 0 }}>
        <div className="title">{o.title}</div>
        <div className="sub">
          <span className="ref">#{o.number}</span>
          <span style={{ color: s.tone === "human" ? "var(--human)" : s.tone === "bad" ? "var(--bad)" : undefined, fontWeight: s.tone === "human" ? 550 : undefined }}>
            {o.open_decisions > 0 ? `${s.label} · ${o.open_decisions} decision${o.open_decisions > 1 ? "s" : ""}` : s.label}
          </span>
          {o.progress.total > 0 && <span className="num">{o.progress.done}/{o.progress.total} tasks</span>}
          <span className="wide">{repos.join(", ")}</span>
        </div>
      </div>
      <div className="meta">
        {o.track.length > 0 && (
          <span className="hide-sm wide" style={{ display: "flex", flexDirection: "column", gap: 3 }} title={o.track.map((t) => t.key).join(", ")}>
            {o.track.slice(0, 3).map((t) => <GateTrack key={t.key} task={t as unknown as Task} />)}
            {o.track.length > 3 && <span className="faint" style={{ fontSize: 10.5, lineHeight: 1 }}>+{o.track.length - 3} more</span>}
          </span>
        )}
        {o.prs.length > 0 && (
          <span className="wide" title={o.prs.map((p) => `#${p.number}`).join(", ")} style={{ display: "inline-flex", alignItems: "center", gap: 3 }}>
            <GitPullRequest size={13} /> {o.prs.length}
          </span>
        )}
        <span className="num hide-sm wide">{money(o.cost_usd, o.cost_known)}</span>
        <span className="num" style={{ minWidth: 56 }}>{ago(o.updated_at)}</span>
      </div>
    </li>
  );
}
