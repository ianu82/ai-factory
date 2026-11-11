import { useMemo, useState } from "react";
import { LinearIssueLink } from "../components/LinearIssueLink";
import { useQuery } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";
import { GitPullRequest, Search } from "lucide-react";
import { api, type Outcome, type Repo } from "../api";
import { useWs } from "../App";
import { ago, money, OUTCOME_STATUS } from "../format";
import { Dot, Empty, GateTrack, Skeleton } from "../ui";
import type { Task } from "../api";
import { OutcomeThread } from "../components/OutcomeThread";
import { SetupNotice, useSetupState } from "../components/Setup";
import { BOARD_COLUMNS, boardColumn, boardColumns, inQA, isQueued, qaCount } from "../outcomesBoard";

const FILTERS: { key: string; label: string; match: (o: Outcome) => boolean }[] = [
  { key: "active", label: "Active", match: (o) => ["planning", "running", "awaiting_approval", "needs_input", "attention", "blocked"].includes(o.status) },
  { key: "needs", label: "Needs you", match: (o) => o.open_decisions > 0 },
  { key: "qa", label: "In QA", match: (o) => boardColumn(o) === "qa" },
  { key: "done", label: "Delivered", match: (o) => ["completed", "partially_completed"].includes(o.status) },
  { key: "all", label: "All", match: () => true },
];

type View = "list" | "board";
const VIEW_KEY = "aifactory.outcomes.view";
function savedView(): View {
  try { return localStorage.getItem(VIEW_KEY) === "board" ? "board" : "list"; } catch { return "list"; }
}

export function OutcomesPage() {
  const { ws, openCapture } = useWs();
  const { outcomeId } = useParams();
  const navigate = useNavigate();
  const [filter, setFilter] = useState("active");
  const [query, setQuery] = useState("");
  const [view, setView] = useState<View>(savedView);
  const choose = (next: View) => {
    setView(next);
    try { localStorage.setItem(VIEW_KEY, next); } catch { /* the choice still applies for this visit */ }
  };
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
  const searched = all.filter((o) => !query || `${o.title} ${o.number}`.toLowerCase().includes(query.toLowerCase()));
  const rows = searched
    .filter(FILTERS.find((f) => f.key === effective)!.match)
    .sort((a, b) => (b.open_decisions > 0 ? 1 : 0) - (a.open_decisions > 0 ? 1 : 0) || a.priority - b.priority || b.updated_at.localeCompare(a.updated_at));

  const select = (id: string) => navigate(`/w/${ws.id}/outcomes/${id}`);
  const close = () => navigate(`/w/${ws.id}/outcomes`);

  // An open outcome is its own page: a conversation with its Lead, its decisions and an inspector.
  if (outcomeId) return <OutcomeThread key={outcomeId} outcomeId={outcomeId} onBack={close} />;

  if (!outcomeId && !outcomes.isLoading && !setup.loading && all.length === 0 && !setup.ready) {
    return (
      <>
        <div className="page-head"><h1>Outcomes</h1></div>
        <div className="page-body"><SetupNotice /></div>
      </>
    );
  }

  return (
    <>
      <div className="page-head">
        <h1>Outcomes</h1>
        <span className="spacer" />
        <div className="seg" role="group" aria-label="Outcomes view">
          {(["list", "board"] as const).map((v) => (
            <button key={v} className={view === v ? "on" : ""} aria-pressed={view === v} onClick={() => choose(v)}>{v === "list" ? "List" : "Board"}</button>
          ))}
        </div>
        <label className="search hide-sm">
          <Search />
          <input placeholder="Search outcomes" value={query} onChange={(e) => setQuery(e.target.value)} aria-label="Search outcomes" />
        </label>
      </div>
      {view === "list" && <div className="toolbar" role="tablist" aria-label="Filter outcomes">
        {FILTERS.map((f) => (
          <button key={f.key} role="tab" aria-selected={effective === f.key} className={`chip ${effective === f.key ? "on" : ""}`} onClick={() => setFilter(f.key)}>
            {f.label} <span className="n">{counts[f.key]}</span>
          </button>
        ))}
      </div>}
      <div className={`split ${outcomeId ? "with-panel" : ""}`}>
        <div className="list-pane">
          {outcomes.isLoading ? <Skeleton /> : view === "board" && all.length > 0 ? (
            <Board outcomes={searched} selected={outcomeId} onSelect={select} />
          ) : rows.length === 0 ? (
            <Empty title={all.length ? "Nothing here" : "No outcomes yet"} action={<button className="btn primary" onClick={openCapture}>Describe an outcome</button>}>
              {all.length ? "Try another filter." : "Describe what you want delivered; the Factory plans it, builds it, verifies it and ships it."}
            </Empty>
          ) : (
            <ul className="rows">
              {rows.map((o) => <OutcomeRow key={o.id} o={o} selected={o.id === outcomeId} onSelect={() => select(o.id)} repos={o.repo_ids.map((r) => repoName.get(r) || "?")} />)}
            </ul>
          )}
        </div>
      </div>
    </>
  );
}

/** The status a list row or board card shows: Queued and In QA are read from the tasks (also beside an open
 * decision, as "Queued · 1 decision"), the rest from the outcome. */
function outcomeStatus(o: Outcome) {
  if (isQueued(o)) return { label: "Queued", tone: "idle" as const };
  if (o.status === "running" && inQA(o)) return { label: "In QA", tone: "active" as const };
  return OUTCOME_STATUS[o.status] || { label: o.status, tone: "idle" as const };
}

/** Building with some tasks already in QA: say how many, so tests in flight are not hidden behind "In progress". */
function QaNote({ o }: { o: Outcome }) {
  const n = qaCount(o);
  return n > 0 && !inQA(o) && boardColumn(o) === "building"
    ? <span className="qa-note" title="Tasks being tested by the configured checks, the independent verifier or CI">{n} in QA</span> : null;
}

function OutcomeRow({ o, selected, onSelect, repos }: { o: Outcome; selected: boolean; onSelect: () => void; repos: string[] }) {
  const queued = isQueued(o);
  const s = outcomeStatus(o);
  return (
    <li className={`row ${selected ? "selected" : ""}`} onClick={onSelect} tabIndex={0} aria-selected={selected}
      onKeyDown={(e) => e.key === "Enter" && onSelect()}>
      <Dot tone={s.tone} title={s.label} />
      <div style={{ minWidth: 0 }}>
        <div className="title">{o.title}</div>
        <div className="sub">
          <span className="ref">#{o.number}</span>
          <LinearIssueLink issue={o.linear_issue} />
          <span title={queued ? "Planned. Tasks start as delivery slots free up; the limit is set in Settings → Throughput." : undefined}
            style={{ color: s.tone === "human" ? "var(--human)" : s.tone === "bad" ? "var(--bad)" : undefined, fontWeight: s.tone === "human" ? 550 : undefined }}>
            {o.open_decisions > 0 ? `${s.label} · ${o.open_decisions} decision${o.open_decisions > 1 ? "s" : ""}` : s.label}
          </span>
          {o.progress.total > 0 && <span className="num">{o.progress.done}/{o.progress.total} tasks</span>}
          <QaNote o={o} />
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

/** Read-only Kanban of the workspace's outcomes, one column per stage. Columns scroll sideways inside the board. */
function Board({ outcomes, selected, onSelect }: { outcomes: Outcome[]; selected?: string; onSelect: (id: string) => void }) {
  const columns = boardColumns(outcomes);
  return (
    <div className="board" aria-label="Outcomes board">
      {BOARD_COLUMNS.map((column) => (
        <section key={column.key} className={`board-col ${column.key}`} aria-label={column.label}>
          <h2 title={column.hint}>{column.label} <span className="n">{columns[column.key].length}</span></h2>
          {columns[column.key].length === 0 ? <p className="board-empty">Nothing here</p> : (
            <ul>
              {columns[column.key].map((o) => <BoardCard key={o.id} o={o} selected={o.id === selected} onSelect={() => onSelect(o.id)} />)}
            </ul>
          )}
        </section>
      ))}
    </div>
  );
}

function BoardCard({ o, selected, onSelect }: { o: Outcome; selected: boolean; onSelect: () => void }) {
  const s = outcomeStatus(o);
  return (
    <li className={`board-card ${selected ? "selected" : ""}`} onClick={onSelect} tabIndex={0} aria-selected={selected}
      onKeyDown={(e) => e.key === "Enter" && onSelect()}>
      <div className="board-title"><span className="ref">#{o.number}</span> {o.title}</div>
      <div className="board-sub">
        <Dot tone={s.tone} title={s.label} />
        <span>{s.label}</span>
        {o.progress.total > 0 && <span className="num">{o.progress.done}/{o.progress.total} tasks</span>}
        <QaNote o={o} />
        {o.open_decisions > 0 && <span className="board-decisions">{o.open_decisions} decision{o.open_decisions > 1 ? "s" : ""}</span>}
        {o.prs.length > 0 && <span className="num" title={o.prs.map((p) => `#${p.number}`).join(", ")}><GitPullRequest size={12} /> {o.prs.length}</span>}
        <span className="num board-ago">{ago(o.updated_at)}</span>
      </div>
    </li>
  );
}
