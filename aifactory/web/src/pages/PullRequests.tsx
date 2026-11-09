import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { ChevronDown, ChevronRight, GitMerge, GitPullRequest, GitPullRequestDraft, RefreshCw, ShieldAlert } from "lucide-react";
import { api, type PullRow, type PullsResponse } from "../api";
import { useWs } from "../App";
import { ago } from "../format";
import { Empty, Skeleton } from "../ui";

type Filter = "open" | "ready" | "merged" | "all";
const FILTERS: [Filter, string][] = [["open", "Open"], ["ready", "Ready to merge"], ["merged", "Merged"], ["all", "All"]];
const GROUPS: [PullRow["group"], string][] = [
  ["ready", "Ready to merge"], ["attention", "Needs attention"], ["checks", "In progress"], ["draft", "Draft"], ["merged", "Merged"],
];
const ICON = {
  ready: <GitPullRequest className="pr-ico ready" aria-label="Ready to merge" />,
  attention: <ShieldAlert className="pr-ico attention" aria-label="Needs attention" />,
  checks: <GitPullRequest className="pr-ico checks" aria-label="In progress" />,
  draft: <GitPullRequestDraft className="pr-ico draft" aria-label="Draft" />,
  merged: <GitMerge className="pr-ico merged" aria-label="Merged" />,
};

export function usePulls(filter: Filter = "open") {
  const { ws } = useWs();
  return useQuery({
    queryKey: ["pulls", ws.id, filter],
    queryFn: () => api<PullsResponse>(`/api/workspaces/${ws.id}/pulls?state=${filter}`),
    refetchInterval: 60_000,
    staleTime: 30_000,
  });
}

export function PullRequestsPage() {
  const { ws } = useWs();
  const qc = useQueryClient();
  const [filter, setFilter] = useState<Filter>("open");
  const [closed, setClosed] = useState<Record<string, boolean>>({ draft: true });
  const [refreshing, setRefreshing] = useState(false);
  const q = usePulls(filter);
  const refresh = async () => {
    setRefreshing(true);
    try {
      const fresh = await api<PullsResponse>(`/api/workspaces/${ws.id}/pulls?state=${filter}&refresh=true`);
      qc.setQueryData(["pulls", ws.id, filter], fresh);
    } finally {
      setRefreshing(false);
    }
  };
  const items = q.data?.items || [];
  const repos = q.data?.repositories || [];
  return (
    <>
      <div className="page-head">
        <h1>Pull requests</h1>
        <span className="muted hide-sm" style={{ fontSize: 12.5 }}>
          {repos.length ? `${repos.length} ${repos.length === 1 ? "repository" : "repositories"} on GitHub` : "GitHub"}
          {q.data ? ` · updated ${ago(new Date(q.data.fetched_at * 1000).toISOString())}` : ""}
        </span>
        <span className="spacer" />
        <button className="btn sm" onClick={refresh} disabled={refreshing}><RefreshCw /> {refreshing ? "Checking…" : "Refresh"}</button>
      </div>
      <div className="toolbar" role="tablist" aria-label="Filter pull requests">
        {FILTERS.map(([key, label]) => (
          <button key={key} role="tab" aria-selected={filter === key} className={`chip ${filter === key ? "on" : ""}`} onClick={() => setFilter(key)}>
            {label}
          </button>
        ))}
      </div>
      <div className="page-body">
        {!!q.data?.errors.length && (
          <div className="pr-error" role="status">
            {q.data.errors.every((e) => /\(401\)|Bad credentials/.test(e))
              ? "GitHub rejected this workspace's credential. Update it in Settings → Credentials."
              : q.data.errors.some((e) => /No GitHub access/.test(e))
                ? "No GitHub credential is configured. Add one in Settings → Credentials."
                : `GitHub didn't answer for everything: ${q.data.errors.slice(0, 3).join(" · ")}`}
          </div>
        )}
        {!!q.data?.notes?.length && <p className="pr-note">{q.data.notes.join(" ")}</p>}
        {q.isLoading ? <Skeleton rows={5} /> : q.isError ? (
          <Empty title="Couldn't load pull requests">{(q.error as Error).message}</Empty>
        ) : !repos.length ? (
          <Empty title="No repositories connected">Connect a repository in Settings → Repositories to see its pull requests here.</Empty>
        ) : !items.length && q.data?.errors.length ? null : !items.length ? (
          <Empty title={filter === "merged" ? "Nothing merged recently" : "No open pull requests"}>
            Pull requests opened by the Factory or by people in {repos.join(", ")} appear here, grouped by what they need next.
          </Empty>
        ) : (
          GROUPS.filter(([g]) => items.some((r) => r.group === g)).map(([group, label]) => {
            const rows = items.filter((r) => r.group === group);
            const isClosed = closed[group];
            return (
              <section key={group} className="pr-group">
                <button className="pr-group-head" aria-expanded={!isClosed} onClick={() => setClosed({ ...closed, [group]: !isClosed })}>
                  {isClosed ? <ChevronRight /> : <ChevronDown />}
                  <span>{label}</span>
                  <span className="n">{rows.length}</span>
                </button>
                {!isClosed && <ul className="pr-rows">{rows.map((r) => <PullRowView key={`${r.repo}#${r.number}`} r={r} />)}</ul>}
              </section>
            );
          })
        )}
      </div>
    </>
  );
}

function PullRowView({ r }: { r: PullRow }) {
  const { ws } = useWs();
  const repoShort = r.repo.split("/")[1] || r.repo;
  const when = r.merged_at || r.updated_at;
  return (
    <li className="pr-row">
      <a className="pr-main" href={r.url} target="_blank" rel="noreferrer" title={`${r.repo}#${r.number} — ${r.reason}`}>
        {ICON[r.group]}
        <span className="pr-ref">{repoShort}#{r.number}</span>
        {r.author.avatar ? <img className="pr-avatar" src={r.author.avatar} alt="" title={r.author.login} /> : <span className="pr-avatar" />}
        <span className="pr-text">
          <span className="pr-title">{r.title}</span>
          {r.group !== "ready" || r.factory?.status === "awaiting_approval" ? <span className="pr-reason">{r.reason}</span> : null}
        </span>
      </a>
      <span className="pr-meta">
        {r.factory && (
          <Link className="pr-work" to={`/w/${ws.id}/outcomes/${r.factory.outcome_id}`} title="Open in AI Factory">
            <span className="pr-work-mark" aria-hidden>F</span>#{r.factory.outcome_number} {r.factory.task_key}
          </Link>
        )}
        {r.detail.additions != null && <span className="pr-add">+{r.detail.additions}</span>}
        {r.detail.deletions != null && <span className="pr-del">−{r.detail.deletions}</span>}
        <span className="pr-age">{ago(when)}</span>
      </span>
    </li>
  );
}
