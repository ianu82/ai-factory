import { useState } from "react";
import { useInfiniteQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { ChevronDown, ChevronRight, GitMerge, GitPullRequest, GitPullRequestDraft, RefreshCw, ShieldAlert } from "lucide-react";
import { api, reviewState, type PullRow, type PullsResponse } from "../api";
import { useWs } from "../App";
import { ago } from "../format";
import { Empty, Skeleton, useToast } from "../ui";

type Filter = "open" | "ready" | "merged" | "all";
const FILTERS: [Filter, string][] = [["open", "Open"], ["ready", "Ready to merge"], ["merged", "Merged"], ["all", "All"]];
const GROUPS: [PullRow["group"], string][] = [
  ["ready", "Ready to merge"], ["attention", "Needs attention"], ["checks", "In progress"], ["draft", "Draft"], ["handed_over", "Handed over"], ["merged", "Merged"],
];
const ICON = {
  ready: <GitPullRequest className="pr-ico ready" aria-label="Ready to merge" />,
  attention: <ShieldAlert className="pr-ico attention" aria-label="Needs attention" />,
  checks: <GitPullRequest className="pr-ico checks" aria-label="In progress" />,
  draft: <GitPullRequestDraft className="pr-ico draft" aria-label="Draft" />,
  handed_over: <GitPullRequest className="pr-ico" aria-label="Handed over" />,
  merged: <GitMerge className="pr-ico merged" aria-label="Merged" />,
};

export function usePulls(filter: Filter = "open") {
  const { ws } = useWs();
  return useInfiniteQuery({
    queryKey: ["pulls", ws.id, filter],
    initialPageParam: "",
    queryFn: ({ pageParam }) => api<PullsResponse>(`/api/workspaces/${ws.id}/pulls/recorded?state=${filter}${pageParam ? `&cursor=${encodeURIComponent(pageParam)}` : ""}`),
    getNextPageParam: (page) => page.next_cursor || undefined,
    refetchInterval: 60_000,
    staleTime: 30_000,
  });
}

export function PullRequestsPage() {
  const toast = useToast();
  const [filter, setFilter] = useState<Filter>("open");
  const [closed, setClosed] = useState<Record<string, boolean>>({ draft: true });
  const [refreshing, setRefreshing] = useState(false);
  const q = usePulls(filter);
  const refresh = async () => {
    setRefreshing(true);
    try {
      await q.refetch({ throwOnError: true });
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setRefreshing(false);
    }
  };
  const first = q.data?.pages[0];
  const items = q.data?.pages.flatMap((page) => page.items) || [];
  const repos = first?.repositories || [];
  return (
    <>
      <div className="page-head">
        <h1>Pull requests</h1>
        <span className="muted hide-sm" style={{ fontSize: 12.5 }}>
          {repos.length ? `${repos.length} ${repos.length === 1 ? "repository" : "repositories"}` : "Factory"}
          {q.data ? ` · updated ${ago(new Date(first!.fetched_at * 1000).toISOString())}` : ""}
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
        {!!first?.notes?.length && <p className="pr-note">{first.notes.join(" ")}</p>}
        {q.isLoading ? <Skeleton rows={5} /> : q.isError ? (
          <Empty title="Couldn't load pull requests">{(q.error as Error).message}</Empty>
        ) : !repos.length && !items.length ? (
          <Empty title="No repositories connected">Connect a repository in Settings → Repositories to see its pull requests here.</Empty>
        ) : !items.length ? (
          <Empty title={filter === "merged" ? "Nothing merged recently" : "No open pull requests"}>
            Recorded Factory pull requests appear here, grouped by what they need next.
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
                  <span className="n" title="Across the whole workspace">{first?.counts[group] ?? rows.length}</span>
                </button>
                {!isClosed && <ul className="pr-rows">{rows.map((r) => <PullRowView key={`${r.repo}#${r.number}`} r={r} />)}</ul>}
              </section>
            );
          })
        )}
        {q.hasNextPage && <button className="btn" disabled={q.isFetchingNextPage} onClick={() => q.fetchNextPage()}>
          {q.isFetchingNextPage ? "Loading…" : "Load older pull requests"}
        </button>}
      </div>
    </>
  );
}

export function PullRowView({ r }: { r: PullRow }) {
  const { ws } = useWs();
  const repoShort = r.repo.split("/")[1] || r.repo;
  const when = r.merged_at || r.updated_at;
  return (
    <li className="pr-row">
      <a className="pr-main" href={r.url} target="_blank" rel="noreferrer" title={`${r.repo}#${r.number} — ${r.reason}`}>
        {r.group === "handed_over" && r.draft ? <GitPullRequestDraft className="pr-ico draft" aria-label="Handed over draft" /> : ICON[r.group]}
        <span className="pr-ref">{repoShort}#{r.number}</span>
        {r.author.avatar ? <img className="pr-avatar" src={r.author.avatar} alt="" title={r.author.login} /> : <span className="pr-avatar" />}
        <span className="pr-text">
          <span className="pr-title">{r.title}</span>
          {r.group !== "ready" || r.factory?.status === "awaiting_approval" ? <span className="pr-reason">{r.reason}</span> : null}
        </span>
      </a>
      <span className="pr-meta">
        {r.factory && (
          <Link className="pr-work" to={`/w/${ws.id}/outcomes/${r.factory.outcome_id}#task-${r.factory.task_id || r.factory.task_key}`} title="Open in AI Factory">
            <span className="pr-work-mark" aria-hidden>F</span>#{r.factory.outcome_number} {r.factory.task_key}
          </Link>
        )}
        {r.factory && <span className="muted">review rounds {r.factory.review?.rounds ?? 0}{reviewState(r.factory.review, r.factory.head_sha) ? ` · ${reviewState(r.factory.review, r.factory.head_sha)}` : ""}</span>}
        {r.detail.additions != null && <span className="pr-add">+{r.detail.additions}</span>}
        {r.detail.deletions != null && <span className="pr-del">−{r.detail.deletions}</span>}
        <span className="pr-age">{ago(when)}</span>
      </span>
    </li>
  );
}
