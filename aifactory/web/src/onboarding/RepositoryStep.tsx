import { useEffect, useRef, useState } from "react";
import { useInfiniteQuery, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type Credential, type Repo } from "../api";
import { previewRepository, repositoryDiscoveryUrl, type RepositoryPreview, type ReviewedCommands } from "../repositoryConnect";
import { commandsFromRepository, repositoryStack, saveRepositoryReview } from "../repositoryConnect";
import { GitHubPermissions } from "./GitHubPermissions";
import { getOnboarding, onboardingQueryKey, type OnboardingStepProps } from "./api";

type Discovery = { items: { full_name: string; private: boolean; can_push: boolean; description: string }[]; next_page: number | null; error?: string };
type Review = {
  preview?: RepositoryPreview;
  repoId?: string;
  commands: ReviewedCommands;
  noStart: boolean;
  reviewed: boolean;
  status: "preview" | "editing" | "pending" | "success" | "error";
  error?: string;
  diagnostic?: Credential;
  diagnosticBusy?: boolean;
  diagnosticError?: string;
};
const blank = (): Review => ({ commands: { setup: "", checks: "", start: "", deploys: false }, noStart: false, reviewed: false, status: "preview" });

export function RepositoryStep({ workspace, readOnly, onComplete, onNavigate }: OnboardingStepProps) {
  const id = workspace.id;
  const qc = useQueryClient();
  const [search, setSearch] = useState("");
  const [query, setQuery] = useState("");
  const [reviews, setReviews] = useState<Record<string, Review>>({});
  const [finishing, setFinishing] = useState(false);
  const [error, setError] = useState("");
  const saving = useRef(new Set<string>());
  const reposKey = ["repos", id];
  const repos = useQuery({ queryKey: reposKey, queryFn: () => api<Repo[]>(`/api/workspaces/${id}/repositories`) });
  useEffect(() => {
    const timer = setTimeout(() => setQuery(search.trim()), 300);
    return () => clearTimeout(timer);
  }, [search]);
  // Hydrate saved configuration on revisits, without overwriting another card's edits.
  useEffect(() => {
    if (!repos.data) return;
    setReviews((current) => {
      const next = { ...current };
      for (const repo of repos.data) {
        const key = repo.full_name.toLowerCase();
        if (!next[key]) next[key] = { preview: repo, repoId: repo.id, commands: commandsFromRepository(repo), noStart: !repo.config.qa?.start, reviewed: true, status: "success" };
      }
      return next;
    });
  }, [repos.data]);
  const available = useInfiniteQuery({
    queryKey: ["github-repos", id, query], enabled: !readOnly,
    initialPageParam: 1,
    queryFn: ({ pageParam }) => api<Discovery>(repositoryDiscoveryUrl(id, query, pageParam)),
    getNextPageParam: (page) => page.next_page ?? undefined,
  });
  const update = (key: string, patch: Partial<Review>) => setReviews((current) => ({ ...current, [key]: { ...current[key], ...patch } }));
  const diagnose = async (key: string, name: string) => {
    update(key, { diagnosticBusy: true, diagnosticError: "" });
    try {
      const diagnostic = await api<Credential>(`/api/workspaces/${id}/credentials/github_token/test?repository=${encodeURIComponent(name)}`, { method: "POST" });
      update(key, { diagnostic });
      await Promise.all([["credentials", id], ["models", id], ["github-repos", id], onboardingQueryKey(id)].map((queryKey) => qc.invalidateQueries({ queryKey })));
    } catch (err) { update(key, { diagnosticError: (err as Error).message }); }
    finally { update(key, { diagnosticBusy: false }); }
  };
  const choose = async (name: string) => {
    const key = name.toLowerCase();
    update(key, { ...blank() });
    void diagnose(key, name);
    try {
      const preview = await previewRepository(id, name, api);
      update(key, { preview, commands: commandsFromRepository(preview), status: "editing" });
    } catch (err) { update(key, { status: "error", error: (err as Error).message }); }
  };
  const save = async (key: string, review: Review) => {
    if (!review.preview || !review.reviewed || (!review.commands.start.trim() && !review.noStart) || saving.current.has(key)) return;
    saving.current.add(key);
    update(key, { status: "pending", error: "" });
    try {
      const repo = await saveRepositoryReview(id, review.preview, review.commands, review.repoId, api);
      update(key, { preview: repo, repoId: repo.id, commands: commandsFromRepository(repo), status: "success" });
      void qc.invalidateQueries({ queryKey: reposKey });
    } catch (err) { update(key, { status: "error", error: (err as Error).message }); }
    finally { saving.current.delete(key); }
  };
  const complete = async () => {
    setFinishing(true);
    setError("");
    try {
      // Explicit fetches include inactive credential queries; do not complete on stale readiness.
      await Promise.all([
        qc.fetchQuery({ queryKey: reposKey, queryFn: () => api<Repo[]>(`/api/workspaces/${id}/repositories`), staleTime: 0 }),
        qc.fetchQuery({ queryKey: onboardingQueryKey(id), queryFn: () => getOnboarding(id), staleTime: 0 }),
        qc.fetchQuery({ queryKey: ["credentials", id], queryFn: () => api(`/api/workspaces/${id}/credentials`), staleTime: 0 }),
      ]);
      await onComplete({ action: "complete", step: "repositories" });
      await onNavigate("autonomy");
    } catch (err) { setError((err as Error).message); }
    finally { setFinishing(false); }
  };
  const items = [...new Map((available.data?.pages.flatMap((page) => page.items) || []).map((repo) => [repo.full_name.toLowerCase(), repo])).values()];
  const discoveryError = available.data?.pages.find((page) => page.error)?.error || available.error?.message;
  const entries = Object.entries(reviews);
  const allSaved = entries.length > 0 && entries.every(([, review]) => review.status === "success");

  return <section className="repository-step" aria-label="Repository selection and command review">
    <p>Choose repositories, then review each command before connecting. Saved repositories are reused when you return.</p>
    {repos.isLoading && <p role="status">Loading connected repositories…</p>}
    {repos.isError && <div role="alert"><p>Could not load connected repositories: {repos.error.message}</p><button className="btn" onClick={() => repos.refetch()}>Retry connected repositories</button></div>}
    {!readOnly && <>
      <label className="field"><span>Search accessible GitHub repositories</span><input className="input" type="search" maxLength={200} value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Repository name or owner/name" /></label>
      {available.isLoading && <p role="status">Finding repositories…</p>}
      {discoveryError && <div role="alert"><p>{discoveryError}</p><p>Check that your token includes these repositories and your organization permits access.</p><button className="btn" onClick={() => available.refetch()}>Retry search</button></div>}
      {!available.isLoading && !discoveryError && !items.length && <p role="status">No accessible repositories match this search. Try another name or check your token’s repository access.</p>}
      <ul className="repository-options" aria-label="Accessible repositories">
        {items.map((repo) => {
          const selected = !!reviews[repo.full_name.toLowerCase()];
          return <li key={repo.full_name}>
            <label className="check"><input type="checkbox" checked={selected} disabled={selected || !repo.can_push || repos.isLoading || repos.isError || finishing} onChange={() => choose(repo.full_name)} />
              <span>{repo.full_name}<small>{repo.can_push ? (repo.private ? "Private · push access" : "Public · push access") : "Read-only access · write permission required"}</small></span>
            </label>
            {repo.description && <p className="muted">{repo.description}</p>}
          </li>;
        })}
      </ul>
      {available.hasNextPage && <button className="btn" disabled={available.isFetchingNextPage} onClick={() => available.fetchNextPage()}>{available.isFetchingNextPage ? "Loading more…" : "Load more repositories"}</button>}
    </>}
    {entries.map(([key, review], index) => {
      const locked = readOnly || finishing || review.status === "pending" || review.status === "success";
      const edit = (commands: ReviewedCommands) => update(key, { commands, reviewed: false, error: "", status: "editing" });
      const prefix = `repo-review-${index}`;
      return <section className="repository-review" key={key} aria-label={`Review ${review.preview?.full_name || key}`}>
        <h2>{review.preview?.full_name || key}</h2>
        <div aria-live="polite">
          {review.status === "preview" && <p role="status">Detecting stack and commands…</p>}
          {review.status === "pending" && <p role="status">Saving reviewed configuration…</p>}
          {review.status === "success" && <p role="status">Connected · configuration saved</p>}
          {review.error && <p role="alert">{review.error}</p>}
        </div>
        {!review.preview && review.status === "error" && <button className="btn" disabled={readOnly || finishing} onClick={() => choose(key)}>Retry preview</button>}
        {!review.repoId && <button className="btn ghost" disabled={readOnly || finishing || review.status === "pending" || review.status === "preview" || review.diagnosticBusy} onClick={() => setReviews((current) => {
          const next = { ...current };
          delete next[key];
          return next;
        })}>Remove selection</button>}
        {review.preview && <>
          <p>Detected stack: {repositoryStack(review.preview).join(", ") || "Unknown — no stack detected"}. Default branch: {review.preview.default_branch}.</p>
          <form onSubmit={(e) => { e.preventDefault(); void save(key, review); }}>
            <label className="field"><span>Setup commands (one per line)</span><textarea className="textarea mono" disabled={locked} value={review.commands.setup} placeholder="Unknown — enter commands or leave empty if not needed" onChange={(e) => edit({ ...review.commands, setup: e.target.value })} /><span className="hint">{review.commands.setup.trim() ? "Installs dependencies in the sandbox." : "No setup command known; confirm whether setup is needed."}</span></label>
            <label className="field"><span>Test / check commands (config.checks, one per line)</span><textarea className="textarea mono" disabled={locked} value={review.commands.checks} placeholder="Unknown — enter your test commands" onChange={(e) => edit({ ...review.commands, checks: e.target.value })} /><span className="hint">{review.commands.checks.trim() ? "Finite checks run during verification." : "No test command known; no tests will run unless you supply one."}</span></label>
            <label className="field"><span>Start command (config.qa.start)</span><textarea className="textarea mono" disabled={locked} value={review.commands.start} placeholder="Unknown — no start command detected" onChange={(e) => { edit({ ...review.commands, start: e.target.value }); update(key, { noStart: false }); }} /><span className="hint">{review.preview.config.start_detection?.reason} {review.commands.start.trim() ? "Must listen on $PORT for runtime QA. This is not a smoke check." : "Start command unknown / not configured. Enter one or acknowledge no runnable app below."}</span></label>
            {!review.commands.start.trim() && <label className="check"><input type="checkbox" disabled={locked} checked={review.noStart} onChange={(e) => update(key, { noStart: e.target.checked, reviewed: false })} /> This repository has no runnable app (for example, a library or documentation). Continue without runtime QA.</label>}
            <div><h3>Existing smoke checks (unchanged)</h3><p className="hint">Finite commands run on the merged result, separate from tests and the runtime server.</p><pre className="mono">{review.preview.config.smoke?.join("\n") || "No smoke checks configured"}</pre></div>
            <label className="check"><input type="checkbox" disabled={locked} checked={review.commands.deploys} onChange={(e) => edit({ ...review.commands, deploys: e.target.checked })} /> This repository deploys when main changes — enable release on merge (release.mode=on_merge). Not automatically detected.</label>
            <label className="check"><input type="checkbox" disabled={locked} checked={review.reviewed} onChange={(e) => update(key, { reviewed: e.target.checked })} /> I have reviewed setup, test and start commands, including any missing commands.</label>
            {review.status === "success" ? <button type="button" className="btn" disabled={readOnly || finishing} onClick={() => update(key, { status: "editing", reviewed: false })}>Edit saved configuration</button> : <button className="btn primary" disabled={locked || !review.reviewed || (!review.commands.start.trim() && !review.noStart)}>{review.status === "pending" ? "Saving…" : review.status === "error" ? "Retry save" : review.repoId ? "Save reviewed changes" : "Confirm & connect repository"}</button>}
          </form>
        </>}
        <div className="repository-diagnostics" aria-labelledby={`${prefix}-permissions`}>
          <h3 id={`${prefix}-permissions`}>GitHub permissions for {review.preview?.full_name || key}</h3>
          <button className="btn" disabled={readOnly || review.diagnosticBusy || finishing} onClick={() => diagnose(key, review.preview?.full_name || key)}>{review.diagnosticBusy ? "Checking permissions…" : "Retest GitHub permissions"}</button>
          {review.diagnosticError && <p role="alert">{review.diagnosticError}</p>}
          {review.diagnostic && <div aria-live="polite"><p>{review.diagnostic.detail}</p><ul>{(review.diagnostic.metadata?.permissions || []).map((p: { permission: string; required: string; state: string; instruction: string }) => <li key={p.permission}>{p.permission}: {p.required} — <strong>{p.state}</strong>{p.state !== "present" && <p>{p.instruction}</p>}</li>)}</ul></div>}
          <GitHubPermissions workspaceId={id} credential={review.diagnostic} />
          <p className="hint">You can replace the token in the GitHub onboarding step without visiting Settings.</p>
        </div>
      </section>;
    })}
    {error && <p role="alert">{error}</p>}
    {!readOnly && <button className="btn primary" disabled={!allSaved || repos.isLoading || repos.isError || finishing} onClick={complete}>{finishing ? "Refreshing workspace…" : "Continue with saved repositories"}</button>}
    {!allSaved && <p className="hint">Save every selected repository before continuing. A failure only affects that repository; other edits and successful saves are kept.</p>}
  </section>;
}
