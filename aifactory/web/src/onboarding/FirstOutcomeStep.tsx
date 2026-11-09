import { useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { api, type Outcome, type Repo } from "../api";
import { NewOutcome } from "../pages/NewOutcome";
import { useOutcomeRequestId } from "../outcomeDraft";
import { type OnboardingStepProps } from "./api";

export function outcomeExamples(repo: Repo) {
  const stack = repo.config.detected?.join(", ") || "";
  const context = stack ? `Detected stack: ${stack}. Use its existing conventions and test tools.`
    : "The stack has not been detected. Inspect the repository first; do not assume a framework or invent commands.";
  return [
    { title: `Document getting started with ${repo.full_name}`, brief: `${context} Improve the README with verified setup and check instructions. Mark anything that cannot be verified explicitly.` },
    { title: `Improve error handling in ${repo.full_name}`, brief: `${context} Find one user-facing error path, make its message actionable without exposing secrets, and add regression coverage.` },
    { title: `Add a regression test in ${repo.full_name}`, brief: `${context} Identify one important existing behavior with missing coverage and add a deterministic test without changing that behavior.` },
  ];
}

export function FirstOutcomeStep({ workspace, progress, readOnly, onComplete }: OnboardingStepProps) {
  const navigate = useNavigate();
  const repos = useQuery({ queryKey: ["repos", workspace.id], queryFn: () => api<Repo[]>(`/api/workspaces/${workspace.id}/repositories`) });
  const outcomes = useQuery({ queryKey: ["outcomes", workspace.id], queryFn: () => api<Outcome[]>(`/api/workspaces/${workspace.id}/outcomes`) });
  const [safeExample, setSafeExample] = useState(false);
  const [repoId, setRepoId] = useState("");
  const [title, setTitle] = useState("");
  const [brief, setBrief] = useState("");
  const [createdId, setCreatedId] = useState<string | null>(progress.first_outcome_id);
  const [busy, setBusy] = useState(false);
  const lock = useRef(false);
  const [error, setError] = useState("");
  const repo = repos.data?.find((r) => r.id === repoId) || repos.data?.[0];
  const draft = { title: title.trim(), brief, repo_ids: repo ? [repo.id] : [] };
  const requestId = useOutcomeRequestId(draft);
  const detail = (id: string) => `/w/${workspace.id}/outcomes/${id}`;
  const complete = async (id: string) => {
    await onComplete({ action: "complete", step: "outcome", first_outcome_id: id });
    navigate(detail(id));
  };
  const submit = async (existingId?: string) => {
    if (lock.current || readOnly) return;
    if (!existingId && !createdId && (!repo || title.trim().length < 3)) return;
    lock.current = true; setBusy(true); setError("");
    try {
      let id = existingId || createdId;
      if (!id) {
        const outcome = await api<Outcome>(`/api/workspaces/${workspace.id}/outcomes`, {
          method: "POST", json: { ...draft, request_id: requestId },
        });
        id = outcome.id;
        setCreatedId(id);
      }
      await complete(id);
    } catch (err) { setError((err as Error).message); }
    finally { lock.current = false; setBusy(false); }
  };
  return <section aria-label="First outcome">
    <p>Describe a small, useful result. The planner reads the repository and follows your saved approval policy.</p>
    {createdId ? <>
      <p>Your outcome has been created. If saving setup progress failed, retry below—this will not create another outcome.</p>
      <Link className="btn" to={detail(createdId)}>Open outcome</Link>{" "}
      <button className="btn primary" disabled={busy || readOnly} onClick={() => void submit(createdId)}>Retry progress and open outcome</button>
    </> : <>
      {repos.isError && <p role="alert">Could not load repositories. <button className="btn" onClick={() => repos.refetch()}>Retry repositories</button></p>}
      {repos.data?.length === 0 && <p>Connect a repository in the Repositories step first.</p>}
      <form onSubmit={(e) => { e.preventDefault(); void submit(); }}>
        <fieldset disabled={busy || readOnly || !repo}>
          <legend>Describe your first outcome</legend>
          <label className="field" htmlFor="first-repository">Repository<select id="first-repository" className="select" value={repo?.id || ""} onChange={(e) => setRepoId(e.target.value)}>{repos.data?.map((r) => <option key={r.id} value={r.id}>{r.full_name}</option>)}</select></label>
          {repo && <section aria-label="Safe README example">
            <h2>Safe README example</h2>
            <p>Add or improve CONTRIBUTING in this repository’s root README, using repository facts and preserving unrelated documentation. No code, dependency, workflow or other file changes.</p>
            <button className="btn" type="button" disabled={!progress.ready} onClick={() => setSafeExample(true)}>Try guided README example</button>
            {!progress.ready && <p>Finish AI models, GitHub and Repositories before starting the guided example.</p>}
          </section>}
          {repo && <><p>{repo.config.detected?.length ? `Detected stack: ${repo.config.detected.join(", ")}` : "Stack unknown: examples ask the planner to inspect the repository, not assume a framework."}</p>
            <p>Choose one of three starting points, then edit it. Examples never submit.</p>
            <div className="onboarding-examples">{outcomeExamples(repo).map((example) => <button className="btn" type="button" key={example.title} onClick={() => { setTitle(example.title); setBrief(example.brief); }}>{example.title}</button>)}</div></>}
          <button className="btn" type="button" onClick={() => { setTitle(""); setBrief(""); }}>Blank form</button>
          <label className="field" htmlFor="first-title">Outcome title<input id="first-title" className="input" required minLength={3} maxLength={300} value={title} onChange={(e) => setTitle(e.target.value)} /></label>
          <label className="field" htmlFor="first-description">Description<textarea id="first-description" className="textarea" value={brief} onChange={(e) => setBrief(e.target.value)} /></label>
        </fieldset>
        <button className="btn primary" disabled={busy || readOnly || !repo || title.trim().length < 3}>{busy ? "Creating and saving…" : "Create first outcome"}</button>
      </form>
      {!!outcomes.data?.length && <section aria-label="Recover an existing outcome"><h2>Already created an outcome?</h2>
        <p>These are server-saved workspace outcomes. Open one or use it to finish setup instead of creating another.</p>
        {outcomes.data.map((o) => <div className="onboarding-recovery" key={o.id}><Link to={detail(o.id)}>Open outcome #{o.number}: {o.title}</Link>
          <button className="btn" disabled={busy || readOnly} onClick={() => { setCreatedId(o.id); void submit(o.id); }}>Use this outcome</button></div>)}
      </section>}
      {outcomes.isError && <p role="alert">Could not check for previously created outcomes. <button className="btn" onClick={() => outcomes.refetch()}>Retry existing outcomes</button></p>}
    </>}
    {safeExample && repo && <NewOutcome initialRepository={repo.id} safeExample firstOutcome onClose={() => setSafeExample(false)} />}
    {error && <p role="alert">{error}</p>}
  </section>;
}
