import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, type CredentialsResponse, type LinearConfig, type LinearDiscovery, type LinearStage, type LinearState, type LinearTeam, type Repo } from "../api";
import { useWs } from "../App";
import { Skeleton, useToast } from "../ui";

const STAGES: LinearStage[] = ["queued", "building", "verifying", "merging", "done", "blocked"];
const emptyStates = (): LinearConfig["states"] => ({ queued: null, building: null, verifying: null, merging: null, done: null, blocked: null });

export function LinearSettings() {
  const { ws } = useWs();
  const config = useQuery({ queryKey: ["linear", ws.id], queryFn: () => api<LinearConfig>(`/api/workspaces/${ws.id}/linear`) });
  return (
    <section className="s-block">
      <h2>Linear</h2>
      <p>Import labelled issues and sync delivery status back to your team's board. Credentials stay on the control plane.</p>
      {config.isLoading ? <Skeleton rows={4} /> : config.error ? (
        <div role="alert">Could not load Linear configuration: {config.error.message} <button className="btn sm" onClick={() => config.refetch()}>Retry</button></div>
      ) : config.data && <LinearForm key={ws.id} saved={config.data} />}
    </section>
  );
}

function LinearForm({ saved }: { saved: LinearConfig }) {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const admin = ws.role === "owner" || ws.role === "admin";
  const [draft, setDraft] = useState(saved);
  const [busy, setBusy] = useState(false);
  const base = `/api/workspaces/${ws.id}`;
  const credentials = useQuery({ queryKey: ["credentials", ws.id], queryFn: () => api<CredentialsResponse>(`${base}/credentials`), enabled: admin });
  const configured = !!credentials.data?.items.some((credential) => credential.kind === "linear_api_key" && credential.configured);
  const repos = useQuery({ queryKey: ["repos", ws.id], queryFn: () => api<Repo[]>(`${base}/repositories`) });
  const teams = useQuery({ queryKey: ["linear-teams", ws.id], queryFn: () => api<LinearDiscovery<LinearTeam>>(`${base}/linear/teams`), enabled: admin && configured, retry: false });
  const states = useQuery({ queryKey: ["linear-states", ws.id, draft.team_id], queryFn: () => api<LinearDiscovery<LinearState>>(`${base}/linear/teams/${encodeURIComponent(draft.team_id!)}/states`), enabled: admin && configured && !!draft.team_id, retry: false });
  const teamItems = configured ? teams.data?.items || [] : [];
  const stateItems = configured ? states.data?.items || [] : [];
  const discoveryReady = configured && !teams.isFetching && !teams.isError && !states.isFetching && !states.isError
    && teamItems.some((team) => team.id === draft.team_id)
    && STAGES.every((stage) => stateItems.some((state) => state.id === draft.states[stage]));
  const valid = !!draft.intake_label.trim() && (!draft.enabled || (discoveryReady && !!draft.repo_ids.length
    && !!repos.data && draft.repo_ids.every((id) => repos.data.some((repo) => repo.id === id))));
  const save = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!admin || !valid || busy) return;
    setBusy(true);
    try {
      const result = await api<LinearConfig>(`${base}/linear`, { method: "PUT", json: draft });
      setDraft(result);
      qc.setQueryData(["linear", ws.id], result);
      await Promise.all([qc.invalidateQueries({ queryKey: ["linear", ws.id] }), qc.invalidateQueries({ queryKey: ["me"] })]);
      toast("Linear configuration saved");
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(false);
    }
  };
  return (
    <form onSubmit={save}>
      {!admin && <p>Only workspace admins can change Linear configuration.</p>}
      {admin && (credentials.isLoading ? <p>Checking Linear credentials…</p> : credentials.error ? (
        <p role="alert">Could not check credentials: {credentials.error.message} <button type="button" className="btn sm" onClick={() => credentials.refetch()}>Retry credentials</button></p>
      ) : !configured && <p role="alert">Add a Linear API key in <Link to={`/w/${ws.id}/settings/credentials`}>Credentials</Link> before enabling or discovering teams. You can still disable the integration.</p>)}
      <fieldset disabled={!admin || busy} style={{ border: 0, padding: 0, margin: 0 }}>
        <label className="check"><input type="checkbox" checked={draft.enabled} onChange={(event) => setDraft({ ...draft, enabled: event.target.checked })} /> Enable Linear intake and status sync</label>
        <div className="s-grid" style={{ marginTop: 16 }}>
          <label className="field"><span>Team</span>
            <select className="select" value={draft.team_id || ""} disabled={!configured || teams.isFetching || teams.isError} onChange={(event) => setDraft({ ...draft, team_id: event.target.value || null, states: emptyStates() })}>
              <option value="">Select a team</option>
              {draft.team_id && !teamItems.some((team) => team.id === draft.team_id) && <option value={draft.team_id}>{draft.team_id} (saved; unavailable)</option>}
              {teamItems.map((team) => <option key={team.id} value={team.id}>{team.name}{team.key ? ` (${team.key})` : ""}</option>)}
            </select>
          </label>
          <label className="field"><span>Intake label</span><input className="input" value={draft.intake_label} placeholder="factory-auto" required onChange={(event) => setDraft({ ...draft, intake_label: event.target.value })} /></label>
        </div>
        {admin && configured && (teams.isFetching ? <p>Loading Linear teams…</p> : teams.error ? (
          <p role="alert">Could not discover teams: {teams.error.message} <button type="button" className="btn sm" onClick={() => teams.refetch()}>Retry teams</button></p>
        ) : teamItems.length === 0 && <p>No Linear teams found. Check the API key's access.</p>)}
        <h3>Workflow states</h3>
        <p>Choose the team's actual state for each Factory stage. Changing teams clears all six selections.</p>
        {admin && configured && draft.team_id && (states.isFetching ? <p>Loading workflow states…</p> : states.error ? (
          <p role="alert">Could not discover workflow states: {states.error.message} <button type="button" className="btn sm" onClick={() => states.refetch()}>Retry states</button></p>
        ) : stateItems.length === 0 && <p>No workflow states found for this team.</p>)}
        <div className="s-grid">
          {STAGES.map((stage) => <label className="field" key={stage}><span>{stage[0].toUpperCase() + stage.slice(1)}</span>
            <select className="select" value={draft.states[stage] || ""} disabled={!configured || !draft.team_id || states.isFetching || states.isError} onChange={(event) => setDraft({ ...draft, states: { ...draft.states, [stage]: event.target.value || null } })}>
              <option value="">Select a workflow state</option>
              {draft.states[stage] && !stateItems.some((state) => state.id === draft.states[stage]) && <option value={draft.states[stage]!}>{draft.states[stage]} (saved; unavailable)</option>}
              {stateItems.map((state) => <option key={state.id} value={state.id}>{state.name} ({state.type})</option>)}
            </select>
          </label>)}
        </div>
        <h3>Allowed repositories</h3>
        <p>Linear work may only touch these connected workspace repositories.</p>
        {repos.isLoading ? <Skeleton rows={2} /> : repos.error ? <p role="alert">Could not load repositories: {repos.error.message} <button type="button" className="btn sm" onClick={() => repos.refetch()}>Retry repositories</button></p> : (
          repos.data?.length ? repos.data.map((repo) => <label className="check" key={repo.id}><input type="checkbox" checked={draft.repo_ids.includes(repo.id)} onChange={(event) => setDraft({ ...draft, repo_ids: event.target.checked ? [...draft.repo_ids, repo.id] : draft.repo_ids.filter((id) => id !== repo.id) })} /> {repo.full_name}</label>) : <p>Connect a repository in Settings → Repositories first.</p>
        )}
        {draft.repo_ids.filter((id) => repos.data && !repos.data.some((repo) => repo.id === id)).map((id) => <p role="alert" key={id}>Repository {id} is no longer connected. {admin && <button type="button" className="btn sm" onClick={() => setDraft({ ...draft, repo_ids: draft.repo_ids.filter((repoId) => repoId !== id) })}>Remove from allowlist</button>}</p>)}
        {admin && <div style={{ marginTop: 16 }}><button className="btn primary" disabled={!valid || busy}>{busy ? "Saving…" : "Save Linear settings"}</button>{draft.enabled && !valid && <p>Select an available team, all six states and at least one connected repository to enable Linear.</p>}</div>}
      </fieldset>
    </form>
  );
}
