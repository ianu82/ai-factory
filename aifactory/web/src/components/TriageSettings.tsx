import { useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type EffectiveGoals, type Goal, type Repo, type TriageConfig } from "../api";
import { useWs } from "../App";
import { useToast } from "../ui";

const defaults: TriageConfig = { enabled: true, interval_seconds: 1800, run_usd: 1, day_usd: 10, run_tokens: 100000, max_turns: 20 };
type GoalDraft = Omit<Goal, "id" | "workspace_id">;

export function TriageSettings({ save }: { save: (patch: { triage: TriageConfig }) => Promise<void> }) {
  const { ws, me } = useWs();
  const admin = ws.role === "owner" || ws.role === "admin";
  const toast = useToast();
  const qc = useQueryClient();
  const base = `/api/workspaces/${ws.id}`;
  const [config, setConfig] = useState({ ...defaults, ...ws.settings.triage });
  useEffect(() => setConfig({ ...defaults, ...ws.settings.triage }), [ws.settings.triage]);
  const [offset, setOffset] = useState(0);
  const [editing, setEditing] = useState<string | null>(null);
  const [draft, setDraft] = useState<GoalDraft | null>(null);
  const [busy, setBusy] = useState(false);
  const goals = useQuery({ queryKey: ["goals", ws.id, offset], queryFn: () => api<Goal[]>(`${base}/goals?state=all&limit=50&offset=${offset}`) });
  const effective = useQuery({ queryKey: ["triage-goals", ws.id], queryFn: () => api<EffectiveGoals>(`${base}/triage/goals`) });
  const repos = useQuery({ queryKey: ["repos", ws.id], queryFn: () => api<Repo[]>(`${base}/repositories`) });
  const members = useQuery({ queryKey: ["members", ws.id], queryFn: () => api<{ user: { id: string; name: string; email: string } }[]>(`${base}/members`) });
  const refresh = () => Promise.all([qc.invalidateQueries({ queryKey: ["goals", ws.id] }), qc.invalidateQueries({ queryKey: ["triage-goals", ws.id] })]);
  const mutate = async (body: Partial<GoalDraft>, id: string | null) => {
    if (!admin || busy) return;
    setBusy(true);
    try {
      await api(`${base}/goals${id ? `/${id}` : ""}`, { method: id ? "PATCH" : "POST", json: body });
      await refresh();
      setDraft(null);
      toast("Goal saved");
    } catch (error) { toast((error as Error).message, true); }
    finally { setBusy(false); }
  };
  const toggle = (ids: string[], id: string, on: boolean) => on ? [...ids, id] : ids.filter((v) => v !== id);
  return <>
    <section className="s-block">
      <h2>Goals / triage</h2>
      <p>Review signals against goals on a schedule. Automatic admission is off by default. P0 is most urgent; P3 is least urgent.</p>
      {!admin && <p>Only workspace admins can change goals and triage settings.</p>}
      <form onSubmit={async (event) => {
        event.preventDefault();
        if (!admin || busy) return;
        const valid = [config.run_usd, config.day_usd].every((n) => Number.isFinite(n) && n > 0)
          && [[config.interval_seconds, 31536000], [config.run_tokens, 6000000], [config.max_turns, 120]].every(([n, max]) => Number.isSafeInteger(n) && n > 0 && n <= max);
        if (!valid) { toast("Use positive finite budgets and bounded integer limits", true); return; }
        setBusy(true);
        try { await save({ triage: config }); } finally { setBusy(false); }
      }}>
        <fieldset disabled={!admin || busy} style={{ border: 0, margin: 0, padding: 0 }}>
          <label className="check"><input type="checkbox" checked={config.enabled} onChange={(e) => setConfig({ ...config, enabled: e.target.checked })} /> Enable scheduled triage (uncheck to turn off)</label>
          <div className="s-grid">
            {([
              ["interval_seconds", "Interval (seconds)", 31536000], ["run_usd", "Budget per run (USD)", undefined],
              ["day_usd", "Budget per day (USD)", undefined], ["run_tokens", "Maximum tokens per run", 6000000],
              ["max_turns", "Maximum turns per run", 120],
            ] as const).map(([key, label, max]) => <label className="field" key={key}><span>{label}</span>
              <input className="input" type="number" required min={max ? 1 : undefined} max={max} step={max ? 1 : "any"} value={Number.isNaN(config[key]) ? "" : config[key]} onChange={(e) => setConfig({ ...config, [key]: e.target.valueAsNumber })} />
            </label>)}
          </div>
          {admin && <button className="btn primary" disabled={busy}>Save triage settings</button>}
        </fieldset>
      </form>
    </section>
    <section className="s-block">
      <h2>Effective goals and policy</h2>
      {effective.isPending ? <p role="status">Loading effective policy…</p> : effective.error ? <p role="alert">{effective.error.message} <button className="btn sm" onClick={() => effective.refetch()}>Retry</button></p> : <>
        <p>Goal source: <b>{effective.data?.source}</b></p>
        {effective.data?.goals.map((g) => <div key={g.id} className="s-line"><div className="l"><b>{g.name}</b><span>{g.description}</span><span>Policy: {g.effective_admission.source} · {g.effective_admission.enabled ? "Automatic admission" : "Human review"} · maximum urgency P{g.effective_admission.max_urgency}{g.effective_admission.invalid ? " · unknown policy denied" : ""}</span><span>Admission scope: {g.effective_admission.repo_ids.map((id) => repos.data?.find((r) => r.id === id)?.full_name || id).join(", ") || "None"}</span></div></div>)}
        {effective.data?.goals.length === 0 && <p>No active goals selected.</p>}
      </>}
    </section>
    <section className="s-block">
      <h2>Workspace goals</h2>
      <p>Workspace goals are the fallback when no active workstreams contribute. Attached goals keep their identity.</p>
      {goals.error && <p role="alert">{goals.error.message} <button className="btn sm" onClick={() => goals.refetch()}>Retry</button></p>}
      {goals.isPending && <p role="status">Loading goals…</p>}
      {goals.data?.map((goal) => <div className="s-line" key={goal.id}>
        <div className="l"><b>{goal.name} {goal.state === "archived" ? "(archived)" : ""}</b><span>{goal.description}</span>{goal.workstream_id && <span>Workstream: {goal.workstream_id}</span>}</div>
        {admin && <div><button className="btn sm" disabled={busy} onClick={() => { setEditing(goal.id); const { id: _id, workspace_id: _ws, ...body } = goal; setDraft(body); }}>Edit</button>
          {goal.state === "active" && <button className="btn sm" disabled={busy} onClick={() => mutate({ state: "archived" }, goal.id)}>Archive</button>}</div>}
      </div>)}
      <div style={{ display: "flex", gap: 8 }}>
        <button className="btn sm" disabled={offset === 0 || goals.isFetching} onClick={() => setOffset(Math.max(0, offset - 50))}>Previous</button>
        <button className="btn sm" disabled={goals.data?.length !== 50 || goals.isFetching} onClick={() => setOffset(offset + 50)}>Next</button>
        {admin && <button className="btn primary" disabled={busy} onClick={() => { setEditing(null); setDraft({ name: "", description: "", owner_id: me.user.id, repo_ids: [], admission: { enabled: false, repo_ids: [], max_urgency: 3 }, state: "active", workstream_id: null }); }}>Add goal</button>}
      </div>
      {draft && admin && <form onSubmit={(e) => { e.preventDefault(); mutate(draft, editing); }}>
        <fieldset disabled={busy} style={{ border: 0, padding: 0 }}>
          <h3>{editing ? "Edit goal" : "New goal"}</h3>
          <label className="field"><span>Name</span><input className="input" required maxLength={200} value={draft.name} onChange={(e) => setDraft({ ...draft, name: e.target.value })} /></label>
          <label className="field"><span>Description</span><textarea className="textarea" required maxLength={4000} value={draft.description} onChange={(e) => setDraft({ ...draft, description: e.target.value })} /></label>
          <label className="field"><span>Owner</span><select className="select" required value={draft.owner_id} onChange={(e) => setDraft({ ...draft, owner_id: e.target.value })}>
            <option value="">Select a workspace member</option>
            {members.data?.map(({ user }) => <option key={user.id} value={user.id}>{user.name || user.email}</option>)}
          </select></label>
          {(members.error || repos.error) && <p role="alert">Unable to load members or repositories. Reload before saving.</p>}
          <b>Goal repositories</b>
          {repos.data?.map((repo) => <label className="check" key={repo.id}><input type="checkbox" checked={draft.repo_ids.includes(repo.id)} onChange={(e) => setDraft({ ...draft, repo_ids: toggle(draft.repo_ids, repo.id, e.target.checked), admission: { ...draft.admission, repo_ids: e.target.checked ? draft.admission.repo_ids : draft.admission.repo_ids.filter((id) => id !== repo.id) } })} /> {repo.full_name}</label>)}
          {draft.repo_ids.filter((id) => !repos.data?.some((repo) => repo.id === id)).map((id) => <label className="check" key={id}><input type="checkbox" checked onChange={() => setDraft({ ...draft, repo_ids: draft.repo_ids.filter((v) => v !== id), admission: { ...draft.admission, repo_ids: draft.admission.repo_ids.filter((v) => v !== id) } })} /> {id} (unavailable; uncheck to remove)</label>)}
          <label className="check"><input type="checkbox" checked={draft.admission.enabled} onChange={(e) => setDraft({ ...draft, admission: { ...draft.admission, enabled: e.target.checked } })} /> Admit triaged work automatically</label>
          <p>When an admission-level policy is installed, it takes precedence over this toggle. It cannot expand the goal repositories.</p>
          <b>Explicit automatic admission allowlist</b>
          {repos.data?.filter((r) => draft.repo_ids.includes(r.id)).map((repo) => <label className="check" key={repo.id}><input type="checkbox" checked={draft.admission.repo_ids.includes(repo.id)} onChange={(e) => setDraft({ ...draft, admission: { ...draft.admission, repo_ids: toggle(draft.admission.repo_ids, repo.id, e.target.checked) } })} /> {repo.full_name}</label>)}
          <label className="field"><span>Maximum urgency admitted automatically</span><select className="select" value={draft.admission.max_urgency} onChange={(e) => setDraft({ ...draft, admission: { ...draft.admission, max_urgency: Number(e.target.value) } })}>
            {[0, 1, 2, 3].map((n) => <option key={n} value={n}>P{n} {n === 0 ? "(all priorities)" : `and less urgent`}</option>)}
          </select></label>
          <button className="btn primary" disabled={busy || !!members.error || !!repos.error || !members.data || !repos.data || (draft.state === "active" && !draft.repo_ids.length)}>Save goal</button>
          <button className="btn ghost" type="button" onClick={() => setDraft(null)}>Cancel</button>
        </fieldset>
      </form>}
    </section>
  </>;
}
