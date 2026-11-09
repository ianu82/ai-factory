import { useEffect, useMemo, useRef, useState } from "react";
import { useInfiniteQuery, useQuery, useQueryClient } from "@tanstack/react-query";
import { NavLink, useParams } from "react-router-dom";
import { ChevronDown, ChevronRight, Trash2 } from "lucide-react";
import { api, type Credential, type ModelCheckResult, type Repo, type Settings, type Workspace } from "../api";
import { useWs } from "../App";
import { connectReviewedRepository, previewRepository, repositoryDiscoveryUrl, type RepositoryPreview } from "../repositoryConnect";
import { ago } from "../format";
import { onboardingQueryKey } from "../onboarding/api";
import { Avatar, Dot, Skeleton, useToast } from "../ui";
import { LinearSettings } from "../components/LinearSettings";

const SECTIONS = [
  ["policies", "Autonomy & limits"],
  ["models", "Models & workers"],
  ["credentials", "Credentials"],
  ["linear", "Linear"],
  ["repositories", "Repositories"],
  ["members", "Members"],
  ["workspace", "Workspace"],
] as const;

export function SettingsPage() {
  const { section = "policies" } = useParams();
  const { ws } = useWs();
  return (
    <>
      <div className="page-head"><h1>Settings</h1><span className="muted" style={{ fontSize: 12.5 }}>{ws.name}</span></div>
      <div className="page-body">
        <div className="settings">
          <nav aria-label="Settings sections">
            {SECTIONS.map(([key, label]) => <NavLink key={key} to={`/w/${ws.id}/settings/${key}`} className={section === key ? "active" : ""}>{label}</NavLink>)}
          </nav>
          <div>
            {section === "policies" && <Policies />}
            {section === "models" && <Models key={ws.id} />}
            {section === "credentials" && <Credentials />}
            {section === "linear" && <LinearSettings key={ws.id} />}
            {section === "repositories" && <Repositories />}
            {section === "members" && <Members />}
            {section === "workspace" && <WorkspaceSection />}
          </div>
        </div>
      </div>
    </>
  );
}

function useSaveSettings() {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  return async (patch: Partial<Settings> | { name?: string; paused?: boolean; description?: string }, isSettings = true) => {
    try {
      await api<Workspace>(`/api/workspaces/${ws.id}`, { method: "PATCH", json: isSettings ? { settings: patch } : patch });
      await qc.invalidateQueries({ queryKey: ["me"] });
      toast("Saved");
    } catch (err) {
      toast((err as Error).message, true);
    }
  };
}

function Seg<T extends string>({ value, options, onChange, disabled }: { value: T; options: [T, string][]; onChange: (v: T) => void; disabled?: boolean }) {
  return (
    <div className="seg" role="radiogroup">
      {options.map(([v, label]) => (
        <button key={v} role="radio" aria-checked={value === v} className={value === v ? "on" : ""} disabled={disabled} onClick={() => onChange(v)}>{label}</button>
      ))}
    </div>
  );
}

function Policies() {
  const { ws } = useWs();
  const save = useSaveSettings();
  const s = ws.settings;
  const admin = ws.role === "owner" || ws.role === "admin";
  const [nums, setNums] = useState({ concurrency: s.concurrency, max_attempts: s.max_attempts, auto_retry_rounds: s.auto_retry_rounds ?? 1, run_usd: s.budgets.run_usd, outcome_usd: s.budgets.outcome_usd });
  useEffect(() => setNums({ concurrency: s.concurrency, max_attempts: s.max_attempts, auto_retry_rounds: s.auto_retry_rounds ?? 1, run_usd: s.budgets.run_usd, outcome_usd: s.budgets.outcome_usd }), [s]);
  const autonomy = (key: keyof Settings["autonomy"], v: "auto" | "approval") => save({ autonomy: { ...s.autonomy, [key]: v } });
  return (
    <>
      <section className="s-block">
        <h2>When does a person step in?</h2>
        <p>Everything else runs unattended. Every approval is recorded with who gave it.</p>
        <div className="s-line"><div className="l"><b>Plans</b><span>Start building as soon as the planner finishes, or wait for approval.</span></div>
          <Seg value={s.autonomy.plan} options={[["auto", "Automatic"], ["approval", "Approve first"]]} onChange={(v) => autonomy("plan", v)} disabled={!admin} /></div>
        <div className="s-line"><div className="l"><b>Merges</b><span>Merge verified PRs once CI passes, or ask a person.</span></div>
          <Seg value={s.autonomy.merge} options={[["auto", "Automatic"], ["approval", "Approve first"]]} onChange={(v) => autonomy("merge", v)} disabled={!admin} /></div>
        <div className="s-line"><div className="l"><b>Releases</b><span>For repositories that deploy when main changes, require approval before merging.</span></div>
          <Seg value={s.autonomy.release} options={[["auto", "Automatic"], ["approval", "Approve first"]]} onChange={(v) => autonomy("release", v)} disabled={!admin} /></div>
      </section>
      <section className="s-block">
        <h2>Throughput and limits</h2>
        <p>Independent tasks run side by side up to the concurrency limit. Merges into the same repository are serialized and revalidated against the latest main.</p>
        <div className="s-grid">
          <label className="field"><span>Tasks in parallel</span><input className="input num" type="number" min={1} max={50} value={nums.concurrency} disabled={!admin} onChange={(e) => setNums({ ...nums, concurrency: Number(e.target.value) })} /><span className="hint">1–50</span></label>
          <label className="field"><span>Attempts per task</span><input className="input num" type="number" min={1} max={8} value={nums.max_attempts} disabled={!admin} onChange={(e) => setNums({ ...nums, max_attempts: Number(e.target.value) })} /><span className="hint">Failed checks or verification feed the next attempt</span></label>
          <label className="field"><span>Automatic retry rounds</span><input className="input num" type="number" min={0} max={3} value={nums.auto_retry_rounds} disabled={!admin} onChange={(e) => setNums({ ...nums, auto_retry_rounds: Number(e.target.value) })} /><span className="hint">When attempts run out on concrete findings, start a fresh set before asking you (0–3)</span></label>
          <label className="field"><span>Budget per agent session (USD)</span><input className="input num" type="number" min={0.5} step={0.5} value={nums.run_usd} disabled={!admin} onChange={(e) => setNums({ ...nums, run_usd: Number(e.target.value) })} /></label>
          <label className="field"><span>Budget per outcome (USD)</span><input className="input num" type="number" min={1} step={1} value={nums.outcome_usd} disabled={!admin} onChange={(e) => setNums({ ...nums, outcome_usd: Number(e.target.value) })} /><span className="hint">Work pauses for a decision when reached</span></label>
        </div>
        {admin && <div style={{ marginTop: 14 }}><button className="btn primary" onClick={() => save({ concurrency: nums.concurrency, max_attempts: nums.max_attempts, auto_retry_rounds: nums.auto_retry_rounds, budgets: { ...s.budgets, run_usd: nums.run_usd, outcome_usd: nums.outcome_usd } })}>Save limits</button></div>}
      </section>
    </>
  );
}

function Models() {
  const { ws, meta } = useWs();
  const save = useSaveSettings();
  const s = ws.settings;
  const admin = ws.role === "owner" || ws.role === "admin";
  const models = useQuery({ queryKey: ["models", ws.id], queryFn: () => api<{ provider: string | null; label?: string; models: string[]; error?: string }>(`/api/workspaces/${ws.id}/models`) });
  const [m, setM] = useState(s.models);
  useEffect(() => setM(s.models), [s.models]);
  const fingerprint = JSON.stringify([ws.id, s.provider, s.models]);
  const dirty = JSON.stringify(m) !== JSON.stringify(s.models);
  const generation = useRef(0);
  const [check, setCheck] = useState<{ fingerprint: string; result: ModelCheckResult }>();
  const [checking, setChecking] = useState(false);
  const [checkError, setCheckError] = useState("");
  const invalidateCheck = () => {
    generation.current += 1;
    setCheck(undefined);
    setChecking(false);
    setCheckError("");
  };
  useEffect(() => { invalidateCheck(); }, [fingerprint]);
  const checkModels = async () => {
    const request = ++generation.current;
    setChecking(true);
    setCheck(undefined);
    setCheckError("");
    try {
      const result = await api<ModelCheckResult>(`/api/workspaces/${ws.id}/models/check`, { method: "POST" });
      if (request === generation.current) setCheck({ fingerprint, result });
    } catch (err) {
      if (request === generation.current) setCheckError((err as Error).message);
    } finally {
      if (request === generation.current) setChecking(false);
    }
  };
  const currentCheck = !dirty && check?.fingerprint === fingerprint ? check.result : undefined;
  const options = useMemo(() => Array.from(new Set([...(models.data?.models || []), m.default || "", m.planner || "", m.builder || "", m.verifier || ""].filter(Boolean))), [models.data, m]);
  const harness = (role: keyof Settings["harness"], value: string) => save({ harness: { ...s.harness, [role]: value } });
  return (
    <>
      <section className="s-block">
        <h2>Models</h2>
        <p>{models.data?.provider ? `Using ${models.data.label}. ` : ""}Each role is pinned to an exact model; the Factory never silently substitutes another one.</p>
        {models.data?.error && <div className="err" style={{ marginBottom: 10 }}>{models.data.error}</div>}
        <div className="s-grid">
          {(["default", "planner", "builder", "verifier"] as const).map((role) => (
            <label key={role} className="field">
              <span>{role === "default" ? "Default model" : `${role[0].toUpperCase()}${role.slice(1)}`}</span>
              <select className="select" value={m[role] || ""} disabled={!admin} onChange={(e) => { invalidateCheck(); setM({ ...m, [role]: e.target.value || undefined }); }}>
                {role !== "default" && <option value="">Use default</option>}
                {options.map((o) => <option key={o} value={o}>{o}</option>)}
              </select>
              {role !== "default" && <span className="hint" role="status">
                <Dot tone={currentCheck?.roles[role].status === "supported" ? "good" : currentCheck?.roles[role].status === "unsupported" ? "bad" : currentCheck ? "warn" : "idle"} />{" "}
                {checking && !dirty ? "Checking image input…" : currentCheck ? <>
                  {currentCheck.roles[role].model} · Images: {currentCheck.roles[role].status === "unknown" ? "unknown (check failed)" : currentCheck.roles[role].status}
                  <br />{currentCheck.roles[role].explanation}
                </> : dirty ? "Images: not checked — save models first" : "Images: not checked"}
              </span>}
            </label>
          ))}
        </div>
        <p>Checks send one tiny image per distinct saved model and may incur a small provider charge.</p>
        {checkError && <div className="err" role="alert">{checkError}</div>}
        {admin && <div style={{ marginTop: 14, display: "flex", gap: 8 }}>
          <button className="btn primary" onClick={() => { invalidateCheck(); void save({ models: m }); }}>Save models</button>
          <button className="btn" disabled={checking || dirty} onClick={checkModels}>{checking ? "Checking…" : "Check models"}</button>
          <Seg value={s.provider} options={[["mindshub", "MindsHub Inference"], ["openai", "OpenAI"]]} onChange={(v) => { invalidateCheck(); void save({ provider: v }); }} />
        </div>}
      </section>
      <section className="s-block">
        <h2>Workers</h2>
        <p>Native, Codex and Anton run the same workflow and are held to the same acceptance. The verifier is always a fresh session, never the builder.</p>
        {(["planner", "builder", "verifier"] as const).map((role) => (
          <div key={role} className="s-line">
            <div className="l"><b>{role[0].toUpperCase() + role.slice(1)}</b><span>{role === "verifier" ? "Independent QA of every criterion" : role === "planner" ? "Reads the code and splits the outcome into tasks" : "Implements each task"}</span></div>
            <select className="select" style={{ width: 200 }} value={s.harness[role]} disabled={!admin} onChange={(e) => harness(role, e.target.value)}>
              {meta.harnesses.map((h) => <option key={h.name} value={h.name} disabled={!h.available}>{h.label}{h.available ? "" : " — unavailable"}</option>)}
            </select>
          </div>
        ))}
        <ul style={{ margin: "12px 0 0", paddingLeft: 0, listStyle: "none", fontSize: 12.5, color: "var(--muted)", display: "grid", gap: 4 }}>
          {meta.harnesses.map((h) => <li key={h.name} style={{ display: "flex", gap: 8, alignItems: "center" }}><Dot tone={h.available ? "good" : "idle"} /> <b style={{ color: "var(--ink)", fontWeight: 550 }}>{h.label}</b> {h.detail}</li>)}
          <li style={{ display: "flex", gap: 8, alignItems: "center" }}><Dot tone="idle" /> Sandbox isolation: {meta.sandbox.isolation}</li>
        </ul>
      </section>
    </>
  );
}

const CRED_HELP: Record<string, { what: string; placeholder: string }> = {
  mindshub_api_key: { what: "Powers every planner, builder and verifier session. Create a key in MindsHub console → API keys.", placeholder: "mdb_…" },
  github_token: { what: "Fine-grained token with Contents, Pull requests and Commit statuses (read & write) on the repositories this workspace may change.", placeholder: "github_pat_…" },
  openai_api_key: { what: "Optional alternative provider.", placeholder: "sk-…" },
  linear_api_key: { what: "Personal Linear API key. Used only by the control plane, never sent to sandboxes. After saving, choose your team and workflow states in Settings → Linear.", placeholder: "lin_api_…" },
};

function Credentials() {
  const { ws } = useWs();
  const q = useQuery({ queryKey: ["credentials", ws.id], queryFn: () => api<{ items: Credential[]; server_fallbacks: Record<string, boolean> }>(`/api/workspaces/${ws.id}/credentials`) });
  return (
    <section className="s-block">
      <h2>Credentials</h2>
      <p>Stored encrypted on the Factory server. Values are never shown again, never sent to sandboxes, and never appear in logs, PRs or evidence. Agents reach models through a run-scoped gateway token.</p>
      {q.isLoading ? <Skeleton rows={3} /> : q.error ? <p role="alert">Could not load credentials: {q.error.message} <button className="btn sm" onClick={() => q.refetch()}>Retry</button></p> : q.data?.items.map((c) => <CredentialRow key={`${ws.id}-${c.kind}`} c={c} fallback={q.data!.server_fallbacks[c.kind]} />)}
    </section>
  );
}

function CredentialRow({ c, fallback }: { c: Credential; fallback: boolean }) {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const [value, setValue] = useState("");
  const [editing, setEditing] = useState(!c.configured);
  const [busy, setBusy] = useState(false);
  const admin = ws.role === "owner" || ws.role === "admin";
  const refresh = () => Promise.all([
    qc.invalidateQueries({ queryKey: ["credentials", ws.id] }),
    qc.invalidateQueries({ queryKey: ["models", ws.id] }),
    qc.invalidateQueries({ queryKey: ["linear-teams", ws.id] }),
    qc.invalidateQueries({ queryKey: ["linear-states", ws.id] }),
    qc.invalidateQueries({ queryKey: onboardingQueryKey(ws.id) }),
  ]);
  const saveKey = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    try {
      const r = await api<Credential>(`/api/workspaces/${ws.id}/credentials/${c.kind}`, { method: "PUT", json: { value } });
      setValue("");
      setEditing(false);
      toast(r.status === "valid" ? `${c.label} connected` : `${c.label} saved, but the test failed: ${r.detail}`, r.status !== "valid");
      await refresh();
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setValue("");
      setBusy(false);
    }
  };
  const test = async () => {
    setBusy(true);
    try {
      const r = await api<Credential>(`/api/workspaces/${ws.id}/credentials/${c.kind}/test`, { method: "POST" });
      toast(r.status === "valid" ? r.detail || "Working" : r.detail || "Test failed", r.status !== "valid");
      await refresh();
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(false);
    }
  };
  const remove = async () => {
    if (!confirm(`Remove the ${c.label} credential? Running work using it will fail.`)) return;
    setBusy(true);
    try {
      await api(`/api/workspaces/${ws.id}/credentials/${c.kind}`, { method: "DELETE" });
      setValue("");
      setEditing(true);
      await refresh();
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(false);
    }
  };
  const tone = c.status === "valid" ? "good" : c.status === "invalid" ? "bad" : c.configured ? "warn" : "idle";
  return (
    <div className="cred">
      <div className="cred-top">
        <Dot tone={tone} />
        <b>{c.label}</b>
        {c.configured && <span className="masked">{c.hint}</span>}
        <span style={{ flex: 1 }} />
        {c.configured && admin && !editing && (
          <>
            <button className="btn sm ghost" onClick={test} disabled={busy}>Test</button>
            <button className="btn sm ghost" onClick={() => setEditing(true)} disabled={busy}>Replace</button>
            <button className="icon-btn" onClick={remove} disabled={busy} aria-label={`Remove ${c.label}`}><Trash2 /></button>
          </>
        )}
      </div>
      <div className="muted" style={{ fontSize: 12.5, marginTop: 6 }}>
        {c.configured ? (
          <span style={{ color: c.status === "invalid" ? "var(--bad)" : undefined }}>{c.detail || c.status}{c.tested_at ? ` · checked ${ago(c.tested_at)}` : ""}</span>
        ) : fallback ? "Using the server's bootstrap credential until you add one for this workspace." : CRED_HELP[c.kind]?.what}
      </div>
      {editing && admin && (
        <form onSubmit={saveKey}>
          <input className="input mono" type="password" autoComplete="off" spellCheck={false} disabled={busy} placeholder={CRED_HELP[c.kind]?.placeholder}
            value={value} onChange={(e) => setValue(e.target.value)} aria-label={`${c.label} value`} />
          <button className="btn primary" disabled={busy || value.trim().length < 8}>{busy ? "Testing…" : "Save & test"}</button>
          {c.configured && <button type="button" className="btn ghost" onClick={() => { setEditing(false); setValue(""); }}>Cancel</button>}
        </form>
      )}
    </div>
  );
}

function Repositories() {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const admin = ws.role === "owner" || ws.role === "admin";
  const repos = useQuery({ queryKey: ["repos", ws.id], queryFn: () => api<Repo[]>(`/api/workspaces/${ws.id}/repositories`) });
  const [adding, setAdding] = useState(false);
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [search, setSearch] = useState("");
  const [review, setReview] = useState<RepositoryPreview | null>(null);
  const [commands, setCommands] = useState({ setup: "", checks: "", start: "", deploys: false });
  useEffect(() => {
    const timer = setTimeout(() => setSearch(name), 300);
    return () => clearTimeout(timer);
  }, [name]);
  const available = useInfiniteQuery({
    queryKey: ["github-repos", ws.id, search], enabled: adding && !review && admin,
    initialPageParam: 1,
    queryFn: ({ pageParam }) => api<{ items: { full_name: string; private: boolean; can_push: boolean; description: string }[]; error?: string; next_page: number | null }>(repositoryDiscoveryUrl(ws.id, search, pageParam)),
    getNextPageParam: (page) => page.next_page ?? undefined,
  });
  const preview = async (full: string) => {
    setBusy(true);
    try {
      const result = await previewRepository(ws.id, full, api);
      setReview(result);
      setCommands({ setup: lines(result.config.setup), checks: lines(result.config.checks), start: result.config.qa?.start || "", deploys: false });
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(false);
    }
  };
  const connect = async () => {
    if (!review) return;
    setBusy(true);
    try {
      await connectReviewedRepository(ws.id, review, commands, api);
      toast(`Connected ${review.full_name}`);
      setName("");
      setReview(null);
      setAdding(false);
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["repos", ws.id] }),
        qc.invalidateQueries({ queryKey: onboardingQueryKey(ws.id) }),
      ]);
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(false);
    }
  };
  const connected = new Set((repos.data || []).map((r) => r.full_name));
  const matches = [...new Map((available.data?.pages.flatMap((page) => page.items) || []).map((r) => [r.full_name, r])).values()].filter((r) => !connected.has(r.full_name));
  return (
    <section className="s-block">
      <div style={{ display: "flex", alignItems: "center" }}>
        <h2 style={{ flex: 1 }}>Repositories</h2>
        {admin && !adding && <button className="btn sm primary" onClick={() => setAdding(true)}>Add repository</button>}
      </div>
      <p>The Factory only changes repositories connected here. Setup and checks run in a fresh sandbox for every attempt; smoke runs on the merged result.</p>
      {adding && (
        <div className="cred" style={{ marginBottom: 12 }}>
          {review ? (
            <form style={{ display: "grid" }} onSubmit={(e) => { e.preventDefault(); connect(); }}>
              <h3 style={{ overflowWrap: "anywhere" }}>Review {review.full_name} · {review.default_branch}</h3>
              <p>{(review.config.detected || []).join(", ") || "Stack unknown"}. Review commands before connecting.</p>
              <div className="s-grid">
                <label className="field"><span>Setup commands</span><textarea className="textarea mono" value={commands.setup} disabled={busy} onChange={(e) => setCommands({ ...commands, setup: e.target.value })} /></label>
                <label className="field"><span>Check commands</span><textarea className="textarea mono" value={commands.checks} disabled={busy} onChange={(e) => setCommands({ ...commands, checks: e.target.value })} /></label>
              </div>
              <label className="field"><span>Runtime QA start command</span><textarea className="textarea mono" value={commands.start} disabled={busy} onChange={(e) => setCommands({ ...commands, start: e.target.value })} /><span className="hint">{review.config.start_detection?.reason || "Start command unknown."} Leave blank to disable runtime QA. Must listen on $PORT.</span></label>
              <label className="check"><input type="checkbox" checked={commands.deploys} disabled={busy} onChange={(e) => setCommands({ ...commands, deploys: e.target.checked })} /> This repository deploys when main changes (not automatically detected)</label>
              <button className="btn primary" disabled={busy}>{busy ? "Connecting…" : "Confirm & connect"}</button>
              <button type="button" className="btn ghost" disabled={busy} onClick={() => setReview(null)}>Back</button>
            </form>
          ) : (
            <>
              <form onSubmit={(e) => { e.preventDefault(); if (name.includes("/")) preview(name); }} style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 0 }}>
                <input className="input" aria-label="Repository name or search" autoFocus placeholder="owner/name or search your repositories" value={name} disabled={busy} onChange={(e) => setName(e.target.value)} />
                <button className="btn primary" disabled={busy || !name.includes("/")}>{busy ? "Detecting…" : "Preview"}</button>
                <button type="button" className="btn ghost" disabled={busy} onClick={() => setAdding(false)}>Cancel</button>
              </form>
              {(available.data?.pages.find((page) => page.error)?.error || available.error) && <div className="err" role="alert" style={{ marginTop: 8 }}>{available.data?.pages.find((page) => page.error)?.error || available.error?.message}</div>}
              {available.isLoading && <div role="status">Finding repositories…</div>}
              <ul className="rows" style={{ marginTop: 8 }}>
                {matches.map((r) => (
                  <li key={r.full_name}>
                    <button type="button" className="btn ghost" style={{ width: "100%", textAlign: "left", whiteSpace: "normal", overflowWrap: "anywhere" }} disabled={busy || !r.can_push} onClick={() => preview(r.full_name)}>
                      {r.full_name} · {r.can_push ? (r.private ? "private" : "public") : "read-only access"}
                    </button>
                  </li>
                ))}
              </ul>
              {available.hasNextPage && <button className="btn ghost" disabled={available.isFetchingNextPage || busy} onClick={() => available.fetchNextPage()}>{available.isFetchingNextPage ? "Loading…" : "Load more repositories"}</button>}
            </>
          )}
        </div>
      )}
      {repos.isLoading ? <Skeleton rows={2} /> : (repos.data || []).length === 0 && !adding ? <div className="muted">No repositories yet.</div> :
        (repos.data || []).map((r) => <RepoCard key={r.id} repo={r} admin={admin} />)}
    </section>
  );
}

const lines = (xs?: string[]) => (xs || []).join("\n");
const unlines = (s: string) => s.split("\n").map((x) => x.trim()).filter(Boolean);

function RepoCard({ repo, admin }: { repo: Repo; admin: boolean }) {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const [open, setOpen] = useState(false);
  const c = repo.config;
  const [form, setForm] = useState({
    setup: lines(c.setup), checks: lines(c.checks), smoke: lines(c.smoke), protected_paths: lines(c.protected_paths),
    context: c.context || "", merge_mode: c.merge?.mode || "", method: c.merge?.method || "squash",
    health_url: c.release?.health_url || "", release_mode: c.release?.mode || "", require_ci: !!c.require_ci,
  });
  const save = async () => {
    try {
      await api(`/api/workspaces/${ws.id}/repositories/${repo.id}`, { method: "PATCH", json: { config: {
        setup: unlines(form.setup), checks: unlines(form.checks), smoke: unlines(form.smoke), protected_paths: unlines(form.protected_paths),
        context: form.context, require_ci: form.require_ci,
        merge: { mode: form.merge_mode || undefined, method: form.method, revalidate: true },
        release: { mode: form.release_mode || undefined, health_url: form.health_url || undefined },
      } } });
      toast("Repository saved");
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["repos", ws.id] }),
        qc.invalidateQueries({ queryKey: onboardingQueryKey(ws.id) }),
      ]);
    } catch (err) {
      toast((err as Error).message, true);
    }
  };
  const remove = async () => {
    if (!confirm(`Disconnect ${repo.full_name}? Past outcomes keep their history.`)) return;
    try {
      await api(`/api/workspaces/${ws.id}/repositories/${repo.id}`, { method: "DELETE" });
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["repos", ws.id] }),
        qc.invalidateQueries({ queryKey: onboardingQueryKey(ws.id) }),
      ]);
    } catch (err) {
      toast((err as Error).message, true);
    }
  };
  return (
    <div className="repo">
      <div className="repo-head" onClick={() => setOpen(!open)} style={{ cursor: "pointer" }}>
        {open ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
        <b>{repo.full_name}</b>
        <span className="muted mono">{repo.default_branch}</span>
        <span style={{ flex: 1 }} />
        <span className="muted" style={{ fontSize: 12.5 }}>{(c.checks || []).length} checks · {(c.detected || []).join(", ")}</span>
      </div>
      {open && (
        <div className="repo-body">
          <div className="s-grid">
            <label className="field"><span>Setup</span><textarea className="textarea mono" value={form.setup} disabled={!admin} onChange={(e) => setForm({ ...form, setup: e.target.value })} /><span className="hint">One command per line, run before each attempt</span></label>
            <label className="field"><span>Checks</span><textarea className="textarea mono" value={form.checks} disabled={!admin} onChange={(e) => setForm({ ...form, checks: e.target.value })} /><span className="hint">Must pass before independent verification</span></label>
            <label className="field"><span>Smoke after merge</span><textarea className="textarea mono" value={form.smoke} disabled={!admin} onChange={(e) => setForm({ ...form, smoke: e.target.value })} /></label>
            <label className="field"><span>Protected paths</span><textarea className="textarea mono" value={form.protected_paths} disabled={!admin} placeholder="lib/payments/**" onChange={(e) => setForm({ ...form, protected_paths: e.target.value })} /><span className="hint">CI workflows, .env and keys are always protected</span></label>
          </div>
          <label className="field"><span>Team notes for agents</span><textarea className="textarea" value={form.context} disabled={!admin} placeholder="Architecture, conventions, gotchas…" onChange={(e) => setForm({ ...form, context: e.target.value })} /></label>
          <div className="s-grid">
            <label className="field"><span>Merge</span>
              <select className="select" value={form.merge_mode} disabled={!admin} onChange={(e) => setForm({ ...form, merge_mode: e.target.value })}>
                <option value="">Workspace policy ({ws.settings.autonomy.merge === "auto" ? "automatic" : "approval"})</option>
                <option value="auto">Always automatic</option><option value="approval">Always ask</option>
              </select></label>
            <label className="field"><span>Merge method</span>
              <select className="select" value={form.method} disabled={!admin} onChange={(e) => setForm({ ...form, method: e.target.value })}>
                <option value="squash">Squash</option><option value="merge">Merge commit</option><option value="rebase">Rebase</option>
              </select></label>
            <label className="field"><span>Releases</span>
              <select className="select" value={form.release_mode} disabled={!admin} onChange={(e) => setForm({ ...form, release_mode: e.target.value })}>
                <option value="">Not managed by the Factory</option><option value="on_merge">Deploys when main changes</option>
              </select></label>
            <label className="field"><span>Health check after release</span><input className="input mono" placeholder="https://app.example.com/health" value={form.health_url} disabled={!admin} onChange={(e) => setForm({ ...form, health_url: e.target.value })} /></label>
          </div>
          <label className="check"><input type="checkbox" checked={form.require_ci} disabled={!admin} onChange={(e) => setForm({ ...form, require_ci: e.target.checked })} /> Require GitHub CI checks before merging</label>
          {admin && <div style={{ display: "flex", gap: 8 }}><button className="btn primary" onClick={save}>Save</button><span style={{ flex: 1 }} /><button className="btn ghost danger" onClick={remove}>Disconnect</button></div>}
        </div>
      )}
    </div>
  );
}

function Members() {
  const { ws, me } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const q = useQuery({ queryKey: ["members", ws.id], queryFn: () => api<{ user: { id: string; name: string; email: string }; role: string }[]>(`/api/workspaces/${ws.id}/members`) });
  const admin = ws.role === "owner" || ws.role === "admin";
  const setRole = async (userId: string, role: string) => {
    try {
      await api(`/api/workspaces/${ws.id}/members/${userId}`, { method: "PATCH", json: { role } });
      qc.invalidateQueries({ queryKey: ["members", ws.id] });
    } catch (err) {
      toast((err as Error).message, true);
    }
  };
  return (
    <section className="s-block">
      <h2>Members</h2>
      <p>Everyone in {ws.org_name} on MindsHub can sign in and joins this workspace. Organization owners and admins become admins here.</p>
      {(q.data || []).map((m) => (
        <div key={m.user.id} className="s-line">
          <div className="l" style={{ display: "flex", gap: 10, alignItems: "center" }}><Avatar name={m.user.name || m.user.email} /><span><b>{m.user.name}{m.user.id === me.user.id ? " (you)" : ""}</b><span>{m.user.email}</span></span></div>
          <select className="select" style={{ width: 140 }} value={m.role} disabled={!admin || m.user.id === me.user.id} onChange={(e) => setRole(m.user.id, e.target.value)}>
            {["viewer", "member", "admin", "owner"].map((r) => <option key={r} value={r}>{r}</option>)}
          </select>
        </div>
      ))}
    </section>
  );
}

function WorkspaceSection() {
  const { ws } = useWs();
  const save = useSaveSettings();
  const [name, setName] = useState(ws.name);
  const admin = ws.role === "owner" || ws.role === "admin";
  return (
    <>
    <section className="s-block">
      <h2>Workspace</h2>
      <p>Each workspace has its own repositories, credentials, policies and history.</p>
      <div className="s-grid">
        <label className="field"><span>Name</span><input className="input" value={name} disabled={!admin} onChange={(e) => setName(e.target.value)} /></label>
      </div>
      {admin && <div style={{ marginTop: 12 }}><button className="btn primary" onClick={() => save({ name }, false)}>Save</button></div>}
      <div className="s-line" style={{ marginTop: 18, borderTop: "1px solid var(--line)" }}>
        <div className="l"><b>{ws.paused ? "Paused" : "Running"}</b><span>Pausing stops new plans and tasks from starting. Running steps finish.</span></div>
        {admin && <button className="btn" onClick={() => save({ paused: !ws.paused }, false)}>{ws.paused ? "Resume" : "Pause workspace"}</button>}
      </div>
    </section>
    <FeedbackIntake />
    </>
  );
}

function FeedbackIntake() {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const [key, setKey] = useState<string | null>(null);
  const admin = ws.role === "owner" || ws.role === "admin";
  const q = useQuery({ queryKey: ["feedback-key", ws.id], queryFn: () => api<{ configured: boolean; hint: string; endpoint: string; updated_at: string | null }>(`/api/workspaces/${ws.id}/feedback-key`) });
  const endpoint = `${location.origin}${q.data?.endpoint || `/api/feedback/${ws.id}`}`;
  const rotate = async () => {
    if (q.data?.configured && !confirm("Replace the feedback key? Products using the current key will stop sending feedback until they are updated.")) return;
    try {
      const r = await api<{ key: string }>(`/api/workspaces/${ws.id}/feedback-key`, { method: "POST" });
      setKey(r.key);
      qc.invalidateQueries({ queryKey: ["feedback-key", ws.id] });
    } catch (err) {
      toast((err as Error).message, true);
    }
  };
  const example = `curl -X POST ${endpoint} \\
  -H "Authorization: Bearer ${key || "<feedback key>"}" \\
  -H "Content-Type: application/json" \\
  -d '{"title": "Export does nothing on Safari", "body": "…", "repo": "owner/name", "source_id": "ticket-123"}'`;
  return (
    <section className="s-block">
      <h2>Product feedback</h2>
      <p>Your product can send what its users report straight to Signals. Repeats of the same <code className="mono">source_id</code> (or title) are counted, not duplicated, and nothing becomes work until someone accepts it.</p>
      <div className="cred">
        <div className="cred-top">
          <Dot tone={q.data?.configured ? "good" : "idle"} />
          <b>Feedback key</b>
          {q.data?.configured && <span className="masked">{q.data.hint}</span>}
          <span style={{ flex: 1 }} />
          {admin && <button className="btn sm" onClick={rotate}>{q.data?.configured ? "Replace key" : "Create key"}</button>}
        </div>
        <div className="muted" style={{ fontSize: 12.5, marginTop: 6 }}>
          {key ? "Copy it now. It is stored only as a hash and cannot be shown again." : q.data?.configured ? `Created ${ago(q.data.updated_at)}.` : "No key yet. Members can still log feedback by hand on the Signals page."}
        </div>
        {key && <input className="input mono" readOnly value={key} onFocus={(e) => e.currentTarget.select()} aria-label="New feedback key" style={{ marginTop: 10 }} />}
        <pre className="log" style={{ marginTop: 10, whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>{example}</pre>
      </div>
    </section>
  );
}
