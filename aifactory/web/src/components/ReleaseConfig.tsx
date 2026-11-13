import { useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type Repo } from "../api";
import { useToast } from "../ui";
import {
  WEEKDAYS, newWebhook, newWorkflow, releaseDraft, releaseLines, serializeRelease,
  type ReleaseDraft, type TargetDraft, type SmokeDraft, type WebhookCompletion,
} from "../releaseTypes";

function Text({ label, value, onChange, multiline = false, type = "text", hint }: {
  label: string; value: string; onChange: (value: string) => void; multiline?: boolean; type?: string; hint?: string;
}) {
  return <label className="field"><span>{label}</span>{multiline
    ? <textarea className="textarea mono" aria-label={label} value={value} onChange={(e) => onChange(e.target.value)} />
    : <input className="input mono" aria-label={label} type={type} value={value} onChange={(e) => onChange(e.target.value)} />}
    {hint && <span className="hint">{hint}</span>}</label>;
}
function NumberField({ label, value, min, max, onChange }: {
  label: string; value: number; min: number; max: number; onChange: (value: number) => void;
}) {
  return <label className="field"><span>{label}</span><input className="input" aria-label={label} type="number" min={min} max={max} step={1}
    value={Number.isNaN(value) ? "" : value} onChange={(e) => onChange(e.target.value === "" ? NaN : Number(e.target.value))} /></label>;
}
function Choice<T extends string>({ label, value, options, onChange }: {
  label: string; value: T; options: readonly (readonly [T, string])[]; onChange: (value: T) => void;
}) {
  return <label className="field"><span>{label}</span><select className="select" aria-label={label} value={value} onChange={(e) => onChange(e.target.value as T)}>
    {options.map(([value, text]) => <option key={value} value={value}>{text}</option>)}
  </select></label>;
}

function CompletionEditor({ value, onChange }: { value: WebhookCompletion; onChange: (value: WebhookCompletion) => void }) {
  // Raw text is kept until save so malformed status lists cannot become a valid partial list.
  const [statuses, setStatuses] = useState(value.mode === "synchronous" ? (value.success_statuses ?? [200, 204]).join(", ") : "200, 204");
  return <>
    <Choice label="Webhook completion" value={value.mode} options={[["synchronous", "Synchronous: response confirms finished deploy"], ["poll", "Asynchronous: poll deployment status"]]}
      onChange={(mode) => onChange(mode === "synchronous" ? { mode, success_statuses: statuses.split(",").map((v) => v.trim() ? Number(v.trim()) : NaN) }
        : { mode, status_url: "", handle_key: "id", state_key: "state", success_values: ["success"], failure_values: ["failure"], interval_seconds: 10, timeout_seconds: 1800 })} />
    {value.mode === "synchronous" ? <Text label="Final success HTTP statuses (comma separated; never 202)" value={statuses} onChange={(text) => {
      setStatuses(text); onChange({ ...value, success_statuses: text.split(",").map((v) => v.trim() ? Number(v.trim()) : NaN) });
    }} /> : <>
      <Text label="Polling status URL (HTTPS)" value={value.status_url} onChange={(status_url) => onChange({ ...value, status_url })} />
      <div className="s-grid">
        <Text label="Response handle key" value={value.handle_key} onChange={(handle_key) => onChange({ ...value, handle_key })} />
        <Text label="Status state key" value={value.state_key} onChange={(state_key) => onChange({ ...value, state_key })} />
        <Text label="Success state values (one per line)" multiline value={value.success_values.join("\n")} onChange={(text) => onChange({ ...value, success_values: text.split("\n") })} />
        <Text label="Failure state values (one per line)" multiline value={value.failure_values.join("\n")} onChange={(text) => onChange({ ...value, failure_values: text.split("\n") })} />
        <NumberField label="Poll interval (seconds)" min={1} max={300} value={value.interval_seconds ?? 10} onChange={(interval_seconds) => onChange({ ...value, interval_seconds })} />
        <NumberField label="Poll timeout (seconds)" min={1} max={86400} value={value.timeout_seconds ?? 1800} onChange={(timeout_seconds) => onChange({ ...value, timeout_seconds })} />
      </div>
    </>}
  </>;
}

function TargetEditor({ label, value, onChange }: { label: string; value: TargetDraft; onChange: (value: TargetDraft) => void }) {
  return <details style={{ marginBlock: 12 }}><summary>{label} deployment · {value.kind === "workflow" ? "GitHub Actions" : "Webhook"}</summary>
    <div className="s-grid" style={{ marginTop: 12 }}>
      <Choice label={`${label} target`} value={value.kind} options={[["workflow", "GitHub Actions workflow"], ["webhook", "Deploy webhook"]]}
        onChange={(kind) => onChange(kind === "workflow" ? newWorkflow() : newWebhook())} />
      {value.kind === "workflow" ? <>
        <Text label="Workflow filename or ID" value={value.workflow} onChange={(workflow) => onChange({ ...value, workflow })} />
        <Text label="Correlation input name" value={value.correlation_input} onChange={(correlation_input) => onChange({ ...value, correlation_input })}
          hint="Your workflow must declare and accept this workflow_dispatch input for the stable operation key. Do not include it in the JSON below." />
        <Text label="Workflow inputs (JSON object of strings)" multiline value={value.inputs} onChange={(inputs) => onChange({ ...value, inputs })} hint='Example: {"environment": "staging"}. Keep credentials in CI, never in inputs.' />
      </> : <>
        <Text label="Webhook endpoint (HTTPS)" value={value.endpoint} onChange={(endpoint) => onChange({ ...value, endpoint })} />
        <NumberField label="Webhook request timeout (seconds)" min={1} max={120} value={value.timeout_seconds ?? 30} onChange={(timeout_seconds) => onChange({ ...value, timeout_seconds })} />
      </>}
    </div>
    {value.kind === "webhook" && <CompletionEditor key={value.kind} value={value.completion} onChange={(completion) => onChange({ ...value, completion })} />}
  </details>;
}
function SmokeEditor({ label, value, onChange }: { label: string; value: SmokeDraft; onChange: (value: SmokeDraft) => void }) {
  return <details style={{ marginBlock: 12 }}><summary>{label} smoke · {releaseLines(value.commands).length} commands, {releaseLines(value.urls).length} URLs</summary>
    <div className="s-grid" style={{ marginTop: 12 }}>
      <Text label={`${label} smoke commands (one per line)`} multiline value={value.commands} onChange={(commands) => onChange({ ...value, commands })} />
      <Text label={`${label} smoke URLs (one HTTPS URL per line)`} multiline value={value.urls} onChange={(urls) => onChange({ ...value, urls })} />
      <NumberField label={`${label} smoke timeout (seconds)`} min={1} max={600} value={value.timeout_seconds} onChange={(timeout_seconds) => onChange({ ...value, timeout_seconds })} />
    </div><p className="hint">At least one command or URL is required. Both forms can run together. URLs must not contain credentials, queries or fragments.</p>
  </details>;
}

export function ReleaseConfig({ repo, workspaceId, admin }: { repo: Repo; workspaceId: string; admin: boolean }) {
  const qc = useQueryClient(), toast = useToast();
  const [draft, setDraft] = useState(() => releaseDraft(repo.config.release));
  const [dirty, setDirty] = useState(false);
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);
  const [secret, setSecret] = useState("");
  const [savingSecret, setSavingSecret] = useState(false);
  const base = `/api/workspaces/${workspaceId}/repositories/${repo.id}`;
  const secretKey = ["release-secret", workspaceId, repo.id];
  const status = useQuery({ queryKey: secretKey, queryFn: () => api<{ configured: boolean }>(`${base}/release/webhook-secret/status`) });
  // Reconcile background updates without overwriting an admin's unsaved edits.
  useEffect(() => { if (!dirty) setDraft(releaseDraft(repo.config.release)); }, [repo.config.release, dirty]);
  const change = <K extends keyof ReleaseDraft>(key: K, value: ReleaseDraft[K]) => {
    setDirty(true); setError(""); setDraft((old) => ({ ...old, [key]: value }));
  };
  const save = async () => {
    if (!admin || saving) return;
    setError("");
    try {
      const release = serializeRelease(draft, repo.config.release, status.data?.configured === true);
      setSaving(true);
      const updated = await api<Repo>(base, { method: "PATCH", json: { config: { release } } });
      // Update the cache before releasing dirty state, avoiding a reset to the old prop.
      qc.setQueryData<Repo[]>(["repos", workspaceId], (repos) => repos?.map((r) => r.id === repo.id ? updated : r));
      setDraft(releaseDraft(updated.config.release)); setDirty(false);
      toast("Release configuration saved");
      await qc.invalidateQueries({ queryKey: ["repos", workspaceId] });
    } catch (err) { setError((err as Error).message); toast((err as Error).message, true); }
    finally { setSaving(false); }
  };
  const saveSecret = async () => {
    if (!admin || savingSecret) return;
    if (secret.trim().length < 8 || new TextEncoder().encode(secret).length > 8192) {
      setError("Webhook secret must be at least 8 characters after trimming and at most 8192 bytes."); return;
    }
    setSavingSecret(true); setError("");
    try {
      await api(`${base}/release/webhook-secret`, { method: "PUT", json: { value: secret } });
      qc.setQueryData(secretKey, { configured: true });
      toast("Webhook secret saved");
    } catch { setError("Could not save webhook secret. Check your access and try again."); }
    finally { setSecret(""); setSavingSecret(false); }
  };
  const disabled = !admin || saving;
  const cut = draft.cut, schedule = draft.schedule;
  return <section aria-label="Release configuration" style={{ marginBlock: 20 }}>
    <h3>Releases</h3>
    {!admin && <p className="hint">Only workspace owners and admins can change release configuration or secrets.</p>}
    <fieldset disabled={disabled} style={{ border: 0, padding: 0, margin: 0, minWidth: 0 }}>
      <div className="s-grid">
        <Choice label="Release mode" value={draft.mode} options={[["", "Not managed by the Factory"], ["continuous", "Continuous"], ["on_merge", "Deploys when main changes (legacy on_merge)"], ["train", "Scheduled release train"]]}
          onChange={(mode) => change("mode", mode)} />
        {draft.mode !== "train" && <Text label="Health check after release (legacy)" value={draft.health_url} onChange={(value) => change("health_url", value)} hint="Existing legacy health-check settings are preserved." />}
      </div>
      {draft.mode === "train" && <>
        <p className="hint">The Factory triggers your existing deployment tooling; it does not edit team CI. GitHub credentials need Actions read/write permission to dispatch workflows and read their runs (classic tokens need repo access; private repositories also need repository access). Each workflow must support workflow_dispatch and declare the correlation input to match its run to the release operation.</p>
        <div className="field"><span>Release weekdays (local time)</span><div style={{ display: "flex", flexWrap: "wrap", gap: 12 }}>
          {WEEKDAYS.map((day) => <label className="check" key={day}><input type="checkbox" checked={schedule.weekdays.includes(day)}
            onChange={(e) => change("schedule", { ...schedule, weekdays: e.target.checked ? [...schedule.weekdays, day] : schedule.weekdays.filter((d) => d !== day) })} />{day}</label>)}
        </div></div>
        <div className="s-grid">
          <Text label="Local release time" type="time" value={schedule.time} onChange={(time) => change("schedule", { ...schedule, time })} />
          <Text label="IANA timezone" value={schedule.timezone} onChange={(timezone) => change("schedule", { ...schedule, timezone })} hint="For example UTC, America/New_York or Europe/London; scheduling follows local daylight saving time." />
          <Choice label="Cut release from main" value={cut.mode} options={[["tag", "Tag on main"], ["version_pr", "Version-bump pull request"]]}
            onChange={(mode) => change("cut", { ...cut, mode, version_file: mode === "version_pr" ? { path: "VERSION", format: "plain" } : null })} />
          <Text label="Tag prefix" value={cut.tag_prefix} onChange={(tag_prefix) => change("cut", { ...cut, tag_prefix })} />
          <Text label="Initial version" value={cut.initial_version} onChange={(initial_version) => change("cut", { ...cut, initial_version })} hint="major.minor.patch; subsequent releases increment patch only." />
          <Choice label="Production approval policy" value={draft.approval} options={[["inherit", "Inherit workspace release policy"], ["approval", "Always require approval"], ["auto", "Automatic promotion after staging smoke"]]} onChange={(value) => change("approval", value)} />
        </div>
        {cut.mode === "version_pr" && cut.version_file && <div className="s-grid">
          <Text label="Version file path" value={cut.version_file.path} onChange={(path) => change("cut", { ...cut, version_file: { ...cut.version_file!, path } })} />
          <Choice label="Version file format" value={cut.version_file.format} options={[["plain", "Plain version text"], ["json", "JSON object"]]}
            onChange={(format) => change("cut", { ...cut, version_file: { path: cut.version_file!.path, format, ...(format === "json" ? { key: "version" } : {}) } })} />
          {cut.version_file.format === "json" && <Text label="Literal JSON version key" value={cut.version_file.key ?? ""} onChange={(key) => change("cut", { ...cut, version_file: { ...cut.version_file!, key } })} />}
        </div>}
        {(["staging", "production", "rollback"] as const).map((key) => <TargetEditor key={key} label={key[0].toUpperCase() + key.slice(1)} value={draft[key]} onChange={(value) => change(key, value)} />)}
        <p className="hint">Webhook acceptance is not completion: a synchronous response must confirm the finished deployment (never HTTP 202). Asynchronous deployments must return a handle and expose a polling URL with explicit success/failure states and bounded polling timeouts. All webhook targets share this repository’s write-only secret. Use HTTPS without embedded credentials, query strings or fragments.</p>
        {(["staging_smoke", "production_smoke"] as const).map((key) => <SmokeEditor key={key} label={key === "staging_smoke" ? "Staging" : "Production"} value={draft[key]} onChange={(value) => change(key, value)} />)}
      </>}
      <button className="btn primary sm" disabled={!dirty || disabled} onClick={save}>{saving ? "Saving…" : "Save release configuration"}</button>
    </fieldset>
    <details style={{ marginTop: 16 }}><summary>Release webhook secret · {status.isPending ? "Checking…" : status.isError ? "Status unavailable" : status.data.configured ? "Configured" : "Not configured"}</summary>
      <p className="hint">Stored separately and never returned or displayed, not even partially. Saving replaces the shared secret for this repository only. Deployment credentials stay in your CI/platform.</p>
      {status.isError && <p role="alert">Could not check configured state. <button className="btn sm" onClick={() => status.refetch()}>Retry</button></p>}
      <fieldset disabled={!admin || savingSecret} style={{ border: 0, padding: 0, margin: 0 }}>
        <label className="field"><span>New webhook secret</span><input className="input" type="password" autoComplete="new-password" spellCheck={false} value={secret} onChange={(e) => setSecret(e.target.value)} /></label>
        <button className="btn sm" disabled={!admin || savingSecret || !secret} onClick={saveSecret}>{savingSecret ? "Saving…" : "Save webhook secret"}</button>
      </fieldset>
    </details>
    {error && <p className="err" role="alert">{error}</p>}
  </section>;
}
