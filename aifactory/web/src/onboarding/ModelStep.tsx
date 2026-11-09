import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, type Settings, type Workspace } from "../api";
import { type OnboardingStepProps } from "./api";
import { ConnectionField, useConnection } from "./connections";

export const MODEL_ROLES = ["planner", "builder", "verifier"] as const;
export function effectiveModels(models: Settings["models"]) {
  return Object.fromEntries(MODEL_ROLES.map((role) => [role, models[role] || models.default || "gpt-terra"])) as Record<typeof MODEL_ROLES[number], string>;
}
export const providerHelp = (provider: string) => provider === "openai"
  ? "Create an API key in your OpenAI project with model access and billing enabled. Paste it here; it is stored encrypted on the server."
  : "Create a MindsHub API key (starting with mdb_) with inference access and credits. Paste it here; it is stored encrypted on the server.";

export function ModelStep({ workspace, readOnly, onComplete, onNavigate }: OnboardingStepProps) {
  const [provider, setProvider] = useState(workspace.settings.provider || "mindshub");
  const [selected, setSelected] = useState(() => effectiveModels(workspace.settings.models || {}));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const c = useConnection(workspace.id, `${provider}_api_key`, readOnly);
  const models = useQuery({ queryKey: ["models", workspace.id], queryFn: () => api<{ provider: string | null; models: string[]; error?: string }>(`/api/workspaces/${workspace.id}/models`) });
  // A provider not yet saved cannot be resolved by GET /models. Its credential
  // test supplies the same catalog until PATCH makes it the workspace provider.
  const usingWorkspaceCatalog = provider === (workspace.settings.provider || "mindshub");
  const catalogError = usingWorkspaceCatalog && (models.isError || !!models.data?.error);
  const catalogLoading = usingWorkspaceCatalog && models.isFetching;
  const options = usingWorkspaceCatalog && models.data?.provider === provider && !models.data.error ? models.data.models : c.credential?.metadata?.models || [];
  const valid = c.credential?.status === "valid" && c.credential.metadata?.diagnostic_code === "models_available"
    && c.credential.metadata?.test_status === "valid" && !catalogError && !catalogLoading && MODEL_ROLES.every((r) => options.includes(selected[r]));
  const submit = async () => {
    if (!valid || c.busy || c.editing || saving || readOnly) return;
    setSaving(true); setError("");
    try {
      await api<Workspace>(`/api/workspaces/${workspace.id}`, { method: "PATCH", json: { settings: { provider, models: { ...workspace.settings.models, ...selected } } } });
      await c.refresh();
      await onComplete({ action: "complete", step: "models" });
      await onNavigate("github");
    } catch { setError("Could not save model choices. Your selections are kept; retry saving."); }
    finally { setSaving(false); }
  };
  return <section aria-label="AI model connection">
    <div className="field"><label htmlFor="model-provider">Model provider</label><select id="model-provider" className="input" value={provider} disabled={saving || c.busy || readOnly} onChange={(e) => { c.setEditing(false); setProvider(e.target.value as Settings["provider"]); }}><option value="mindshub">MindsHub</option><option value="openai">OpenAI</option></select></div>
    <ConnectionField key={provider} connection={c} label={`${provider === "mindshub" ? "MindsHub" : "OpenAI"} API key`} prefix="model" help={providerHelp(provider)} readOnly={readOnly || saving} />
    <form onSubmit={(e) => { e.preventDefault(); void submit(); }}>
      <p>Each role starts with its effective workspace default. Choose an available model for every role; we never silently substitute models.</p>
      {catalogLoading && <p role="status">Loading model catalog…</p>}
      {catalogError && <p role="alert">Could not load the workspace model catalog. <button type="button" className="btn" onClick={() => models.refetch()}>Retry models</button></p>}
      {MODEL_ROLES.map((role) => {
        const unavailable = !options.includes(selected[role]);
        return <div className="field" key={role}><label htmlFor={`model-${role}`}>{role[0].toUpperCase() + role.slice(1)} model</label>
          <select id={`model-${role}`} className="input" value={selected[role]} disabled={readOnly || saving || c.busy} aria-invalid={unavailable} aria-describedby={`model-${role}-help`} onChange={(e) => setSelected({ ...selected, [role]: e.target.value })}>
            {unavailable && <option value={selected[role]}>{selected[role]} — unavailable / not verified</option>}
            {options.map((name: string) => <option key={name} value={name}>{name}</option>)}
          </select><span id={`model-${role}-help`} className="hint">{unavailable ? "This selection is not in the available catalog. Verify your key, then choose an available model to continue." : "Exact model saved for this role, also visible in Settings."}</span>
        </div>;
      })}
      <div aria-live="polite">{error && <p role="alert">{error}</p>}{saving && <p>Saving model choices…</p>}</div>
      <button className="btn primary" disabled={!valid || readOnly || saving || c.busy || c.editing}>{saving ? "Saving…" : "Save models and continue"}</button>
    </form>
  </section>;
}
