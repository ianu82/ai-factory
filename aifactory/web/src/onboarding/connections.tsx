import { useCallback, useEffect, useRef, useState } from "react";
import { useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { api, type Credential, type CredentialsResponse } from "../api";
import { onboardingQueryKey } from "./api";

// Non-secret versions live only in the query client's lifetime. Testing changes
// updated_at, so remember both versions to prevent refetch/test loops.
const tested = new WeakMap<QueryClient, Set<string>>();
export const credentialVersion = (id: string, c: Credential) => JSON.stringify([id, c.kind, c.hint, c.updated_at]);
export const needsAutomaticTest = (c: Credential) => c.configured && !(c.kind === "github_token" && c.metadata?.repository);

export function useConnection(id: string, kind: string, readOnly: boolean) {
  const qc = useQueryClient();
  const query = useQuery({ queryKey: ["credentials", id], queryFn: () => api<CredentialsResponse>(`/api/workspaces/${id}/credentials`) });
  const credential = query.data?.items.find((c) => c.kind === kind);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const locked = useRef(false);
  const input = useRef<HTMLInputElement>(null);
  const inputRef = useCallback((node: HTMLInputElement | null) => {
    input.current = node;
    if (node) return () => { node.value = ""; input.current = null; };
  }, []);
  const [editing, setEditing] = useState(false);
  const versions = tested.get(qc) || new Set<string>();
  tested.set(qc, versions);
  const refresh = () => Promise.all([
    ...[["credentials", id], ["models", id], ["github-repos", id], ["repos", id], ["me"], onboardingQueryKey(id)].map((queryKey) => qc.invalidateQueries({ queryKey })),
  ]);
  const run = async (replace = false) => {
    if (locked.current || readOnly) return;
    locked.current = true;
    setBusy(true); setError("");
    const value = replace ? input.current?.value.trim() : undefined;
    if (input.current) input.current.value = "";
    if (credential) versions.add(credentialVersion(id, credential));
    try {
      const c = await api<Credential>(`/api/workspaces/${id}/credentials/${kind}${replace ? "" : "/test"}`, {
        method: replace ? "PUT" : "POST", ...(replace ? { json: { value } } : {}),
      });
      versions.add(credentialVersion(id, c));
      qc.setQueryData<CredentialsResponse>(["credentials", id], (old) => old && ({ ...old, items: old.items.map((item) => item.kind === kind ? c : item) }));
      setEditing(false);
    } catch {
      // Never render arbitrary request/upstream errors alongside secret entry.
      setError("Could not save or test this credential. Check your connection and retry, or replace it.");
    } finally {
      if (input.current) input.current.value = "";
      await refresh();
      locked.current = false; setBusy(false);
    }
  };
  useEffect(() => {
    if (!readOnly && credential && needsAutomaticTest(credential) && !versions.has(credentialVersion(id, credential))) void run();
  }, [credential, readOnly, id, kind]); // run is guarded against duplicate entry/refetches
  return { query, credential, busy, error, input, inputRef, editing, setEditing, run, refresh };
}

export function ConnectionField({ connection: c, label, help, prefix, readOnly }: {
  connection: ReturnType<typeof useConnection>; label: string; help: string; prefix: string; readOnly: boolean;
}) {
  const code = c.credential?.metadata?.diagnostic_code;
  const recovery = code === "identity_invalid" || code === "github_invalid" ? "Replace the credential with a new key or token."
    : code === "inference_denied" || code === "models_empty" ? "Enable inference and credits in your provider account, then retry, or replace the key."
    : ["network_error", "rate_limited", "service_error"].includes(code) ? "Your saved credential is kept. Retry the test when the service is reachable." : "";
  return <div>
    {c.query.isLoading && <p role="status">Loading stored credential…</p>}
    {c.query.isError && <div role="alert">Could not load stored credentials. <button className="btn" onClick={() => c.query.refetch()}>Retry credentials</button></div>}
    {c.credential?.configured && <p>Stored {label}: <span className="masked">{c.credential.hint}</span></p>}
    {(!c.credential?.configured || c.editing) && !c.query.isLoading && !c.query.isError && <form onSubmit={(e) => { e.preventDefault(); void c.run(true); }}>
      <label className="field" htmlFor={`${prefix}-secret`}>{label}</label>
      <input ref={c.inputRef} id={`${prefix}-secret`} className="input" type="password" required minLength={8} maxLength={8192} autoComplete="off" disabled={readOnly || c.busy} aria-describedby={`${prefix}-help ${prefix}-status`} />
      <p id={`${prefix}-help`} className="hint">{help}</p>
      <button className="btn primary" disabled={readOnly || c.busy}>Save and test</button>
      {c.editing && <button type="button" className="btn" disabled={c.busy} onClick={() => c.setEditing(false)}>Cancel replacement</button>}
    </form>}
    {c.credential?.configured && !c.editing && <div>
      <button className="btn" disabled={readOnly || c.busy} onClick={() => c.run()}>Retry test</button>{" "}
      <button className="btn" disabled={readOnly || c.busy} onClick={() => c.setEditing(true)}>Replace {label}</button>
    </div>}
    <div id={`${prefix}-status`} role="status" aria-live="polite">
      {c.busy ? <p>Testing saved credential…</p> : <><p>{c.credential?.detail}</p><p>{recovery}</p></>}
      {c.error && <p role="alert">{c.error}</p>}
    </div>
  </div>;
}
