import { useState } from "react";
import { api } from "../api";
import { Link } from "react-router-dom";
import { useWs } from "../App";
import { STEP_LABELS, useOnboarding } from "../onboarding/state";

/** Server readiness is authoritative; never infer it from credential hints. */
export function useSetupState() {
  const { ws } = useWs();
  const state = useOnboarding(ws.id);
  return { loading: state.isLoading, ready: state.data?.ready ?? false, error: state.error };
}

export function SetupNotice() {
  const { ws } = useWs();
  const state = useOnboarding(ws.id);
  const admin = ws.role === "owner" || ws.role === "admin";
  return (
    <section className="setup" aria-label="Workspace setup">
      <h2>Finish setting up {ws.name}</h2>
      {state.error ? <p role="alert">Could not load workspace readiness. <button className="btn sm" onClick={() => state.refetch()}>Try again</button></p> : <>
        <p>{admin ? "Your saved setup is ready to resume." : "An owner or admin must finish workspace setup."}</p>
        {!!state.data?.missing_steps.length && <p>Missing: {state.data.missing_steps.map((s) => STEP_LABELS[s]).join(", ")}.</p>}
      </>}
      <Link className="btn primary" to={`/w/${ws.id}/onboarding`}>{admin ? "Resume onboarding" : "View workspace setup"}</Link>
    </section>
  );
}

/** Fetch only the backend's allowlisted prose, never copy client-side settings. */
export function CopySetupPrompt() {
  const { ws } = useWs();
  const [status, setStatus] = useState("");
  const [busy, setBusy] = useState(false);
  return <div>
    <button className="btn" disabled={busy} onClick={async () => {
      setBusy(true);
      try {
        const prompt = await api<{ text: string }>(`/api/workspaces/${ws.id}/onboarding/prompt`);
        await navigator.clipboard.writeText(prompt.text);
        setStatus("Setup prompt copied. Enter keys only in Factory.");
      } catch { setStatus("Could not copy setup prompt. Please retry and allow clipboard access."); }
      finally { setBusy(false); }
    }}>{busy ? "Copying…" : "Copy setup prompt"}</button>
    <p role="status">{status}</p>
  </div>;
}
