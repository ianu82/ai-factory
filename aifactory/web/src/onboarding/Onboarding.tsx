import { useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { useWs } from "../App";
import { api } from "../api";
import { ONBOARDING_STEPS, onboardingQueryKey, patchOnboarding, type OnboardingAction } from "./api";
import { deferMember, STEP_LABELS, useOnboarding } from "./state";
import { ModelStep } from "./ModelStep";
import { GitHubStep } from "./GitHubStep";
import { AutonomyStep } from "./AutonomyStep";
import { TeamStep } from "./TeamStep";
import { FirstOutcomeStep } from "./FirstOutcomeStep";
import { RepositoryStep } from "./RepositoryStep";

export function Onboarding() {
  const { ws, me } = useWs();
  const progress = useOnboarding(ws.id);
  const qc = useQueryClient();
  const navigate = useNavigate();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const heading = useRef<HTMLHeadingElement>(null);
  const admin = ws.role === "owner" || ws.role === "admin";
  const step = progress.data?.current_step;
  const index = step ? ONBOARDING_STEPS.indexOf(step) : 0;
  useEffect(() => { heading.current?.focus(); }, [step, admin]);

  const save = async (action: OnboardingAction) => {
    // Do not let an older in-flight snapshot overwrite a successful deferral
    // or navigation and immediately redirect the user back from Inbox.
    await qc.cancelQueries({ queryKey: onboardingQueryKey(ws.id) });
    const state = await patchOnboarding(ws.id, action);
    qc.setQueryData(onboardingQueryKey(ws.id), state);
  };
  const run = async (action: () => Promise<void>) => {
    setBusy(true);
    setError("");
    try { await action(); }
    catch (err) { setError((err as Error).message); }
    finally { setBusy(false); }
  };
  const leave = () => run(async () => {
    if (admin) await save({ action: "defer", deferred: true });
    else deferMember(me.user.id, ws.id);
    navigate(`/w/${ws.id}/inbox`);
  });
  const signOut = () => run(async () => {
    const result = await api<{ logout_url?: string }>("/auth/logout", { method: "POST" });
    qc.clear();
    if (result.logout_url) window.location.href = result.logout_url;
    else navigate("/signin");
  });

  return (
    <div className="onboarding-frame">
      <header className="onboarding-header">
        <div><span className="mono">FACTORY / WORKSPACE SETUP</span><strong>{ws.name}</strong><span className="muted">{ws.org_name} · {ws.role}</span></div>
        <div className="onboarding-header-actions">
          <button className="btn" disabled={busy} onClick={leave}>Leave for now</button>
          <button className="btn ghost" disabled={busy} onClick={signOut}>Sign out</button>
        </div>
        {me.workspaces.length > 1 && <label className="onboarding-workspace">Workspace
          <select aria-label="Workspace" value={ws.id} disabled={busy} onChange={(e) => navigate(`/w/${e.target.value}/inbox`)}>
            {me.workspaces.map((w) => <option key={w.id} value={w.id}>{w.org_name} / {w.name}</option>)}
          </select>
        </label>}
      </header>
      <div className="onboarding-layout">
        <nav className="onboarding-nav" aria-label="Workspace setup steps">
          <ol>{ONBOARDING_STEPS.map((s, i) => {
            const done = progress.data?.completed_steps.includes(s);
            return <li key={s}>
              <button aria-current={admin && step === s ? "step" : undefined}
                disabled={!admin || !progress.data || busy}
                onClick={() => run(() => save({ action: "navigate", step: s }))}>
                <span className="mono">{i + 1}</span><span>{STEP_LABELS[s]}<small>{done ? "Saved · Revisit" : "Not completed"}</small></span>
              </button>
            </li>;
          })}</ol>
          <p className="muted">Progress is saved to this workspace. You can leave and return at any time.</p>
        </nav>
        <main className="onboarding-main" aria-busy={progress.isLoading || busy}>
          <div className="onboarding-content">
            {progress.isLoading ? <p role="status">Loading saved workspace progress…</p> : progress.data ? admin ? <>
              <p className="mono muted">STEP {index + 1} OF {ONBOARDING_STEPS.length} · {progress.data.completed_steps.length} SAVED</p>
              <h1 ref={heading} tabIndex={-1}>{STEP_LABELS[progress.data.current_step]}</h1>
              <p>{progress.data.completed_steps.includes(progress.data.current_step) ? "This step is saved. You can revisit it without losing progress." : "Set up your workspace one step at a time."}</p>
              {step === "models" ? <ModelStep key={ws.id} workspace={ws} progress={progress.data} readOnly={!admin}
                onNavigate={(next) => save({ action: "navigate", step: next })}
                onComplete={save} onDefer={leave} /> : step === "github" ? <GitHubStep key={ws.id} workspace={ws} progress={progress.data} readOnly={!admin}
                onNavigate={(next) => save({ action: "navigate", step: next })}
                onComplete={save} onDefer={leave} /> : step === "repositories" ? <RepositoryStep key={ws.id} workspace={ws} progress={progress.data} readOnly={!admin}
                onNavigate={(next) => save({ action: "navigate", step: next })}
                onComplete={save} onDefer={leave} /> : step === "autonomy" ? <AutonomyStep key={ws.id} workspace={ws} progress={progress.data} readOnly={!admin}
                onNavigate={(next) => save({ action: "navigate", step: next })} onComplete={save} onDefer={leave} />
                : step === "team" ? <TeamStep key={ws.id} workspace={ws} progress={progress.data} readOnly={!admin}
                onNavigate={(next) => save({ action: "navigate", step: next })} onComplete={save} onDefer={leave} />
                : <FirstOutcomeStep key={ws.id} workspace={ws} progress={progress.data} readOnly={!admin}
                onNavigate={(next) => save({ action: "navigate", step: next })} onComplete={save} onDefer={leave} />}
            </> : <>
              <h1 ref={heading} tabIndex={-1}>An admin needs to finish workspace setup</h1>
              <p>Your {ws.role} role can view setup progress, but only an owner or admin can configure this workspace. Ask a workspace admin to finish the steps below.</p>
              <h2>Missing steps</h2>
              {progress.data.missing_steps.length ? <ul>{progress.data.missing_steps.map((s) => <li key={s}>{STEP_LABELS[s]}</li>)}</ul> : <p>No operational prerequisites are missing. This workspace is ready.</p>}
              <p>You can leave for now and use the rest of the app. No credentials are tested from this page.</p>
            </> : <>
              <h1>Workspace setup</h1><p role="alert">Could not load saved progress: {(progress.error as Error)?.message}</p>
              <button className="btn" onClick={() => progress.refetch()}>Try again</button>
            </>}
          </div>
          <div className="onboarding-message" aria-live="polite">{error && <p role="alert">{error}</p>}{busy && <span>Saving…</span>}</div>
          <footer className="onboarding-actions">
            {admin && progress.data ? <>
              <button className="btn" disabled={busy || index === 0} onClick={() => run(() => save({ action: "navigate", step: ONBOARDING_STEPS[index - 1] }))}>Back</button>

            </> : <span className="muted">Workspace setup · read-only</span>}
          </footer>
        </main>
      </div>
    </div>
  );
}
