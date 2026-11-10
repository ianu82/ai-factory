import { useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { api, type Settings, type Me } from "../api";
import { type OnboardingStepProps } from "./api";

export const AUTONOMY_PRESETS: Record<string, Settings["autonomy"]> = {
  Cautious: { plan: "approval", merge: "approval", release: "approval" },
  Balanced: { plan: "auto", merge: "approval", release: "approval" },
  Autonomous: { plan: "auto", merge: "auto", release: "approval" },
};

export function AutonomyStep({ workspace, readOnly, onComplete, onNavigate }: OnboardingStepProps) {
  const qc = useQueryClient();
  const savedPolicy = workspace.settings.learning?.preset ?? workspace.settings.autonomy;
  const [choice, setChoice] = useState(() => Object.keys(AUTONOMY_PRESETS).find((name) =>
    (["plan", "merge", "release"] as const).every((key) => AUTONOMY_PRESETS[name][key] === savedPolicy?.[key])) || "Custom/current");
  const [budget, setBudget] = useState(String(workspace.settings.budgets?.outcome_usd ?? ""));
  const [busy, setBusy] = useState(false);
  const lock = useRef(false);
  const [error, setError] = useState("");
  const valid = budget.trim() !== "" && Number.isFinite(Number(budget)) && Number(budget) > 0;
  const policy = AUTONOMY_PRESETS[choice] || savedPolicy;
  const submit = async () => {
    if (lock.current || readOnly || !valid) return;
    lock.current = true; setBusy(true); setError("");
    try {
      // Refresh budgets before replacing this nested object; never reset run/token limits.
      const me = await api<Me>("/api/me");
      const latest = me.workspaces.find((w) => w.id === workspace.id);
      if (!latest) throw new Error("Workspace is no longer accessible.");
      await api(`/api/workspaces/${workspace.id}`, { method: "PATCH", json: { settings: {
        ...(AUTONOMY_PRESETS[choice] ? { autonomy: policy } : {}),
        budgets: { ...latest.settings.budgets, outcome_usd: Number(budget) },
      } } });
      await qc.invalidateQueries({ queryKey: ["me"] });
      await onComplete({ action: "complete", step: "autonomy", acknowledged: true });
      await onNavigate("team");
    } catch (err) { setError((err as Error).message); }
    finally { lock.current = false; setBusy(false); }
  };
  return <form onSubmit={(e) => { e.preventDefault(); void submit(); }}>
    <fieldset disabled={busy || readOnly}>
      <legend>Approval policy</legend>
      <p>Choose your post-learning preset or keep your current selection. Each value below is saved for explicit application later.</p>
      {Object.entries(AUTONOMY_PRESETS).map(([name, values]) => <label className="onboarding-policy" key={name}>
        <input type="radio" name="autonomy" value={name} checked={choice === name} onChange={() => setChoice(name)} />
        <span><strong>{name}</strong><small>plan: {values.plan} · merge: {values.merge} · release: {values.release}</small></span>
      </label>)}
      <label className="onboarding-policy"><input type="radio" name="autonomy" checked={choice === "Custom/current"} onChange={() => setChoice("Custom/current")} />
        <span><strong>Custom/current</strong><small>Keep the saved workspace policy unchanged.</small></span></label>
      <p className="mono">Selected: plan: {policy?.plan || "not configured"} · merge: {policy?.merge || "not configured"} · release: {policy?.release || "not configured"}</p>
      <label className="field" htmlFor="outcome-budget">Budget per outcome (USD)<input id="outcome-budget" className="input" type="number" step="any" min="0" required value={budget} aria-invalid={!valid} onChange={(e) => setBudget(e.target.value)} /></label>
      {!valid && <p role="alert">Enter a finite budget greater than zero.</p>}
    </fieldset>
    <p>While learning, plans and merges require a person’s approval: nothing is planned into work or merged without you. After the first fully completed outcome, Inbox offers an admin the explicit switch to this preset; completion alone never changes policy.</p>
    <p>Release approval applies to repositories with release.mode=on_merge (deploy-on-main), including in Autonomous. Repositories without that mode do not trigger this release gate.</p>
    <p>You can change approvals and budgets later in Settings. Run and token budgets and other settings are kept.</p>
    {error && <p role="alert">{error}</p>}
    <button className="btn primary" disabled={busy || readOnly || !valid}>{busy ? "Saving…" : "Save autonomy and continue"}</button>
  </form>;
}
