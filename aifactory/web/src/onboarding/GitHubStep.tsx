import { GitHubConnection } from "../components/Setup";
import { useState } from "react";
import { type OnboardingStepProps } from "./api";
import { ConnectionField, useConnection } from "./connections";
import { GitHubPermissions } from "./GitHubPermissions";

export function GitHubStep(props: OnboardingStepProps) {
  const { workspace, readOnly, onNavigate } = props;
  return <GitHubConnection workspaceId={workspace.id} admin={!readOnly} destination="setup" onContinue={() => void onNavigate("repositories")}>
    <GitHubTokenStep {...props} />
  </GitHubConnection>;
}

function GitHubTokenStep({ workspace, progress, readOnly, onComplete, onNavigate }: OnboardingStepProps) {
  const c = useConnection(workspace.id, "github_token", readOnly);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const credential = c.credential;
  const identity = credential?.status === "valid" && credential.metadata?.test_status === "valid" && !!credential.metadata?.login;
  const missing = credential?.metadata?.permissions?.some((p: { state: string }) => p.state === "missing");
  const continueToRepos = async () => {
    if (!identity || missing || c.busy || c.editing || readOnly || saving) return;
    setSaving(true); setError("");
    try {
      if (progress.completed_steps.includes("github")) await onComplete({ action: "complete", step: "github" });
      // Identity-only checks need repository context: navigate, do not assert
      // completion. Repository diagnostics refresh server-derived progress.
      await onNavigate("repositories");
    } catch { setError("Could not save your place. Retry to choose repositories."); }
    finally { setSaving(false); }
  };
  return <section aria-label="GitHub connection">
    <ConnectionField connection={c} label="GitHub token" prefix="github" readOnly={readOnly || saving} help="Paste a fine-grained personal access token with the repository permissions below. It is stored encrypted on the server, never in this browser." />
    <div aria-live="polite">{credential?.metadata?.login && <p>GitHub account: <strong>{credential.metadata.login}</strong></p>}</div>
    <GitHubPermissions workspaceId={workspace.id} credential={credential} />
    {missing && <p role="alert">Required permissions are missing. Replace your token with the grants listed above, then test again.</p>}
    {identity && !missing && !progress.completed_steps.includes("github") && <p>Account verified. Choose a repository next to check access. This step is not marked complete until repository access is verified.</p>}
    <form onSubmit={(e) => { e.preventDefault(); void continueToRepos(); }}>
      <div aria-live="polite">{error && <p role="alert">{error}</p>}</div>
      <button className="btn primary" disabled={!identity || !!missing || c.busy || c.editing || saving || readOnly}>{saving ? "Saving…" : "Continue to repository selection"}</button>
    </form>
  </section>;
}
