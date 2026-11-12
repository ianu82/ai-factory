import { useEffect, useState, type ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import { onboardingQueryKey } from "../onboarding/api";

type Connection = {
  configured: boolean; installation_id: string | null; ready: boolean; status: string; account: string;
  granted_permissions: Record<string, string>; missing_permissions: Record<string, string>; manage_url: string | null;
};

/** Same connection controls in setup and settings; PAT controls remain unchanged when disabled. */
export function GitHubConnection({ workspaceId, admin, destination, children, onContinue }: {
  workspaceId: string; admin: boolean; destination: "setup" | "settings"; children: ReactNode; onContinue?: () => void;
}) {
  const qc = useQueryClient();
  const root = `/api/workspaces/${workspaceId}/github/connection`;
  const q = useQuery({ queryKey: ["github-connection", workspaceId], queryFn: () => api<Connection>(root) });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const refresh = async () => {
    await Promise.all([["me"], ["credentials", workspaceId], ["github-repos", workspaceId], ["repos", workspaceId], onboardingQueryKey(workspaceId)]
      .map((queryKey) => qc.invalidateQueries({ queryKey })));
  };
  useEffect(() => { if (q.data) void refresh(); }, [q.dataUpdatedAt]); // Refresh after installation return/remount.
  const act = async (disconnect = false) => {
    setBusy(true); setError("");
    try {
      if (disconnect) {
        await api(root, { method: "DELETE" });
        await q.refetch(); await refresh();
      } else {
        const result = await api<{ url: string }>(`${root}/start?destination=${destination}`, { method: "POST" });
        window.location.assign(result.url);
      }
    } catch (err) { setError((err as Error).message); }
    finally { setBusy(false); }
  };
  if (q.isLoading) return <p role="status">Loading GitHub connection…</p>;
  if (q.error) return <p role="alert">Could not load GitHub connection. <button className="btn" onClick={() => q.refetch()}>Retry</button></p>;
  const c = q.data!;
  return <section aria-label="GitHub App connection" style={{ minWidth: 0, overflowWrap: "anywhere" }}>
    {c.configured && <>
      <h3>GitHub App</h3>
      <p>Required repository grants: contents, pull requests and issues write; checks, actions, deployments, metadata and commit statuses read. The App owner configures these in GitHub; installing cannot change its permission configuration.</p>
      <p role="status">{c.installation_id ? `${c.account || "GitHub"}: ${c.status}${c.ready ? " · connected" : " · not ready"}` : "Not connected"}</p>
      {!!c.installation_id && <ul aria-label="GitHub App permissions">{Object.entries(c.granted_permissions).map(([name, grant]) => <li key={name}>{name.replaceAll("_", " ")}: {grant}</li>)}</ul>}
      {!!c.installation_id && Object.keys(c.missing_permissions).length > 0 && <p role="alert">Missing permissions: {Object.entries(c.missing_permissions).map(([name, grant]) => `${name.replaceAll("_", " ")} ${grant}`).join(", ")}. Ask the App owner to update grants, then reconnect.</p>}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
        {admin && <button className="btn primary" disabled={busy} onClick={() => act()}>{busy ? "Connecting…" : "Install the GitHub App"}</button>}
        {c.manage_url && <a className="btn" href={c.manage_url} target="_blank" rel="noreferrer">Manage GitHub access</a>}
        {c.ready && onContinue && <button className="btn" disabled={!admin || busy} onClick={onContinue}>Continue to repository selection</button>}
      </div>
    </>}
    {!!c.installation_id && <p>{admin && <button className="btn" disabled={busy} onClick={() => act(true)}>Disconnect GitHub App</button>} Disconnect to use a saved token or the server fallback instead.</p>}
    {error && <p role="alert">{error}</p>}
    {!c.configured && children}
    {!admin && c.configured && <p>An owner or admin must connect GitHub.</p>}
  </section>;
}
