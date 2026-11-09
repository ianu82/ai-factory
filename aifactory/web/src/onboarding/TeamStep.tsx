import { useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type Role } from "../api";
import { useWs } from "../App";
import { type OnboardingStepProps } from "./api";

const ROLE_RANK: Record<Role, number> = { viewer: 0, member: 1, admin: 2, owner: 3 };
type Member = { user: { id: string; name: string; email: string | null }; role: Role };

export function TeamStep({ workspace, readOnly, onComplete, onNavigate }: OnboardingStepProps) {
  const { me } = useWs();
  const qc = useQueryClient();
  const members = useQuery({ queryKey: ["members", workspace.id], queryFn: () => api<Member[]>(`/api/workspaces/${workspace.id}/members`) });
  const [busy, setBusy] = useState(false);
  const lock = useRef(false);
  const [error, setError] = useState("");
  const run = async (action: () => Promise<void>) => {
    if (lock.current || readOnly) return;
    lock.current = true; setBusy(true); setError("");
    try { await action(); } catch (err) { setError((err as Error).message); }
    finally { lock.current = false; setBusy(false); }
  };
  return <section aria-label="Workspace team">
    <p>Organization: <strong>{workspace.org_name}</strong> · Workspace: <strong>{workspace.name}</strong></p>
    <p>Colleagues in this MindsHub organization join by signing in with their own MindsHub API keys. Never share your key. Organization owners and admins become workspace admins when they join.</p>
    <p>This list shows existing workspace memberships, not organization users who have never signed in.</p>
    {members.isLoading && <p role="status">Loading memberships…</p>}
    {members.isError && <p role="alert">Could not load memberships. <button className="btn" onClick={() => members.refetch()}>Retry members</button></p>}
    {members.data?.map((member) => <div className="s-line" key={member.user.id}>
      <div className="l"><b>{member.user.name || member.user.email || member.user.id}{member.user.id === me.user.id ? " (you)" : ""}</b><span>{member.user.email}</span></div>
      <label className="field" htmlFor={`team-role-${member.user.id}`}>Role for {member.user.name || member.user.email || member.user.id}
        <select id={`team-role-${member.user.id}`} className="select" value={member.role} disabled={readOnly || busy || member.user.id === me.user.id}
          onChange={(e) => void run(async () => {
            await api(`/api/workspaces/${workspace.id}/members/${member.user.id}`, { method: "PATCH", json: { role: e.target.value } });
            await qc.invalidateQueries({ queryKey: ["members", workspace.id] });
          })}>
          {(Object.keys(ROLE_RANK) as Role[]).map((role) => <option key={role} value={role} disabled={ROLE_RANK[role] > ROLE_RANK[workspace.role]}>{role}</option>)}
        </select>
      </label>
    </div>)}
    <p className="hint">As in Settings, you cannot change your own role here. You can assign roles up to your own rank.</p>
    {error && <p role="alert">{error}</p>}
    <button className="btn primary" disabled={readOnly || busy || !members.data} onClick={() => void run(async () => {
      await onComplete({ action: "complete", step: "team", acknowledged: true });
      await onNavigate("outcome");
    })}>Continue to first outcome</button>
  </section>;
}
