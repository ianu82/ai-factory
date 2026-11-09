import { useQuery } from "@tanstack/react-query";
import { api, type Credential, type CredentialsResponse } from "../api";

/** One API-owned permission contract for every token recipe in the app. */
export function GitHubPermissions({ workspaceId, credential }: { workspaceId: string; credential?: Credential }) {
  const q = useQuery({ queryKey: ["credentials", workspaceId], queryFn: () => api<CredentialsResponse>(`/api/workspaces/${workspaceId}/credentials`) });
  return <div>
    <h3>Required repository permissions</h3>
    {q.isLoading && <p role="status">Loading required permissions…</p>}
    {q.isError && <p role="alert">Could not load permission instructions. <button className="btn" onClick={() => q.refetch()}>Retry permissions</button></p>}
    <ul>{q.data?.github_permissions?.map((p) => {
      const evidence = credential?.metadata?.permissions?.find((entry: { permission: string }) => entry.permission === p.permission);
      const state = evidence?.state === "present" || evidence?.state === "missing" ? evidence.state : "unverified";
      return <li key={p.permission}>{p.label}: {p.required === "write" ? "Read and write" : "Read-only"} — <strong>{state}</strong></li>;
    })}</ul>
    <p>In GitHub, open Settings → Developer settings → Personal access tokens → Fine-grained tokens → Generate new token. Choose the resource owner (your account or the organization that owns the repositories), then select the repositories the Factory should access. Under Repository permissions, set every permission listed above. Metadata is always required.</p>
    <p>Your organization may require an owner to approve the token or authorize access before it works. Repository-specific access is checked after you choose a repository. Unverified means a read-only check cannot prove that grant; it does not mean present.</p>
  </div>;
}
