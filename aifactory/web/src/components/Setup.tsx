import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { Check } from "lucide-react";
import { api, type Credential, type Repo } from "../api";
import { useWs } from "../App";

type CredResponse = { items: Credential[]; server_fallbacks: Record<string, boolean> };

export function useSetupState() {
  const { ws } = useWs();
  const creds = useQuery({ queryKey: ["credentials", ws.id], queryFn: () => api<CredResponse>(`/api/workspaces/${ws.id}/credentials`) });
  const repos = useQuery({ queryKey: ["repos", ws.id], queryFn: () => api<Repo[]>(`/api/workspaces/${ws.id}/repositories`) });
  const ok = (kind: string) => {
    const c = creds.data?.items.find((x) => x.kind === kind);
    return (c?.configured && c.status !== "invalid") || !!creds.data?.server_fallbacks[kind];
  };
  const models = ok("mindshub_api_key") || ok("openai_api_key");
  const github = ok("github_token");
  const hasRepos = (repos.data?.length || 0) > 0;
  return { loading: creds.isLoading || repos.isLoading, models, github, hasRepos, ready: models && github && hasRepos };
}

export function SetupChecklist() {
  const { ws, openCapture } = useWs();
  const s = useSetupState();
  const base = `/w/${ws.id}/settings`;
  const steps = [
    { done: s.models, title: "Connect AI models", text: "Save a MindsHub Inference key. It stays encrypted on the server and never enters a sandbox.", to: `${base}/credentials`, cta: "Add key" },
    { done: s.github, title: "Connect GitHub", text: "A token that can push branches and open pull requests on the repositories you choose.", to: `${base}/credentials`, cta: "Connect" },
    { done: s.hasRepos, title: "Add a repository", text: "The Factory detects setup and test commands; review them before the first run.", to: `${base}/repositories`, cta: "Add" },
  ];
  return (
    <div className="setup">
      <h2>Set up {ws.name}</h2>
      <p>Three steps, then describe your first outcome. Routine work runs unattended under the policies you set.</p>
      <ol className="steps">
        {steps.map((st) => (
          <li key={st.title}>
            <span className={`done-mark ${st.done ? "ok" : ""}`}><Check strokeWidth={3} /></span>
            <span><b>{st.title}</b><span>{st.text}</span></span>
            {st.done ? <span className="status good">Done</span> : ws.role === "owner" || ws.role === "admin"
              ? <Link className="btn sm" to={st.to}>{st.cta}</Link> : <span className="faint" style={{ fontSize: 12 }}>Ask an admin</span>}
          </li>
        ))}
        <li>
          <span className={`done-mark`}><Check strokeWidth={3} /></span>
          <span><b>Describe an outcome</b><span>What should be true when the work is done? The Factory plans, builds, verifies and ships it.</span></span>
          <button className="btn sm primary" disabled={!s.ready} onClick={openCapture}>New outcome</button>
        </li>
      </ol>
    </div>
  );
}
