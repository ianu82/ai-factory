import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useNavigate, useSearchParams } from "react-router-dom";
import { api, type Meta } from "../api";

const ERRORS: Record<string, string> = {
  expired: "Your sign-in took too long. Please try again.",
  state: "Sign-in could not be verified. Please try again.",
  token: "MindsHub did not complete the sign-in.",
  identity: "MindsHub could not confirm your account, or this Factory is not open to it yet.",
};

const FLOW = [
  ["Plan", "Outcomes, not prompts", "You describe the outcome. A planner reads your repositories and splits it into tasks."],
  ["Build", "Parallel by default", "Tasks run side by side in isolated sandboxes with Native, Codex or Anton workers."],
  ["Verify", "Never self-reviewed", "A fresh, independent session checks every acceptance criterion with evidence."],
  ["Ship", "Released, not just merged", "The Factory opens the PR, waits for CI, merges under your policy and smoke-tests the result."],
];

export function SignIn({ meta }: { meta?: Meta }) {
  const [params] = useSearchParams();
  const [key, setKey] = useState("");
  const [useKey, setUseKey] = useState(true);
  const [email, setEmail] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(ERRORS[params.get("error") || ""] || "");
  const navigate = useNavigate();
  const qc = useQueryClient();

  const done = async () => {
    await qc.invalidateQueries({ queryKey: ["me"] });
    navigate("/");
  };
  const withKey = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      await api("/auth/api-key", { method: "POST", json: { key, use_for_inference: useKey } });
      await done();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  };
  const dev = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    try {
      await api("/auth/dev", { method: "POST", json: { email, name: email.split("@")[0] } });
      await done();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="signin">
      <section className="signin-art" aria-hidden="true">
        <div className="brand" style={{ padding: 0 }}>
          <img src="/minds-icon.svg" alt="" />
          <span className="word">FACTORY</span>
        </div>
        <ul className="flow">
          {FLOW.map(([g, headline, text]) => (
            <li key={g}><span className="g">{g}</span><span><b>{headline}</b><span>{text}</span></span></li>
          ))}
        </ul>
        <span className="faint" style={{ fontSize: 12 }}>MindsHub · AI Factory {meta?.version ?? ""}</span>
      </section>
      <section className="signin-form">
        <div className="signin-card">
          <h1>Sign in to Factory</h1>
          {error && <div className="err" role="alert">{error}</div>}
          {meta?.auth.oidc && (
            <>
              <a className="btn primary block" href="/auth/login" style={{ height: 36 }}>Continue with MindsHub</a>
              <div className="or">or</div>
            </>
          )}
          {meta?.auth.api_key !== false && (
            <form onSubmit={withKey} style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              <label className="field">
                <span>MindsHub API key</span>
                <input className="input mono" type="password" autoComplete="off" spellCheck={false} placeholder="mdb_…"
                  value={key} onChange={(e) => setKey(e.target.value)} required />
                <span className="hint">Create one in <a className="linkish" href="https://console.mindshub.ai" target="_blank" rel="noreferrer">MindsHub console</a> → API keys. MindsHub confirms who you are and which organization you belong to.</span>
              </label>
              <label className="check">
                <input type="checkbox" checked={useKey} onChange={(e) => setUseKey(e.target.checked)} />
                Also use this key for my workspace's AI models
              </label>
              <button className={`btn ${meta?.auth.oidc ? "" : "primary"} block`} disabled={busy || !key.trim()} style={{ height: 36 }}>
                {busy ? "Checking with MindsHub…" : "Sign in with API key"}
              </button>
            </form>
          )}
          {meta?.auth.dev && (
            <form onSubmit={dev} style={{ display: "flex", gap: 8, marginTop: 8 }}>
              <input className="input" placeholder="Local development email" value={email} onChange={(e) => setEmail(e.target.value)} required />
              <button className="btn" disabled={busy}>Dev sign-in</button>
            </form>
          )}
        </div>
      </section>
    </div>
  );
}
