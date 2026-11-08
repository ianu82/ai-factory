import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { ArrowUpRight, ExternalLink, MessageSquarePlus, RefreshCw } from "lucide-react";
import { api, type Repo, type Signal } from "../api";
import { useWs } from "../App";
import { ago } from "../format";
import { Dot, Empty, Modal, Prose, Skeleton, useToast } from "../ui";

const SOURCE: Record<string, string> = {
  github_issue: "GitHub issue", ci_failure: "CI failing on main", smoke_failure: "Post-merge smoke", release_failure: "Release check",
  feedback: "Product feedback", improvement: "Factory learning",
};

const RULE: Record<string, string> = {
  merge_autonomy: "Merge policy", plan_autonomy: "Plan policy", review_themes: "Review findings", check_failures: "Checks",
  qa_boot: "Runtime QA", run_budget: "Budget", builder_harness: "Workers", merge_feedback: "Reviewer feedback",
};

export function SignalsPage() {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const [busy, setBusy] = useState(false);
  const [logging, setLogging] = useState(false);
  const q = useQuery({ queryKey: ["signals", ws.id], queryFn: () => api<Signal[]>(`/api/workspaces/${ws.id}/signals`) });
  const repos = useQuery({ queryKey: ["repos", ws.id], queryFn: () => api<Repo[]>(`/api/workspaces/${ws.id}/repositories`) });
  const repoName = (id: string | null) => repos.data?.find((r) => r.id === id)?.full_name;
  const refresh = () => qc.invalidateQueries({ queryKey: ["signals", ws.id] });

  const check = async () => {
    setBusy(true);
    const [gh, learned] = await Promise.allSettled([
      api<{ created: number; updated: number }>(`/api/workspaces/${ws.id}/signals/sync`, { method: "POST" }),
      api<{ created: number; updated: number }>(`/api/workspaces/${ws.id}/signals/analyze`, { method: "POST" }),
    ]);
    const parts = [];
    if (gh.status === "fulfilled") parts.push(`${gh.value.created} new from GitHub`);
    else parts.push(`GitHub: ${(gh.reason as Error).message}`);
    if (learned.status === "fulfilled") parts.push(`${learned.value.created + learned.value.updated} proposals refreshed`);
    toast(parts.join(" · "), gh.status === "rejected");
    refresh();
    setBusy(false);
  };
  const act = async (s: Signal, action: "accept" | "ignore", note?: string) => {
    try {
      const r = await api<Signal>(`/api/workspaces/${ws.id}/signals/${s.id}`, { method: "POST", json: { action, note } });
      refresh();
      if (s.source === "improvement" && action === "accept") {
        toast("Applied");
        qc.invalidateQueries({ queryKey: ["repos", ws.id] });
        qc.invalidateQueries({ queryKey: ["me"] });
      }
      if (action === "accept" && r.outcome_id) navigate(`/w/${ws.id}/outcomes/${r.outcome_id}`);
    } catch (err) {
      toast((err as Error).message, true);
    }
  };

  const all = q.data || [];
  const proposals = all.filter((s) => s.source === "improvement" && s.status === "new");
  const fresh = all.filter((s) => s.source !== "improvement" && s.status === "new");
  const rest = all.filter((s) => s.status !== "new");
  const member = ws.role !== "viewer";
  return (
    <>
      <div className="page-head">
        <h1>Signals</h1>
        <span className="muted hide-sm" style={{ fontSize: 12.5 }}>Feedback, failures and what delivery has taught the Factory. Nothing changes until someone accepts it.</span>
        <span className="spacer" />
        {member && <button className="btn sm ghost" onClick={() => setLogging(true)}><MessageSquarePlus /> Log feedback</button>}
        <button className="btn sm" onClick={check} disabled={busy}><RefreshCw /> {busy ? "Checking…" : "Check now"}</button>
      </div>
      <div className="page-body">
        {q.isLoading ? <Skeleton /> : !all.length ? (
          <Empty title="No signals yet">
            Label a GitHub issue <code className="mono">factory</code>, send product feedback with the key in Settings → Workspace, or log it here.
            Failing CI on the default branch and post-merge smoke failures arrive on their own, and the Factory proposes improvements once it has delivery history to learn from.
          </Empty>
        ) : (
          <>
            {proposals.length > 0 && (
              <>
                <div className="list-section">Proposed improvements · {proposals.length}</div>
                <div className="proposals">
                  {proposals.map((s) => <ProposalCard key={s.id} s={s} repo={repoName(s.repo_id)} act={act} />)}
                </div>
              </>
            )}
            {fresh.length > 0 && <div className="list-section">New · {fresh.length}</div>}
            <ul className="rows">{fresh.map((s) => <SignalRow key={s.id} s={s} repo={repoName(s.repo_id)} act={act} canAct={member} />)}</ul>
            {rest.length > 0 && <div className="list-section">Handled</div>}
            <ul className="rows">{rest.map((s) => <SignalRow key={s.id} s={s} repo={repoName(s.repo_id)} act={act} canAct={false} />)}</ul>
          </>
        )}
      </div>
      {logging && <LogFeedback repos={repos.data || []} onClose={() => setLogging(false)} onDone={refresh} />}
    </>
  );
}

function ProposalCard({ s, repo, act }: { s: Signal; repo?: string; act: (s: Signal, a: "accept" | "ignore", note?: string) => Promise<void> }) {
  const { ws } = useWs();
  const action = s.data.action;
  const [note, setNote] = useState(action?.note || "");
  const [busy, setBusy] = useState(false);
  const admin = ws.role === "owner" || ws.role === "admin";
  const run = async (kind: "accept" | "ignore") => {
    setBusy(true);
    await act(s, kind, action?.type === "team_note" ? note : undefined);
    setBusy(false);
  };
  return (
    <article className="proposal" aria-label={s.title}>
      <div className="proposal-tag">
        <span>{RULE[s.data.rule || ""] || "Improvement"}</span>
        {repo && <span className="mono">{repo}</span>}
        <span>{s.count > 1 ? `raised again ${ago(s.last_seen_at)}` : ago(s.first_seen_at)}</span>
      </div>
      <h4>{s.title}</h4>
      <Prose text={s.body} />
      {!!s.data.evidence?.length && (
        <ul className="evidence" aria-label="Evidence">
          {s.data.evidence.map((e, i) => (
            <li key={i}>
              <span className="ev-label">{e.label}</span>
              <span className="ev-detail">{e.detail}</span>
              {e.url ? <a className="ev-link" href={e.url} target="_blank" rel="noreferrer" aria-label={`Open ${e.label}`}><ExternalLink /></a>
                : e.outcome_id ? <Link className="ev-link" to={`/w/${ws.id}/outcomes/${e.outcome_id}`} aria-label={`Open ${e.label}`}><ArrowUpRight /></Link> : <span />}
            </li>
          ))}
        </ul>
      )}
      {action?.type === "team_note" && admin && (
        <label className="field" style={{ marginTop: 12 }}>
          <span>Team note to add <span className="faint" style={{ fontWeight: 400 }}>· builders and verifiers read it on every task in {repo || "this repository"}</span></span>
          <textarea className="textarea" value={note} onChange={(e) => setNote(e.target.value)} rows={Math.min(8, note.split("\n").length + 1)} />
        </label>
      )}
      <div className="opts">
        {admin ? (
          action?.type === "review" ? (
            <>
              <Link className="btn sm primary" to={`/w/${ws.id}/settings/repositories`}>{action.label || "Open settings"}</Link>
              <button className="btn sm" disabled={busy} onClick={() => run("accept")}>Mark fixed</button>
              <button className="btn sm ghost" disabled={busy} onClick={() => run("ignore")}>Not now</button>
            </>
          ) : (
            <>
              <button className="btn sm primary" disabled={busy || (action?.type === "team_note" && !note.trim())} onClick={() => run("accept")}>
                {busy ? "Applying…" : action?.label || "Apply"}
              </button>
              <button className="btn sm ghost" disabled={busy} onClick={() => run("ignore")}>Not now</button>
            </>
          )
        ) : <span className="muted" style={{ fontSize: 12.5 }}>A workspace admin can apply this.</span>}
      </div>
    </article>
  );
}

function SignalRow({ s, repo, act, canAct }: { s: Signal; repo?: string; act: (s: Signal, a: "accept" | "ignore") => void; canAct: boolean }) {
  const handled = s.source === "improvement" ? ({ accepted: "applied", ignored: "set aside", resolved: "no longer supported by the evidence" } as Record<string, string>)[s.status] : s.status;
  return (
    <li className="row" style={{ cursor: "default" }}>
      <Dot tone={s.status === "new" ? (s.source.includes("failure") ? "bad" : "human") : "idle"} />
      <div style={{ minWidth: 0 }}>
        <div className="title">{s.title}</div>
        <div className="sub">
          <span>{SOURCE[s.source] || s.source}</span>
          {repo && <span className="mono wide">{repo}</span>}
          {s.count > 1 && <span className="num">seen {s.count}×</span>}
          <span>{ago(s.last_seen_at)}</span>
          {s.status !== "new" && <span>{handled || s.status}</span>}
          {s.url && <a className="linkish" href={s.url} target="_blank" rel="noreferrer">source <ExternalLink /></a>}
        </div>
      </div>
      <div className="meta">
        {canAct && <><button className="btn sm ghost" onClick={() => act(s, "ignore")}>Ignore</button><button className="btn sm primary" onClick={() => act(s, "accept")}>Make it an outcome</button></>}
      </div>
    </li>
  );
}

function LogFeedback({ repos, onClose, onDone }: { repos: Repo[]; onClose: () => void; onDone: () => void }) {
  const { ws } = useWs();
  const toast = useToast();
  const [title, setTitle] = useState("");
  const [body, setBody] = useState("");
  const [repo, setRepo] = useState(repos.length === 1 ? repos[0].full_name : "");
  const [busy, setBusy] = useState(false);
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    try {
      await api(`/api/workspaces/${ws.id}/signals`, { method: "POST", json: { title, body, repo: repo || null } });
      toast("Feedback logged");
      onDone();
      onClose();
    } catch (err) {
      toast((err as Error).message, true);
      setBusy(false);
    }
  };
  return (
    <Modal title="Log product feedback" onClose={onClose}>
      <form onSubmit={submit} className="stack">
        <label className="field">
          <span>What did someone report?</span>
          <input className="input" autoFocus value={title} onChange={(e) => setTitle(e.target.value)} placeholder="Export to CSV does nothing on Safari" />
        </label>
        <label className="field">
          <span>Details <span className="faint" style={{ fontWeight: 400 }}>· optional</span></span>
          <textarea className="textarea" value={body} onChange={(e) => setBody(e.target.value)} placeholder="Who saw it, where, how often, what they expected" />
        </label>
        {repos.length > 1 && (
          <label className="field">
            <span>Repository</span>
            <select className="select" value={repo} onChange={(e) => setRepo(e.target.value)}>
              <option value="">Not sure</option>
              {repos.map((r) => <option key={r.id} value={r.full_name}>{r.full_name}</option>)}
            </select>
          </label>
        )}
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button type="button" className="btn ghost" onClick={onClose}>Cancel</button>
          <button className="btn primary" disabled={busy || title.trim().length < 3}>{busy ? "Saving…" : "Log feedback"}</button>
        </div>
      </form>
    </Modal>
  );
}
