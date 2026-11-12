import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ChevronDown, ChevronRight, ExternalLink, GitPullRequest } from "lucide-react";
import { api, type Artifact, type Run, type Task } from "../api";
import { useWs } from "../App";
import { duration, HARNESS_LABEL, money, tokens } from "../format";
import { DiffView, GateTrack, Mark, Prose, Status, useToast } from "../ui";

const STAGE_TEXT: Record<string, string> = {
  prepare: "Preparing a fresh checkout", build: "Building", revise: "Revising", checks: "Running checks",
  verify: "Independent verification", publish: "Publishing", ci: "Checking CI", merge: "Merging", smoke: "Smoke testing",
  ci_repair: "Repairing CI", revalidate: "Revalidating after rebase", revalidate_prepare: "Preparing revalidation",
  revalidate_setup: "Setting up the rebased candidate", revalidate_checks: "Checking the rebased candidate",
  revalidate_verify: "Independent review after rebase",
  reverify: "Independent review after rebase", revalidate_publish: "Publishing the rebased candidate",
  release: "Watching the release", revert: "Reverting the merge", done: "Done",
};

export function TaskCard({ task, runs, defaultOpen = false }: { task: Task; runs: Run[]; defaultOpen?: boolean }) {
  const [open, setOpen] = useState(defaultOpen);
  // Parked at stage "review": CI passed and the pull request's reviewers are next.
  const reviewWait = task.status === "waiting_ci" && task.stage === "review";
  const label = task.status === "running" ? STAGE_TEXT[task.stage] || "Running"
    : reviewWait ? "Waiting for review" : undefined;
  return (
    <div className="task">
      <div className="task-head" onClick={() => setOpen(!open)} role="button" tabIndex={0} aria-expanded={open}
        onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), setOpen(!open))}>
        <span className="key">{task.key}</span>
        <span className="t" title={task.title}>{task.title}</span>
        <span style={{ display: "flex", alignItems: "center", gap: 10 }}>
          {task.pr_url && (
            <a className="linkish" href={task.pr_url} target="_blank" rel="noreferrer" onClick={(e) => e.stopPropagation()}>
              <GitPullRequest /> #{task.pr_number}
            </a>
          )}
          {open ? <ChevronDown size={15} className="muted" /> : <ChevronRight size={15} className="muted" />}
        </span>
      </div>
      <div className="task-track">
        <GateTrack task={task} large />
        <div style={{ display: "flex", gap: 14, marginTop: 8, fontSize: 12.5, alignItems: "center", flexWrap: "wrap" }}>
          <Status status={task.status} kind="task" label={label} />
          {task.status === "waiting_ci" && <span className="muted">Not holding a task slot</span>}
          <span className="muted">{task.repo}</span>
          {task.attempt > 0 && <span className="muted">attempt {task.attempt}</span>}
          <span className="muted num">{money(task.cost_usd, task.cost_known)}</span>
          {task.depends_on.length > 0 && task.status === "pending" && <span className="muted">after {task.depends_on.join(", ")}</span>}
        </div>
        {task.blocked_reason && task.status !== "completed" && task.status !== "waiting_ci" && (
          <div className="prose clamp" style={{ marginTop: 8, fontSize: 12.5, color: "var(--bad)" }}>{task.blocked_reason}</div>
        )}
      </div>
      {open && <TaskBody task={task} runs={runs} />}
    </div>
  );
}

function TaskBody({ task, runs }: { task: Task; runs: Run[] }) {
  const { ws } = useWs();
  const artifacts = useQuery({
    queryKey: ["outcome", ws.id, "artifacts", task.id, task.updated_at],
    queryFn: () => api<Artifact[]>(`/api/workspaces/${ws.id}/tasks/${task.id}/artifacts`),
  });
  const [view, setView] = useState<Artifact | null>(null);
  const ciPending = task.status === "waiting_ci" && task.stage !== "review";
  const attempts = task.evidence.attempts || [];
  const lastVerdict = [...attempts].reverse().find((a) => a.verdict)?.verdict;
  const byCriterion = new Map((lastVerdict?.criteria || []).map((c) => [c.criterion, c]));
  const diffs = (artifacts.data || []).filter((a) => a.kind === "diff");
  const reports = (artifacts.data || []).filter((a) => ["check", "smoke", "report", "setup", "log"].includes(a.kind));
  const shots = (artifacts.data || []).filter((a) => a.kind === "screenshot");
  const lastShots = shots.filter((a) => a.data.attempt === shots[shots.length - 1]?.data.attempt).slice(-8);
  const appRun = [...attempts].reverse().find((a) => a.app_run)?.app_run;

  const open = async (a: Artifact) => {
    if (view?.id === a.id) return setView(null);
    setView(await api<Artifact>(`/api/workspaces/${ws.id}/artifacts/${a.id}`));
  };

  return (
    <div className="task-body">
      <div>
        <div className="muted" style={{ fontSize: 11.5, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>
          Acceptance {lastVerdict ? `· verified by ${HARNESS_LABEL[task.verifier_harness]}` : "· not yet verified"}
        </div>
        <ul className="crit">
          {task.acceptance.map((c) => {
            const v = byCriterion.get(c) || [...byCriterion.values()].find((x) => x.criterion.slice(0, 40) === c.slice(0, 40));
            return (
              <li key={c}>
                <Mark status={v?.status || "none"} />
                <span>{c}</span>
                {v?.evidence && <span className="ev">{v.evidence}</span>}
              </li>
            );
          })}
        </ul>
        {lastVerdict?.findings && lastVerdict.findings.length > 0 && (
          <ul className="crit" style={{ marginTop: 8 }}>
            {lastVerdict.findings.map((f, i) => (
              <li key={i}><Mark status={f.severity === "minor" ? "none" : "fail"} /><span>{f.severity}: {f.description}</span></li>
            ))}
          </ul>
        )}
      </div>

      {attempts.length > 0 && (
        <div>
          <div className="muted" style={{ fontSize: 11.5, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>Attempts</div>
          <div className="attempts">
            {attempts.map((a, i) => (
              <div key={i} className="a">
                <span className="n">#{a.n}</span>
                <span>{ATTEMPT_TEXT[a.result] || a.result}</span>
                {a.checks && a.checks.length > 0 && (
                  <span className="muted">· checks {a.checks.filter((c) => c.ok).length}/{a.checks.length}</span>
                )}
                {(a.detail || a.error || a.reason) && <span className="muted" title={a.detail || a.error || a.reason}>· {(a.detail || a.error || a.reason || "").slice(0, 90)}</span>}
              </div>
            ))}
          </div>
        </div>
      )}

      <dl className="kv">
        <dt>Built by</dt><dd>{HARNESS_LABEL[task.harness]} · verified by a fresh {HARNESS_LABEL[task.verifier_harness]} session</dd>
        {task.branch && <><dt>Branch</dt><dd className="mono">{task.branch}</dd></>}
        {task.evidence.ci && (
          <>
            <dt>CI</dt>
            <dd>
              {ciPending ? "pending" : task.evidence.ci.state === "none" ? "No CI checks reported for this repository"
                : task.evidence.ci.state}
              {(task.evidence.ci.checks || []).map((c) => (
                <div key={c.name} className="checkline">
                  <Mark status={ciPending || !c.conclusion ? "none" : c.conclusion === "success" ? "pass" : "fail"} />
                  {c.url ? <a className="linkish" href={c.url} target="_blank" rel="noreferrer">{c.name} <ExternalLink /></a> : c.name}</div>
              ))}
            </dd>
          </>
        )}
        {task.merge_sha && <><dt>Merged</dt><dd className="mono">{task.merge_sha.slice(0, 10)}</dd></>}
        {task.evidence.smoke && <><dt>Smoke</dt><dd>{task.evidence.smoke.state.replace("_", " ")}</dd></>}
        {task.evidence.release && <><dt>Release</dt><dd>{task.evidence.release.note || task.evidence.release.state}{task.evidence.release.url ? ` · ${task.evidence.release.url}` : ""}</dd></>}
        <dt>Sessions</dt>
        <dd>
          {runs.length === 0 ? "None yet" : runs.map((r) => (
            <div key={r.id} className="checkline">
              <Mark status={r.status === "succeeded" ? "pass"
                : r.status === "running" || r.status === "interrupted" ? "none" : "fail"} />
              <span>{r.role} · {HARNESS_LABEL[r.harness]} · <span className="mono">{r.model}</span>
                {r.status === "interrupted" && " · Interrupted"}</span>
              <span className="muted num">{tokens(r.input_tokens + r.output_tokens)} tok · {money(r.cost_usd, r.cost_known)} · {duration(r.started_at, r.finished_at)}</span>
            </div>
          ))}
        </dd>
      </dl>

      {task.spec && (
        <details>
          <summary className="linkish" style={{ cursor: "pointer" }}>Specification</summary>
          <div style={{ marginTop: 6, fontSize: 12.5 }}><Prose text={task.spec} /></div>
        </details>
      )}

      {appRun && (
        <div>
          <div className="muted" style={{ fontSize: 11.5, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>
            Runtime QA {appRun.started ? `· app ready in ${appRun.ready_seconds}s` : "· app did not start"}
          </div>
          <div className="checks">
            {(appRun.pages || []).map((p) => (
              <div key={p.path} className="checkline"><Mark status={(p.status || 500) < 400 ? "pass" : "fail"} />
                <code>{p.path}</code><span className="muted">{p.status || p.error} {p.title ? `· ${p.title}` : ""}</span></div>
            ))}
            {(appRun.probes || []).map((p) => (
              <div key={`${p.method}${p.path}`} className="checkline"><Mark status={p.ok ? "pass" : "fail"} />
                <code>{p.method} {p.path}</code><span className="muted">{p.status}</span></div>
            ))}
          </div>
          {lastShots.length > 0 && <div className="shots">{lastShots.map((a) => <Shot key={a.id} artifact={a} />)}</div>}
        </div>
      )}

      {(diffs.length > 0 || reports.length > 0) && (
        <div>
          <div className="muted" style={{ fontSize: 11.5, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>Evidence</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {diffs.slice(-1).map((a) => (
              <button key={a.id} className={`chip ${view?.id === a.id ? "on" : ""}`} onClick={() => open(a)}>
                Diff <span className="n">+{a.data.additions} −{a.data.deletions}</span>
              </button>
            ))}
            {reports.slice(-12).map((a) => (
              <button key={a.id} className={`chip ${view?.id === a.id ? "on" : ""}`} onClick={() => open(a)} title={a.name}>
                {a.data.ok === false ? "✗ " : a.data.ok ? "✓ " : ""}{a.kind === "report" ? "QA report" : a.name.slice(0, 28)}
              </button>
            ))}
          </div>
          {view && (
            <div style={{ marginTop: 10 }}>
              {view.kind === "diff" ? <DiffView text={view.content || ""} /> : <pre className="log">{view.content || "(empty)"}</pre>}
            </div>
          )}
        </div>
      )}
      <TakeOver task={task} />
    </div>
  );
}

const FINISHED = new Set(["completed", "abandoned", "cancelled", "reverted", "handed_over"]);

function TakeOver({ task }: { task: Task }) {
  const { ws } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const [asking, setAsking] = useState(false);
  const [busy, setBusy] = useState(false);
  if (FINISHED.has(task.status) || ws.role === "viewer") return null;
  const go = async () => {
    setBusy(true);
    try {
      const r = await api<{ published?: boolean; pr_url?: string; error?: string }>(`/api/workspaces/${ws.id}/tasks/${task.id}/take-over`, { method: "POST" });
      toast(r.pr_url ? "Handed over. The pull request is yours." : r.error ? `Handed over, but the candidate could not be pushed: ${r.error}` : "Handed over. There was no candidate to push yet.", Boolean(r.error));
      qc.invalidateQueries({ queryKey: ["outcome", ws.id] });
      qc.invalidateQueries({ queryKey: ["outcomes", ws.id] });
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(false);
      setAsking(false);
    }
  };
  return (
    <div className="takeover">
      {asking ? (
        <>
          <span>The Factory stops working on {task.key} for good. {task.pr_url ? "Its pull request stays open for you." : "Its latest candidate is pushed to a branch with a draft pull request, unverified."}</span>
          <span className="spacer" />
          <button className="btn sm ghost" onClick={() => setAsking(false)} disabled={busy}>Keep the Factory on it</button>
          <button className="btn sm danger" onClick={go} disabled={busy}>{busy ? "Handing over…" : "Take over"}</button>
        </>
      ) : (
        <>
          <span className="muted">Rather finish this yourself?</span>
          <button className="btn sm ghost" onClick={() => setAsking(true)}>Take over {task.key}</button>
        </>
      )}
    </div>
  );
}

function Shot({ artifact }: { artifact: Artifact }) {
  const { ws } = useWs();
  const [open, setOpen] = useState(false);
  const q = useQuery({
    queryKey: ["outcome", ws.id, "artifact", artifact.id],
    queryFn: () => api<Artifact>(`/api/workspaces/${ws.id}/artifacts/${artifact.id}`),
    staleTime: Infinity,
  });
  const src = q.data?.content ? `data:image/png;base64,${q.data.content}` : "";
  return (
    <figure className="shot">
      <button onClick={() => setOpen(!open)} aria-label={`Screenshot ${artifact.name}`} className={open ? "open" : ""}>
        {src ? <img src={src} alt={artifact.name} /> : <div className="skeleton" style={{ height: 90 }} />}
      </button>
      <figcaption>{artifact.name}{artifact.data.status ? ` · ${artifact.data.status}` : ""}</figcaption>
    </figure>
  );
}

const ATTEMPT_TEXT: Record<string, string> = {
  verified: "Passed checks and independent verification",
  verification_failed: "Independent verification failed — feedback sent to builder",
  checks_failed: "Configured checks failed — feedback sent to builder",
  builder_failed: "Builder session ended without a report",
  builder_blocked: "Builder asked for a decision",
  policy: "Change rejected by repository policy",
  infra: "Model provider or sandbox unavailable — not counted as an attempt",
  checks_preexisting: "Failing checks also fail on the base commit — verification continues",
};
