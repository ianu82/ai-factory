import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronDown, ChevronRight, ExternalLink, GitPullRequest } from "lucide-react";
import { api, type Artifact, type Run, type Task } from "../api";
import { useWs } from "../App";
import { duration, HARNESS_LABEL, money, tokens } from "../format";
import { DiffView, GateTrack, Mark, Prose, Status } from "../ui";

const STAGE_TEXT: Record<string, string> = {
  prepare: "Preparing a fresh checkout", build: "Building", revise: "Revising", checks: "Running checks",
  verify: "Independent verification", publish: "Publishing", ci: "Waiting for CI", merge: "Merging", smoke: "Smoke testing",
  release: "Watching the release", done: "Done",
};

export function TaskCard({ task, runs, defaultOpen = false }: { task: Task; runs: Run[]; defaultOpen?: boolean }) {
  const [open, setOpen] = useState(defaultOpen);
  const label = task.status === "running" ? STAGE_TEXT[task.stage] || "Running" : undefined;
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
          <span className="muted">{task.repo}</span>
          {task.attempt > 0 && <span className="muted">attempt {task.attempt}</span>}
          <span className="muted num">{money(task.cost_usd, task.cost_known)}</span>
          {task.depends_on.length > 0 && task.status === "pending" && <span className="muted">after {task.depends_on.join(", ")}</span>}
        </div>
        {task.blocked_reason && task.status !== "completed" && (
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
  const attempts = task.evidence.attempts || [];
  const lastVerdict = [...attempts].reverse().find((a) => a.verdict)?.verdict;
  const byCriterion = new Map((lastVerdict?.criteria || []).map((c) => [c.criterion, c]));
  const diffs = (artifacts.data || []).filter((a) => a.kind === "diff");
  const reports = (artifacts.data || []).filter((a) => ["check", "smoke", "report", "setup"].includes(a.kind));

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
              {task.evidence.ci.state === "none" ? "No CI checks reported for this repository" : task.evidence.ci.state}
              {(task.evidence.ci.checks || []).map((c) => (
                <div key={c.name} className="checkline"><Mark status={c.conclusion === "success" ? "pass" : c.conclusion ? "fail" : "none"} />
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
              <Mark status={r.status === "succeeded" ? "pass" : r.status === "running" ? "none" : "fail"} />
              <span>{r.role} · {HARNESS_LABEL[r.harness]} · <span className="mono">{r.model}</span></span>
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
    </div>
  );
}

const ATTEMPT_TEXT: Record<string, string> = {
  verified: "Passed checks and independent verification",
  verification_failed: "Independent verification failed — feedback sent to builder",
  checks_failed: "Configured checks failed — feedback sent to builder",
  builder_failed: "Builder session ended without a report",
  builder_blocked: "Builder asked for a decision",
  policy: "Change rejected by repository policy",
};
