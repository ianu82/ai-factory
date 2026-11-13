import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, type ChangeSize, type MetricDefinition, type RatioMetric, type Run, type ScorerDefinition, type ScorerResult, type WorkspaceStats } from "../api";
import { useWs } from "../App";
import { ago, duration, HARNESS_LABEL, money, tokens } from "../format";
import { Dot, Empty, Modal, Skeleton } from "../ui";

const RUN_TONE: Record<string, "good" | "bad" | "active" | "idle"> = {
  succeeded: "good", failed: "bad", error: "bad", running: "active", cancelled: "idle", interrupted: "idle",
};

// Benchmark source markers are data, not title heuristics. No benchmark execution API is required.
function hasOutcome(run: Run) {
  return run.result?.source !== "benchmark" && !!run.outcome_id && run.outcome_number != null;
}

export function RunsPage() {
  const { ws } = useWs();
  const [draftVersion, setDraftVersion] = useState("");
  const [version, setVersion] = useState("");
  const [selected, setSelected] = useState<{ workspaceId: string; run: Run } | null>(null);
  const runs = useQuery({
    queryKey: ["runs", ws.id, { config_version: version }],
    queryFn: () => api<Run[]>(`/api/workspaces/${ws.id}/runs?limit=200${version ? `&config_version=${encodeURIComponent(version)}` : ""}`),
    refetchInterval: 15_000,
  });
  const stats = useQuery({ queryKey: ["runs", ws.id, "stats"], queryFn: () => api<WorkspaceStats>(`/api/workspaces/${ws.id}/stats`), refetchInterval: 15_000 });
  const s = stats.data;
  return (
    <>
      <div className="page-head"><h1>Runs</h1><span className="muted" style={{ fontSize: 12.5 }}>Every agent session, what it cost and how it ended</span></div>
      <div className="page-body">
        <div className="toolbar" style={{ display: "block" }}>
          <h2 style={{ fontSize: 14 }}>Workspace delivery metrics</h2>
          <p className="muted">All delivery history, independent of the version filter.</p>
          {stats.isLoading && <div role="status">Loading metrics…</div>}
          {stats.isError && <div role="alert">Could not load metrics: {stats.error.message} <button className="btn sm" onClick={() => stats.refetch()}>Retry</button></div>}
          {s && <>
            <div className="muted" style={{ fontSize: 12.5, marginBottom: 14 }}>
              {s.tasks.completed} of {s.tasks.total} tasks delivered · {money(s.cost.usd, s.cost.known)} total cost · {s.human.decisions} human decisions · {s.human.interventions} interventions
              {s.tasks.smoke_failed > 0 && <span style={{ color: "var(--bad)" }}> · {s.tasks.smoke_failed} smoke failures after merge</span>}
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(230px, 1fr))", gap: 18, fontSize: 12.5 }}>
              <Ratio label="Autonomy" metric={s.metrics.autonomy} />
              <Ratio label="First-attempt pass rate" metric={s.metrics.first_attempt_pass_rate} />
              <Ratio label="Verifier false-accept rate" metric={s.metrics.verifier_false_accept_rate} />
              <div>
                <b>Median cost per delivered task</b>
                {(["small", "medium", "large"] as ChangeSize[]).map(size => {
                  const sample = s.metrics.median_delivered_task_cost.value[size];
                  return <div key={size} style={{ marginTop: 6 }}>
                    <span style={{ textTransform: "capitalize" }}>{size}</span>: <b className="num">{sample.value === null ? "No data" : money(sample.value, true)}</b>
                    <span className="muted"> · {sample.sample_count} {sample.sample_count === 1 ? "sample" : "samples"}</span>
                  </div>;
                })}
                <div className="muted" style={{ marginTop: 6 }}>
                  {s.metrics.median_delivered_task_cost.sample_count} measured · {s.metrics.median_delivered_task_cost.unknown_count} unknown · {s.metrics.median_delivered_task_cost.excluded_count} excluded
                </div>
                <div className="muted">Unknown size: {s.metrics.median_delivered_task_cost.unknown_size_count} · Unknown cost: {s.metrics.median_delivered_task_cost.unknown_cost_count} (may overlap)</div>
                <Definition definition={s.metrics.median_delivered_task_cost.definition} label="Median cost per delivered task" />
              </div>
            </div>
          </>}
        </div>
        <form className="toolbar" style={{ flexWrap: "wrap", gap: 10 }} onSubmit={e => { e.preventDefault(); setVersion(draftVersion.trim()); }}>
          <label htmlFor="config-version">Configuration version</label>
          <input id="config-version" className="input mono" value={draftVersion} onChange={e => setDraftVersion(e.target.value)} placeholder="Exact version" style={{ maxWidth: 240 }} />
          <button className="btn sm" type="submit">Filter</button>
          <button className="btn sm" type="button" onClick={() => { setDraftVersion(""); setVersion(""); }}>Clear</button>
          <span className="muted" style={{ fontSize: 12 }}>Exact match only. Unknown legacy versions appear when no filter is applied.</span>
        </form>
        <div>
          {runs.isError ? <Empty title="Could not load runs" action={<button className="btn" onClick={() => runs.refetch()}>Retry</button>}>{runs.error.message}</Empty>
            : runs.isLoading ? <Skeleton /> : !runs.data?.length ? (
            <Empty title={version ? "No runs for this version" : "No runs yet"}>{version ? `No exact match for ${version}. Clear the filter to see all runs, including Unknown legacy versions.` : "Planner, builder and verifier sessions appear here as outcomes are worked on."}</Empty>
          ) : (
            <div style={{ overflowX: "auto" }}><table className="table">
              <thead>
                <tr><th></th><th>Session</th><th>Outcome</th><th>Config version</th><th className="hide-sm">Model</th><th className="r">Turns</th><th className="r">Tokens</th><th className="r">Cost</th><th className="r hide-sm">Duration</th><th className="r">Started</th></tr>
              </thead>
              <tbody>
                {runs.data.map((r) => (
                  <tr key={r.id} title={r.error || undefined}>
                    <td style={{ width: 14 }}><Dot tone={RUN_TONE[r.status] || "idle"} title={r.status} /></td>
                    <td><button className="linkish" onClick={() => setSelected({ workspaceId: ws.id, run: r })} aria-label={`Run details: ${r.role}, ${r.id}`}><b style={{ fontWeight: 550 }}>{r.role.replaceAll("_", " ")}</b></button> <span className="muted">· {HARNESS_LABEL[r.harness] || r.harness}{r.attempt > 1 ? ` · attempt ${r.attempt}` : ""}</span>
                      {r.error && <div className="muted" style={{ fontSize: 12, maxWidth: 380, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.error}</div>}
                      <button className="btn sm" style={{ display: "block", marginTop: 6 }} onClick={() => setSelected({ workspaceId: ws.id, run: r })}>Details / scores{r.score_summary?.count ? ` (${r.score_summary.count})` : ""}</button>
                      {r.scores?.map(score => <div key={score.id} className="muted" style={{ fontSize: 12 }}>{score.scorer_id}: {verdict(score)}</div>)}
                    </td>
                    <td style={{ maxWidth: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {hasOutcome(r) ? <Link className="linkish" to={`/w/${ws.id}/outcomes/${r.outcome_id}`}><span className="ref">#{r.outcome_number}{r.task_key ? ` ${r.task_key}` : ""}</span> {r.outcome_title}</Link> : <span className="muted">—</span>}
                    </td>
                    <td className="mono">{r.config_version || <span className="muted">Unknown</span>}</td>
                    <td className="mono hide-sm">{r.model}</td>
                    <td className="r num">{r.turns}</td>
                    <td className="r num">{tokens(r.input_tokens + r.output_tokens)}</td>
                    <td className="r num">{r.status === "running" && !r.cost_usd
                      ? <span className="muted" title="Cost is recorded when the session ends">—</span>
                      : money(r.cost_usd, r.cost_known)}</td>
                    <td className="r num hide-sm">{duration(r.started_at, r.finished_at)}</td>
                    <td className="r num muted">{ago(r.started_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table></div>
          )}
          {!!runs.data?.length && <p className="muted" style={{ padding: "0 22px", fontSize: 12 }}>Showing up to 200 most recent runs{version ? ` matching ${version}` : ""}.</p>}
        </div>
      </div>
      {selected?.workspaceId === ws.id && <RunDetails key={`${ws.id}:${selected.run.id}`} run={runs.data?.find(r => r.id === selected.run.id) || selected.run} workspaceId={ws.id} canJudge={ws.role !== "viewer"} onClose={() => setSelected(null)} />}
    </>
  );
}

function Definition({ definition, label }: { definition: MetricDefinition; label: string }) {
  return <details style={{ marginTop: 8 }}>
    <summary aria-label={`${label} definition`} style={{ cursor: "pointer" }}>Definition</summary>
    <dl style={{ margin: "8px 0", overflowWrap: "anywhere" }}>
      {Object.entries(definition).map(([key, value]) => <div key={key} style={{ marginBottom: 6 }}>
        <dt style={{ textTransform: "capitalize", fontWeight: 550 }}>{key.replaceAll("_", " ")}</dt>
        <dd className="muted" style={{ marginLeft: 0 }}>{typeof value === "string" ? value : Object.entries(value).map(([size, [min, max]]) => `${size}: ${max === null ? `${min}+` : `${min}–${max}`} changed lines`).join("; ")}</dd>
      </div>)}
    </dl>
  </details>;
}

function Ratio({ label, metric }: { label: string; metric: RatioMetric }) {
  return <div>
    <b>{label}</b>
    <div className="num" style={{ fontSize: 22, marginTop: 6 }}>{metric.value === null ? "No data" : `${(metric.value * 100).toLocaleString(undefined, { maximumFractionDigits: 1 })}%`}</div>
    <div className="muted">{metric.numerator} / {metric.denominator} measured · {metric.unknown_count} unknown · {metric.excluded_count} excluded</div>
    <Definition definition={metric.definition} label={label} />
  </div>;
}

function verdict(score: Pick<ScorerResult, "status" | "classification" | "score" | "passed">) {
  return score.status === "succeeded" ? `${score.classification ?? "Unknown"} · score ${score.score ?? "Unknown"} · ${score.passed === null ? "Not judged" : score.passed ? "Pass" : "Fail"}` : score.status;
}

function RunDetails({ run, workspaceId, canJudge, onClose }: { run: Run; workspaceId: string; canJudge: boolean; onClose: () => void }) {
  const qc = useQueryClient();
  const [offset, setOffset] = useState(0);
  const root = `/api/workspaces/${workspaceId}`;
  const details = useQuery({
    queryKey: ["scores", workspaceId, run.id, offset],
    queryFn: () => api<ScorerResult[]>(`${root}/runs/${run.id}/scores?limit=20&offset=${offset}`),
    refetchInterval: 5_000,
  });
  const definitions = useQuery({ queryKey: ["scorers", workspaceId], queryFn: () => api<ScorerDefinition[]>(`${root}/scorers`) });
  const judge = useMutation({
    mutationFn: (scorerId: string) => api<ScorerResult>(`${root}/runs/${run.id}/scores`, { method: "POST", json: { scorer_id: scorerId } }),
    onSuccess: () => setOffset(0),
    onSettled: () => {
      // A failed request may still have reserved a durable result or incurred judge cost.
      qc.invalidateQueries({ queryKey: ["scores", workspaceId, run.id] });
      qc.invalidateQueries({ queryKey: ["runs", workspaceId] });
      qc.invalidateQueries({ queryKey: ["outcome", workspaceId, run.outcome_id] });
      qc.invalidateQueries({ queryKey: ["outcomes", workspaceId] });
      qc.invalidateQueries({ queryKey: ["stats", workspaceId] });
      qc.invalidateQueries({ queryKey: ["dashboard", workspaceId] });
    },
  });
  const applicable = definitions.data?.filter(d => d.roles.includes(run.role)) || [];
  return <Modal title="Run details" onClose={onClose}>
    <p><b>{run.role.replaceAll("_", " ")}</b> · {run.status} · {run.model}</p>
    <p className="mono" style={{ overflowWrap: "anywhere" }}>Run {run.id}<br />Config version: {run.config_version || "Unknown"}</p>
    <p>Execution: {money(run.cost_usd, run.cost_known)} · {duration(run.started_at, run.finished_at)}</p>
    {run.error && <p role="alert">{run.error}</p>}
    {hasOutcome(run) && <p><Link className="linkish" to={`/w/${workspaceId}/outcomes/${run.outcome_id}`}>Open outcome #{run.outcome_number}</Link></p>}
    <h3>Scorer results</h3>
    {run.score_summary && <p className="muted">Judging: {money(run.score_summary.cost_usd, run.score_summary.cost_known)}{!run.score_summary.cost_known && " · Judge cost pending reconciliation"}</p>}
    {details.isLoading && <div role="status">Loading scores…</div>}
    {details.isError && <p role="alert">Could not load scores: {details.error.message} <button className="btn sm" onClick={() => details.refetch()}>Retry</button></p>}
    {details.data?.length === 0 && <p className="muted">{offset ? "No more scores." : "No scorer results yet."}</p>}
    {details.data?.map(score => <section key={score.id} style={{ borderTop: "1px solid var(--line)", padding: "12px 0" }}>
      <b>{score.scorer_id}</b> · {verdict(score)}
      <div className="muted" style={{ fontSize: 12 }}>Definition version: <span className="mono">{score.definition_version}</span> · {money(score.cost_usd, score.cost_known)}</div>
      {score.status === "succeeded" && <div className="muted">Pass threshold: {score.definition.pass_threshold}</div>}
      <p style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>{score.reasoning || (score.status === "queued" || score.status === "running" ? "Judging pending…" : "No reasoning recorded.")}</p>
      {score.error && <p role="alert" style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>{score.error}</p>}
    </section>)}
    <div style={{ display: "flex", gap: 8 }}>
      <button className="btn sm" disabled={offset === 0 || details.isFetching} onClick={() => setOffset(Math.max(0, offset - 20))}>Previous scores</button>
      <button className="btn sm" disabled={!details.data || details.data.length < 20 || details.isFetching} onClick={() => setOffset(offset + 20)}>Next scores</button>
    </div>
    <h3 style={{ marginTop: 20 }}>Apply scorer</h3>
    {definitions.isLoading && <p role="status">Loading scorer definitions…</p>}
    {definitions.isError && <p role="alert">Could not load scorer definitions: {definitions.error.message} <button className="btn sm" onClick={() => definitions.refetch()}>Retry</button></p>}
    {definitions.data && !applicable.length && <p className="muted">No scorer applies to this run's role.</p>}
    {!canJudge && <p className="muted">Viewers can inspect results but cannot apply scorers.</p>}
    {run.status !== "succeeded" && <p className="muted">Scorers require a successful run.</p>}
    {applicable.map(d => <div key={d.id} style={{ margin: "12px 0" }}>
      <b>{d.id}</b> · Pass threshold: {d.pass_threshold}
      <details style={{ margin: "6px 0" }}><summary style={{ cursor: "pointer" }}>Scorer definition</summary>
        <p style={{ whiteSpace: "pre-wrap" }}>{d.instructions}</p>
        <p>{Object.entries(d.classifications).map(([name, score]) => `${name}: ${score}`).join(" · ")}</p>
        <p className="muted">Version: {d.version} · Automatic sample rate: {d.sample_rate * 100}%</p>
      </details>
      {canJudge && <button className="btn sm" disabled={run.status !== "succeeded" || judge.isPending} onClick={() => judge.mutate(d.id)}>Apply {d.id}</button>}
    </div>)}
    {judge.isPending && <p role="status">Judging… Results will appear here when complete.</p>}
    {judge.isError && <p role="alert">Could not apply scorer: {judge.error.message}. Run details are unchanged.</p>}
    {judge.isSuccess && <p role={judge.data.status === "error" ? "alert" : "status"}>{judge.data.status === "error" ? `Scoring failed: ${judge.data.error || "No verdict returned"}` : `Scorer result: ${verdict(judge.data)}`}</p>}
  </Modal>;
}
