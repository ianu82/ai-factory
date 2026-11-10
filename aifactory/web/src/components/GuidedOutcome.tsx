import { useState } from "react";
import type { OutcomeDetail, Task } from "../api";
import { useWs } from "../App";
import { Mark } from "../ui";

export type EvidenceTarget = { taskId: string; section: string; nonce: number };

// Attempt numbers restart when a fresh retry budget is granted. Never search backwards
// for a passing verdict: absence in the current budget/attempt means pending.
export function currentAttempt(task: Task) {
  return task.evidence.attempts?.findLast((a) => a.n === task.attempt && (a.epoch || 0) === (task.evidence.budget_epoch || 0));
}

// A handled rejection belongs to an earlier candidate. A completed rebase uses
// its new checks and verifier, unless policy explicitly retained the original review.
export function currentCandidateEvidence(task: Task) {
  const attempt = currentAttempt(task);
  const revalidation = task.evidence.revalidation?.phase !== "handled" ? task.evidence.revalidation : undefined;
  return {
    revalidation,
    checks: revalidation ? revalidation.checks : attempt?.checks,
    verdict: revalidation ? revalidation.run === false ? attempt?.verdict : revalidation.verifier : attempt?.verdict,
  };
}

type Fact = { state: string; text: string; section?: string; href?: string };
const stopped = new Set(["blocked", "failed", "cancelled", "abandoned", "reverted", "handed_over", "needs_input"]);

export function taskFacts(task: Task): Record<string, Fact> {
  const a = currentAttempt(task);
  const { revalidation, checks, verdict } = currentCandidateEvidence(task);
  const invalidating = stopped.has(task.status) || ["prepare", "build", "checks", "revise", "ci_repair", "revalidate", "revalidate_prepare", "revalidate_setup", "revalidate_checks", "revalidate_verify", "reverify"].includes(task.stage);
  const fresh = !invalidating && (!revalidation || (["passed", "complete"].includes(revalidation.phase) && !!revalidation.head && revalidation.head === task.head_sha));
  const ci = task.evidence.ci;
  const smoke = task.evidence.smoke;
  const pending = (text: string, section?: string): Fact => ({ state: "Pending / unavailable", text, section });
  return {
    building: { state: stopped.has(task.status) ? task.status : a ? a.result : task.status === "running" ? "In progress" : "Pending", text: `Attempt ${task.attempt}: ${task.blocked_reason || "The builder prepares a candidate change. Inspect the current sessions and diff; earlier attempts are history, not proof for this candidate."}`, section: "build" },
    checks: checks?.length ? { state: checks.some((c) => !c.ok) ? "Failed" : fresh ? "Passed" : "Pending revalidation", text: `${checks.filter((c) => c.ok).length}/${checks.length} recorded commands succeeded. Read the command output before relying on it.`, section: "checks" } : pending("No current command results available; checks may not be configured. This is not a pass.", "checks"),
    verification: verdict ? { state: verdict.verdict === "fail" || verdict.criteria.some((c) => c.status !== "pass") ? "Failed / unverified" : fresh ? "Passed" : "Pending revalidation", text: verdict.summary || "A separate verifier reviewed the candidate against its acceptance criteria.", section: "verification" } : pending("No independent verdict for the current attempt. A builder finishing is not independent verification.", "verification"),
    pr: task.pr_url ? { state: fresh ? "Published" : "Inspect candidate", text: "The pull request is available. Its existence does not prove that the current candidate passed its gates.", href: task.pr_url } : pending("No pull request has been recorded yet."),
    ci: !ci || ci.state === "none" ? { state: "Not configured / unavailable", text: "No CI checks reported. CI has not passed. Inspect repository configuration or wait for GitHub to report checks.", section: "ci" } : { state: !fresh || task.status === "waiting_ci" || !ci.head || ci.head !== task.head_sha ? "Pending current head" : ci.state === "success" && !!ci.checks?.length && ci.checks.every((c) => c.conclusion === "success") ? "Passed" : ci.state, text: "GitHub reports checks for a commit. Compare the recorded head with the current candidate and inspect each check.", section: "ci" },
    merge: task.merge_sha ? { state: ["reverted", "cancelled"].includes(task.status) ? task.status : "Merge recorded", text: `Recorded merge commit: ${task.merge_sha}. A merge alone does not prove smoke or delivery succeeded.`, section: "merge" } : pending("No merge commit recorded. The configured merge policy and any required approval still apply.", "merge"),
    smoke: !smoke || smoke.state === "not_configured" ? { state: "Not configured / unavailable", text: "No smoke results available. Smoke is not configured or has not reported yet; this is not a pass.", section: "smoke" } : { state: smoke.state === "passed" ? task.status === "completed" && !!task.merge_sha && smoke.ref === task.merge_sha && !!smoke.checks?.length && smoke.checks.every((c) => c.ok) ? "Passed" : "Pending current merge" : smoke.state, text: "Smoke checks exercise the merged result, not the earlier candidate. Inspect the commands and merge reference.", section: "smoke" },
  };
}

const stages = [
  ["building", "Building"], ["checks", "Checks"], ["verification", "Independent verification"],
  ["pr", "Pull request"], ["ci", "CI"], ["merge", "Merge / approval"], ["smoke", "Smoke"],
];

export function GuidedOutcome({ detail, onOpen }: { detail: OutcomeDetail; onOpen: (target: EvidenceTarget) => void }) {
  const { ws, me } = useWs();
  const key = `factory:outcome-guide:${ws.id}:${detail.outcome.id}:${me.user.id}`;
  const [expanded, setExpanded] = useState(() => {
    try { const saved = localStorage.getItem(key); return saved === "open" || (saved !== "dismissed" && !!detail.guidance); }
    catch { return !!detail.guidance; }
  });
  const toggle = () => {
    const next = !expanded;
    setExpanded(next);
    try { localStorage.setItem(key, next ? "open" : "dismissed"); } catch { /* Storage may be disabled. */ }
  };
  const plan = detail.plans.filter((p) => p.status !== "superseded").sort((a, b) => b.version - a.version)[0];
  const planner = detail.runs.filter((r) => r.role === "planner").sort((a, b) => b.started_at.localeCompare(a.started_at))[0];
  const planDecisions = detail.decisions.filter((d) => d.status === "open" && d.kind === "approve_plan");
  const scroll = (id: string) => {
    const el = document.getElementById(id);
    el?.scrollIntoView({ block: "center" });
    el?.focus({ preventScroll: true });
  };
  return <section className="p-sec guided-outcome" aria-label="Guided outcome">
    <button className="btn sm" aria-expanded={expanded} aria-controls="outcome-guide-stages" onClick={toggle}>{expanded ? "Dismiss guide" : "Reopen guide"}</button>
    {expanded && <div id="outcome-guide-stages">
      <h3>Follow this outcome</h3>
      <p>Live evidence, not a timer. Each task must clear its own gates. Dismissing this guide never dismisses decisions.</p>
      {detail.guidance && <p>README example: check that only the root README changes, CONTRIBUTING uses repository facts, and unrelated content is preserved.</p>}
      <article className="guided-stage" aria-label="Planning">
        <h4><Mark status="none" /> Planning · {detail.outcome.status === "planning" ? "In progress" : plan ? "Plan recorded" : planner?.status || "Pending"}</h4>
        <p>{detail.outcome.status === "planning" ? "The planner is reading repository facts and drafting tasks. Any older plan is history until this planning run finishes." : plan ? "The planner proposed the scope and tasks. Read the plan and risks." : "No plan evidence available yet."}</p>
        {plan && <a href="#guided-plan" onClick={(e) => { e.preventDefault(); scroll("guided-plan"); }}>Inspect plan v{plan.version}</a>}
        {planner && <a href="#guided-planning-runs" onClick={(e) => { e.preventDefault(); scroll("guided-planning-runs"); }}>Inspect planner session</a>}
        <p className="muted">{detail.outcome.status === "blocked" ? `Action needed: ${detail.outcome.blocked_reason}` : "No action unless a decision below asks you."}</p>
      </article>
      <article className="guided-stage" aria-label="Plan review">
        <h4><Mark status="none" /> Plan review · {detail.outcome.status === "planning" ? "Pending current plan" : plan?.status || "Pending"}</h4>
        <p>Review scope before work starts. Approval is recorded by the existing decision controls or workspace policy.</p>
        {planDecisions.map((d) => <a key={d.id} href={`#decision-${d.id}`} onClick={() => scroll(`decision-${d.id}`)}>{d.title}</a>)}
        {plan && <a href="#guided-plan" onClick={(e) => { e.preventDefault(); scroll("guided-plan"); }}>Inspect plan status and approval</a>}
        <p className="muted">{planDecisions.length ? ws.role === "viewer" ? "Action needed: ask a workspace member to review the decision below." : "Action needed: review the decision below; approve only if the scope is right." : "No action now. Missing approval evidence is pending, not approval."}</p>
      </article>
      {stages.map(([id, title]) => <article className="guided-stage" key={id} aria-label={title}>
        <h4>{title}</h4>
        {!detail.tasks.length && <p>Pending / unavailable — no tasks yet. No action now.</p>}
        {detail.tasks.map((task) => {
          const fact = taskFacts(task)[id];
          const decisions = detail.decisions.filter((d) => d.status === "open" && d.task_id === task.id && (id === "merge" ? d.kind === "approve_merge" : d.kind !== "approve_merge"));
          const event = detail.events.filter((e) => e.task_id === task.id && e.data?.attempt === task.attempt && e.data?.head === task.head_sha && !["agent_thought", "agent_tool"].includes(e.kind)).at(-1);
          return <div className="guided-task" key={task.id}>
            <strong><Mark status={fact.state === "Passed" ? "pass" : /fail|blocked|cancelled|reverted/i.test(fact.state) ? "fail" : "none"} /> {task.key} · {fact.state}</strong>
            <p>{fact.text}</p>
            {fact.href && <a href={fact.href} target="_blank" rel="noreferrer">Inspect {task.key} pull request</a>}
            {fact.section && <a href={`#task-${task.id}-${fact.section}`} onClick={(e) => { e.preventDefault(); onOpen({ taskId: task.id, section: fact.section!, nonce: Date.now() }); }}>Inspect {task.key} {title.toLowerCase()} evidence</a>}
            {event && stopped.has(task.status) && <a href={`#event-${event.id}`} onClick={(e) => { e.preventDefault(); scroll(`event-${event.id}`); }}>Inspect latest recorded activity: {event.message}</a>}
            {decisions.map((d) => <a href={`#decision-${d.id}`} key={d.id}>{d.title}</a>)}
            <p className="muted">{decisions.length ? ws.role === "viewer" ? "Action needed: ask a workspace member to review the decision below." : "Action needed: review the decision below. The guide cannot approve it." : stopped.has(task.status) ? `Work is ${task.status}. Inspect the reason and any open decisions; no automatic success is implied.` : "No action now; inspect evidence or wait for the next update."}</p>
          </div>;
        })}
      </article>)}
    </div>}
  </section>;
}
