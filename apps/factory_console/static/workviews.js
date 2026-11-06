import {
  escape as e,
  icon,
  badge,
  relative,
  fullDate,
  empty,
  priority,
  scopedRuns,
  api,
  notify,
  showDialog,
} from "./core.js";
import { workspaceDialog, itemDialog } from "./app.js";
export function board(data, workspace) {
  const runs = scopedRuns(data, workspace),
    local = data.items.filter(
      (i) => i.workspace_id === workspace.id && !i.run_id,
    );
  const cards = [
    ...local,
    ...runs.map((r) => ({
      ...r,
      run_id: r.id,
      priority: "medium",
      mode: "engineering",
      description: r.summary,
    })),
  ];
  const column = (s) =>
    ["planning", "building", "verifying", "ci", "smoking", "unknown"].includes(
      s,
    )
      ? "progress"
      : s === "complete"
        ? "complete"
        : s;
  return `<div class="section-heading">
<div>
<h2>Work board</h2>
<p>A shared view of priorities, progress and blockers.</p>
</div>
<button class="button primary" data-new-work>${icon("plus")}New work</button>
</div>
<div class="board" aria-label="Work board">${[
    ["backlog", "Backlog"],
    ["ready", "Ready"],
    ["progress", "In progress"],
    ["blocked", "Blocked"],
    ["review", "Review"],
    ["complete", "Delivered"],
  ]
    .map(([key, label]) => {
      const list = cards.filter((c) => column(c.status) === key);
      return `<section class="board-column">
<header>
<span class="column-dot ${key}">
</span>
<h3>${label}</h3>
<span class="column-count">${list.length}</span>
</header>
<div class="board-cards">${
        list.length
          ? list
              .sort((a, b) => b.updated_at.localeCompare(a.updated_at))
              .map(
                (
                  c,
                ) => `<button class="work-card ${c.status === "blocked" ? "blocked" : ""}" ${c.run_id ? `data-open-run="${e(c.run_id)}"` : `data-edit-item="${e(c.id)}"`}>
<div class="work-card-meta">
<span>${c.run_id ? "Factory run" : c.mode === "non-coding" ? "Non-coding" : "Engineering"}</span>${priority(c.priority)}</div>
<h3>${e(c.title)}</h3>${c.description ? `<p>${e(c.description)}</p>` : ""}${badge(c.status)}<footer>
<span title="${e(c.run_id ? c.id : workspace.name)}">${c.run_id ? e(c.id.length > 24 ? "…" + c.id.slice(-24) : c.id) : e(workspace.name)}</span>
<span>${relative(c.updated_at)}</span>
</footer>
</button>`,
              )
              .join("")
          : `<div class="column-empty">${key === "backlog" ? "Capture the next piece of work." : key === "progress" ? "Verified run progress appears here." : "Nothing here yet."}</div>`
      }</div>
</section>`;
    })
    .join("")}</div>
<div class="board-footnote">${icon("info")}<span>Run stages come from Factory evidence. New work is saved to the backlog; changing a card never starts a run.</span>
</div>`;
}
export function runs(data, workspace) {
  const list = scopedRuns(data, workspace).sort((a, b) =>
    b.created_at.localeCompare(a.created_at),
  );
  return `<div class="${workspace ? "section-heading" : "page-heading"}">
<div>${workspace ? "" : '<span class="eyebrow">MASTER VIEW</span>'}<h${workspace ? "2" : "1"}>${workspace ? "Workspace runs" : "Factory runs"}<span class="heading-count">${list.length}</span>
</h${workspace ? "2" : "1"}>
<p>Actual deliveries and the evidence behind them.</p>
</div>
<span class="quiet-label">${data.connection.observed_at ? `Observed ${relative(data.connection.observed_at)}` : "Awaiting first observation"}</span>
</div>${
    list.length
      ? `<div class="runs-table">
<div class="runs-table-heading">
<span>Run</span>
<span>Stage</span>
<span>Usage</span>
<span>Updated</span>
</div>${list
          .map(
            (r) => `<a class="run-row" href="#/run/${e(r.id)}">
<div>
<h3>${e(r.title || r.id)}</h3>
<span class="mono">${e(r.repository)}</span>
<small>${e(r.id)}</small>
</div>
<div>${badge(r.status)}${r.status === "blocked" && r.stage !== "unknown" ? `<small>During ${e(r.stage)}</small>` : ""}</div>
<div>
<span>${r.tokens === null ? "Unknown" : r.tokens.toLocaleString() + " tokens"}</span>
<small>${r.cost_usd === null ? "Cost unknown" : "$" + r.cost_usd.toFixed(3)}${r.usage_complete ? "" : " · usage incomplete"}</small>
</div>
<span class="quiet-label" title="${e(fullDate(r.updated_at))}">${relative(r.updated_at)} ${icon("chevron")}</span>
</a>`,
          )
          .join("")}</div>`
      : empty(
          "runs",
          "No runs received yet.",
          "Once the Factory feed is connected, runs and their verification evidence will appear here.",
        )
  }<p class="page-note">A completed role or successful check is one part of a delivery. Delivered means the Factory recorded the complete outcome.</p>`;
}
export function runDetail(data, id) {
  const r = data.runs.find((r) => r.id === id);
  if (!r)
    return empty(
      "search",
      "Run not found.",
      "It may not be in the current feed. The Factory retains its authoritative history.",
      `<a class="button secondary" href="#/runs">All runs</a>`,
    );
  const stages = [
    "planning",
    "building",
    "verifying",
    "ci",
    "smoking",
    "complete",
  ];
  const current = stages.indexOf(r.stage),
    complete = r.status === "complete";
  return `<div class="run-heading">
<a class="back-link" href="#/runs">${icon("arrow")}All runs</a>
<div class="page-heading">
<div>
<span class="eyebrow">FACTORY RUN</span>
<h1>${e(r.title || r.id)}</h1>
<p class="mono">${e(r.repository)}<span>·</span>${e(r.id)}</p>
</div>${badge(r.status)}</div>
</div>
<ol class="pipeline" aria-label="Delivery stages">${stages
    .map(
      (
        s,
        i,
      ) => `<li class="${complete ? "done" : i === current ? (r.status === "blocked" ? "stopped" : "active") : ""}">
<span>${complete ? icon("check") : i + 1}</span>
<div>${s === "complete" ? "Delivery" : s === "ci" ? "CI / merge" : s === "smoking" ? "Smoke checks" : s[0].toUpperCase() + s.slice(1)}</div>
</li>`,
    )
    .join("")}</ol>
<div class="run-detail-grid">
<div>
<section class="detail-section">
<div class="section-title">${icon("file")}<h2>Outcome</h2>
</div>
<p class="outcome-copy">${e(r.summary || "No final summary has been recorded. Inspect the stage timeline and verification results below.")}</p>${
    r.status === "blocked"
      ? `<div class="notice warning">${icon("alert")}<div>
<strong>This run is blocked.</strong>
<p>Its original outcome is retained. Review the evidence before deciding the next piece of work.</p>
</div>
</div>`
      : ""
  }${r.pull_request_url ? `<a class="button secondary" href="${e(r.pull_request_url)}" target="_blank" rel="noopener noreferrer">${icon("git")}Open pull request ${icon("external")}</a>` : ""}</section>
<section class="detail-section">
<div class="section-title">${icon("checkcircle")}<h2>Verification</h2>
<span class="quiet-label">${r.checks.length} recorded checks</span>
</div>${
    r.checks.length
      ? `<div class="checks">${r.checks
          .map(
            (c) => `<div>
<span>${e(c.name)}</span>${badge(c.status)}<small>${c.at ? relative(c.at) : ""}</small>
</div>`,
          )
          .join("")}</div>`
      : `<p class="quiet-copy">No executable check results have been recorded for this run.</p>`
  }</section>
<section class="detail-section">
<div class="section-title">${icon("clock")}<h2>Activity</h2>
</div>${
    r.timeline.length
      ? `<ol class="timeline">${r.timeline
          .map(
            (t) => `<li>
<span class="timeline-mark ${e(t.status)}">
</span>
<div>
<h3>${e(t.label || t.kind.replaceAll("_", " "))}</h3>
<span>${e(t.kind)}</span>
</div>
<time title="${e(fullDate(t.at))}">${relative(t.at)}</time>
</li>`,
          )
          .join("")}</ol>`
      : '<p class="quiet-copy">No stage events have been projected yet.</p>'
  }</section>
</div>
<aside class="run-inspector">
<h3>Run details</h3>
<dl>
<dt>Status</dt>
<dd>${badge(r.status)}</dd>
<dt>Repository</dt>
<dd class="mono">${e(r.repository)}</dd>
<dt>Started</dt>
<dd>${fullDate(r.created_at)}</dd>
<dt>Finished</dt>
<dd>${r.finished_at ? fullDate(r.finished_at) : "Not recorded"}</dd>
<dt>Candidate</dt>
<dd class="mono">${r.candidate_sha ? e(r.candidate_sha.slice(0, 12)) : "Not created"}</dd>
<dt>Recorded usage</dt>
<dd>${r.tokens === null ? "Unknown" : r.tokens.toLocaleString() + " tokens"}</dd>
<dt>Usage completeness</dt>
<dd>${r.usage_complete ? "Complete" : "Incomplete or unknown"}</dd>
<dt>Inference cost</dt>
<dd>${r.cost_usd === null ? "Unknown" : "$" + r.cost_usd.toFixed(3)}</dd>
</dl>
<p>Live observations are shown separately from the delivery outcome. No journal activity is treated as a liveness guarantee.</p>
</aside>
</div>`;
}
export function settings(data, workspace) {
  return `<div class="section-heading">
<div>
<h2>Workspace settings</h2>
<p>One shared setup for this product's work.</p>
</div>
<button class="button secondary" data-edit-workspace>${icon("settings")}Edit workspace</button>
</div>
<div class="settings-content">
<section>
<h3>General</h3>
<dl class="settings-list">
<dt>Workspace</dt>
<dd>${e(workspace.name)}</dd>
<dt>Default mode</dt>
<dd>${workspace.mode === "engineering" ? "Engineering" : "Non-coding"}</dd>
<dt>Description</dt>
<dd>${e(workspace.description || "No description added")}</dd>
</dl>
</section>
<section>
<h3>Repositories <span class="heading-count">${workspace.repositories.length}</span>
</h3>
<p>Context and work can span all repositories in this workspace.</p>
<div class="settings-repos">${workspace.repositories
    .map(
      (r) => `<div>${icon("git")}<span>${e(r)}</span>
<a href="https://github.com/${e(r)}" target="_blank" rel="noopener noreferrer" aria-label="Open ${e(r)}">${icon("external")}</a>
</div>`,
    )
    .join("")}</div>
</section>
<section>
<h3>Execution and verification</h3>
<p>Environments, executable skills and delivery policy are managed by the Factory's trusted configuration. This preview shows their outcomes without changing active runs.</p>
<div class="settings-policy">${icon("checkcircle")}<span>Planning before edits</span>${icon("checkcircle")}<span>Independent verification</span>${icon("checkcircle")}<span>CI and delivery gates</span>
</div>
</section>
</div>`;
}
export function bindWorkViews(data, workspace) {
  document
    .querySelectorAll("[data-open-run]")
    .forEach(
      (b) => (b.onclick = () => (location.hash = `#/run/${b.dataset.openRun}`)),
    );
  document.querySelectorAll("[data-edit-item]").forEach(
    (b) =>
      (b.onclick = () =>
        itemDialog(
          workspace,
          data.items.find((i) => i.id === b.dataset.editItem),
        )),
  );
  document
    .querySelector("[data-edit-workspace]")
    ?.addEventListener("click", () => workspaceDialog(workspace));
}
