import {
  escape as e,
  icon,
  badge,
  relative,
  fullDate,
  empty,
  priority,
  scopedRuns,
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
      priority: null,
      mode: "engineering",
      description: "",
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
  return `<div class="board" aria-label="Work board">${[
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
<span>${c.run_id ? "Run" : "Draft"}</span>${c.priority ? priority(c.priority) : ""}</div>
<h3>${e(c.title)}</h3>${c.description && c.description.trim() !== c.title.trim() ? `<p>${e(c.description)}</p>` : ""}${column(c.status) === "progress" ? badge(c.status) : ""}<footer>
<span>${c.run_id ? e(c.repository.split("/")[1]) : c.mode === "non-coding" ? "Non-coding" : "Engineering"}</span>
<span>${relative(c.updated_at)}</span>
</footer>
</button>`,
              )
              .join("")
          : ""
      }</div>
</section>`;
    })
    .join("")}</div>`;
}
export function runs(data, workspace) {
  const list = scopedRuns(data, workspace).sort((a, b) =>
    b.created_at.localeCompare(a.created_at),
  );
  return `${
    !workspace || data.connection.stale
      ? `<div class="${workspace ? "section-heading" : "page-heading"}">
<div>${workspace ? "" : `<h1>Runs<span class="heading-count">${list.length}</span></h1>`}
</div>
${data.connection.stale ? `<span class="quiet-label">Last updated ${relative(data.connection.observed_at)}</span>` : ""}
</div>`
      : ""
  }${
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
<span>${r.tokens === null ? "—" : r.tokens.toLocaleString() + " tokens"}</span>
${r.cost_usd !== null ? `<small>$${r.cost_usd.toFixed(3)}</small>` : ""}${!r.usage_complete ? "<small>Usage incomplete</small>" : ""}
</div>
<span class="quiet-label" title="${e(fullDate(r.updated_at))}">${relative(r.updated_at)} ${icon("chevron")}</span>
</a>`,
          )
          .join("")}</div>`
      : empty(
          "runs",
          "No runs yet",
          data.connection.connected ? "" : "Factory connection pending.",
        )
  }`;
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
${r.pull_request_url ? `<section class="detail-section"><a class="button secondary" href="${e(r.pull_request_url)}" target="_blank" rel="noopener noreferrer">${icon("git")}Pull request ${icon("external")}</a></section>` : ""}
<section class="detail-section">
<div class="section-title">${icon("checkcircle")}<h2>Verification</h2>
${r.checks.length ? `<span class="quiet-label">${r.checks.length} checks</span>` : ""}
</div>${
    r.checks.length
      ? `<div class="checks">${r.checks
          .map(
            (c) => `<div>
<span>${e(c.name)}</span>${badge(c.status)}<small>${c.at ? relative(c.at) : ""}</small>
</div>`,
          )
          .join("")}</div>`
      : `<p class="quiet-copy">No checks recorded</p>`
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
</aside>
</div>`;
}
export function settings(data, workspace) {
  return `<div class="section-heading">
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
<div class="settings-repos">${workspace.repositories
    .map(
      (r) => `<div>${icon("git")}<span>${e(r)}</span>
<a href="https://github.com/${e(r)}" target="_blank" rel="noopener noreferrer" aria-label="Open ${e(r)}">${icon("external")}</a>
</div>`,
    )
    .join("")}</div>
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
