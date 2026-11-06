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
import { evidence } from "./evidence.js";
const boardFilters = new Map();
const columns = [
  ["backlog", "Backlog"], ["ready", "Ready"], ["building", "Building"],
  ["verifying", "Verifying"], ["ci", "CI / Merge"], ["blocked", "Blocked"],
  ["complete", "Merged"], ["canceled", "Canceled"], ["duplicate", "Duplicate"],
];
const boardStage = (status) => ({planning: "building", smoking: "ci", review: "verifying"})[status] || status;
const reference = (id) => id.length > 24 ? id.slice(0, 10) + "…" + id.slice(-10) : id;
export function board(data, workspace) {
  return `${workspace ? "" : `<div class="page-heading"><h1>Board</h1><button class="button primary" data-new-work>${icon("plus")}New work</button></div>`}
  <div class="board-toolbar">${!workspace && data.workspaces.length > 1 ? `<select id="board-workspace" aria-label="Filter workspace"><option value="">All workspaces</option>${data.workspaces.map(w => `<option value="${e(w.id)}">${e(w.name)}</option>`).join("")}</select>` : ""}
  <label class="search-box">${icon("search")}<input data-search placeholder="Search board" aria-label="Search board"></label>
  <label class="board-empty-toggle"><input type="checkbox" id="board-empty">Hide empty stages</label></div>
  <div class="board" id="work-board" aria-label="Work board"></div>`;
}
function boardCards(data, workspace, state) {
  const scope = workspace || data.workspaces.find(w => w.id === state.workspace);
  const cards = [
    ...data.items.filter(i => !i.run_id && (!scope || i.workspace_id === scope.id)),
    ...scopedRuns(data, scope).map(r => ({...r, run_id: r.id, priority: null})),
  ].filter(c => `${c.title} ${c.id} ${c.repository || ""}`.toLowerCase().includes(state.query.toLowerCase()));
  if (!cards.length && state.query) return empty("search", "No matches", "");
  const groups = [...columns, ...(cards.some(c => !columns.some(([key]) => boardStage(c.status) === key)) ? [["unknown", "Unverified"]] : [])];
  return groups.map(([key, label]) => {
    const list = cards.filter(c => (columns.some(([k]) => k === boardStage(c.status)) ? boardStage(c.status) : "unknown") === key);
    if (state.hideEmpty && !list.length) return "";
    return `<section class="board-column"><header><span class="column-dot ${key}"></span><h3>${label}</h3><span class="column-count">${list.length}</span>${key === "backlog" ? `<button class="icon-button" data-new-board-item aria-label="Add to backlog">${icon("plus")}</button>` : ""}</header>
    <div class="board-cards">${list.sort((a,b) => b.updated_at.localeCompare(a.updated_at)).map(c => {
      const ws = data.workspaces.find(w => w.id === c.workspace_id || w.repositories.includes(c.repository));
      return `<a class="work-card" href="#/${c.run_id ? "run" : "item"}/${e(c.id)}" title="${e(c.title)}">
      <div class="work-card-meta"><span class="mono" title="${e(c.id)}">${e(reference(c.id))}</span>${c.priority ? priority(c.priority) : ""}</div>
      <h3>${e(c.title)}</h3><footer><span title="${e(c.repository || ws?.name)}">${e(c.repository?.split("/")[1] || ws?.name || (c.mode === "non-coding" ? "Non-coding" : "Engineering"))}</span>
      <span>${c.checks?.length ? `<span class="card-evidence" title="Recorded checks">${icon("checkcircle")} ${c.checks.filter(x => x.status === "passed").length}/${c.checks.length}</span>` : ""}<time>${relative(c.updated_at)}</time></span></footer></a>`;
    }).join("")}</div></section>`;
  }).join("") || empty("board", state.query ? "No matches" : "No work yet", "");
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
${evidence(data,"run",r)}
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
  const boardNode = document.querySelector("#work-board");
  if (boardNode) {
    const key = workspace?.id || "all";
    const state = boardFilters.get(key) || {workspace: "", query: "", hideEmpty: true};
    boardFilters.set(key, state);
    const search = document.querySelector("[data-search]");
    const filter = document.querySelector("#board-workspace");
    const hideEmpty = document.querySelector("#board-empty");
    search.value = state.query;
    hideEmpty.checked = state.hideEmpty;
    if (filter) filter.value = state.workspace;
    const redraw = () => {
      boardNode.innerHTML = boardCards(data, workspace, state);
      boardNode.querySelector("[data-new-board-item]")?.addEventListener("click", () => itemDialog(workspace || data.workspaces.find(w => w.id === state.workspace)));
    };
    search.oninput = () => {state.query = search.value; redraw();};
    hideEmpty.onchange = () => {state.hideEmpty = hideEmpty.checked; redraw();};
    if (filter) filter.onchange = () => {state.workspace = filter.value; redraw();};
    redraw();
  }
  document
    .querySelectorAll("[data-open-run]")
    .forEach(
      (b) => (b.onclick = () => (location.hash = `#/run/${b.dataset.openRun}`)),
    );
  document.querySelectorAll("[data-edit-item]").forEach(
    (b) =>
      (b.onclick = (event) => {
        event.preventDefault();
        itemDialog(
          workspace,
          data.items.find((i) => i.id === b.dataset.editItem),
        ); }),
  );
  document
    .querySelector("[data-edit-workspace]")
    ?.addEventListener("click", () => workspaceDialog(workspace));
}
