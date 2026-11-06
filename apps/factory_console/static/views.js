import {
  escape as e,
  icon,
  badge,
  relative,
  fullDate,
  empty,
  priority,
  api,
  notify,
  scopedRuns,
  showDialog,
} from "./core.js";
import { itemDialog } from "./app.js";
const ranks = { urgent: 0, high: 1, medium: 2, low: 3 };
export function inbox(data, workspace) {
  const count = data.items.filter(
    (i) =>
      i.attention_at &&
      !i.resolved &&
      (!workspace || i.workspace_id === workspace.id),
  ).length;
  return `<div class="page-heading">
<div>
<span class="eyebrow">${workspace ? "WORKSPACE ATTENTION" : "MASTER VIEW"}</span>
<h1>${workspace ? "Attention" : "Inbox"}<span class="heading-count">${count}</span>
</h1>
<p>${workspace ? "Decisions and blockers for this workspace." : "Your next action, across every workspace."}</p>
</div>
<button class="button primary" data-new-work>${icon("plus")}New work</button>
</div>
<div class="inbox-toolbar">
<div class="segments" aria-label="Inbox order">
<button class="selected" data-order="priority">${icon("layers")}Priority</button>
<button data-order="oldest">${icon("clock")}Chronological</button>
</div>
<div class="toolbar-filters">${
    workspace
      ? ""
      : `<select id="workspace-filter" aria-label="Filter workspace">
<option value="">All workspaces</option>${data.workspaces.map((w) => `<option value="${e(w.id)}">${e(w.name)}</option>`).join("")}</select>`
  }<select id="chrono-order" aria-label="Chronological order" hidden>
<option value="oldest">Oldest first</option>
<option value="newest">Newest first</option>
</select>
<label class="search-box">${icon("search")}<input data-search placeholder="Search attention" aria-label="Search attention">
</label>
</div>
</div>
<div class="inbox-layout">
<section id="attention-list" aria-label="Items needing attention">
</section>
<aside class="inbox-context">
<span class="eyebrow">FACTORY AT A GLANCE</span>
<h2>${data.connection.connected ? (data.connection.controller.mode === "paused" ? "Ready for your next move." : "Your Factory, in view.") : "A clear view of your work."}</h2>
<p>${data.connection.connected ? (data.connection.controller.mode === "paused" ? "Intake is deliberately paused. Existing work and its evidence stay available." : "Inspect the last recorded stage and the evidence behind it.") : "The live connection is being prepared. Saved workspaces and requests remain available."}</p>
<div class="context-stats">
<a href="#/runs">
<strong>${data.connection.observed_at ? data.runs.filter((r) => !["complete", "blocked"].includes(r.status)).length : "—"}</strong>
<span>Open runs</span>${icon("arrow")}</a>
<a href="#/workspaces">
<strong>${data.workspaces.length}</strong>
<span>Workspaces</span>${icon("arrow")}</a>
</div>
<div class="context-note">${icon("info")}<p>Highest priority comes first. When priorities match, the oldest request for attention wins.</p>
</div>
</aside>
</div>`;
}
function attentionList(
  data,
  workspace,
  order = "priority",
  search = "",
  filter = "",
) {
  let list = data.items.filter(
    (i) =>
      i.attention_at &&
      !i.resolved &&
      (!workspace || i.workspace_id === workspace.id) &&
      (!filter || i.workspace_id === filter) &&
      `${i.title} ${i.description}`
        .toLowerCase()
        .includes(search.toLowerCase()),
  );
  list.sort((a, b) =>
    order === "priority"
      ? ranks[a.priority] - ranks[b.priority] ||
        a.attention_at.localeCompare(b.attention_at)
      : order === "newest"
        ? b.attention_at.localeCompare(a.attention_at)
        : a.attention_at.localeCompare(b.attention_at),
  );
  if (!list.length)
    return empty(
      "checkcircle",
      search || filter ? "No matching items" : "You're clear for now.",
      search || filter
        ? "Try a different workspace or search."
        : "Decisions, approvals and blockers will appear here when they need you.",
      `<a class="button secondary" href="#/runs">View Factory runs ${icon("arrow")}</a>`,
    );
  return `<div class="list-caption">
<span>${list.length} ${list.length === 1 ? "item needs" : "items need"} your attention</span>
<span>${order === "priority" ? "Priority · oldest first" : order === "oldest" ? "Oldest first" : "Newest first"}</span>
</div>${list
    .map((i) => {
      const w = data.workspaces.find((w) => w.id === i.workspace_id);
      return `<button class="attention-row" data-item="${e(i.id)}">
<div class="row-leading">${priority(i.priority)}</div>
<div class="row-content">
<div class="row-meta">
<span>${e(w?.name || "Workspace")}</span>
<span>·</span>
<span>${i.kind === "failure" ? "Run needs review" : i.mode === "non-coding" ? "Non-coding task" : "Engineering task"}</span>
</div>
<h3>${e(i.title)}</h3>
<p>${e(i.description || "Open the item to review the next action.")}</p>
<div class="row-bottom">${badge(i.status)}<span title="${e(fullDate(i.attention_at))}">${icon("clock")}${relative(i.attention_at)}</span>
</div>
</div>${icon("chevron", "row-chevron")}</button>`;
    })
    .join("")}`;
}
export function workspaces(data) {
  return `<div class="page-heading">
<div>
<span class="eyebrow">MINDSHUB TEAM</span>
<h1>Workspaces</h1>
<p>Products, repositories and the work that moves them forward.</p>
</div>
<button class="button primary" data-new-workspace>${icon("plus")}New workspace</button>
</div>
<div class="workspace-list">${data.workspaces
    .map((w) => {
      const runs = scopedRuns(data, w);
      return `<a class="workspace-card" href="#/workspace/${e(w.id)}/chat">
<div class="workspace-card-top">
<span class="workspace-monogram large">${e(w.name.slice(0, 2).toUpperCase())}</span>${icon("arrow")}</div>
<h2>${e(w.name)}</h2>
<p>${e(w.description || "A shared home for this product’s work.")}</p>
<div class="repo-list">${w.repositories.map((r) => `<span>${icon("git")}${e(r)}</span>`).join("")}</div>
<footer>
<span>${w.mode === "non-coding" ? "Non-coding" : "Engineering"}</span>
<span>${runs.length} ${runs.length === 1 ? "run" : "runs"}</span>
</footer>
</a>`;
    })
    .join("")}</div>`;
}
const filters = new Map();
export function bindViews(data, workspace, refresh) {
  const key = workspace?.id || "all";
  const state = filters.get(key) || {
    order: "priority",
    query: "",
    workspace: "",
  };
  filters.set(key, state);
  const node = document.querySelector("#attention-list");
  if (!node) return;
  const search = document.querySelector("[data-search]");
  const workspaceFilter = document.querySelector("#workspace-filter");
  const chrono = document.querySelector("#chrono-order");
  search.value = state.query;
  if (workspaceFilter) workspaceFilter.value = state.workspace;
  chrono.value = state.order === "newest" ? "newest" : "oldest";
  const redraw = () => {
    chrono.hidden = state.order === "priority";
    document
      .querySelectorAll("[data-order]")
      .forEach((button) =>
        button.classList.toggle(
          "selected",
          button.dataset.order === "priority"
            ? state.order === "priority"
            : state.order !== "priority",
        ),
      );
    node.innerHTML = attentionList(
      data,
      workspace,
      state.order,
      state.query,
      state.workspace,
    );
    node.querySelectorAll("[data-item]").forEach(
      (button) =>
        (button.onclick = () => {
          const item = data.items.find(
            (item) => item.id === button.dataset.item,
          );
          const ws = data.workspaces.find((ws) => ws.id === item.workspace_id);
          showDialog(`<div class="dialog-heading"><h2>Needs attention</h2><button class="icon-button" data-close aria-label="Close">${icon("close")}</button></div>
        <div class="attention-detail-meta">${priority(item.priority)}<span>${e(ws.name)}</span>${badge(item.status)}</div>
        <h3 class="attention-title">${e(item.title)}</h3><p class="attention-description">${e(item.description)}</p>
        <p class="field-note">First needed attention ${e(fullDate(item.attention_at))}. Editing this item preserves its original age.</p>
        <div class="dialog-footer"><button class="button secondary" id="edit-attention">Edit details</button>
        ${item.run_id ? `<a class="button secondary" href="#/run/${e(item.run_id)}">Inspect run ${icon("arrow")}</a>` : ""}
        <button class="button primary" id="handle-attention">Mark handled</button></div>`);
          document.querySelector("#edit-attention").onclick = () => {
            document.querySelector("#dialog").close();
            itemDialog(ws, item);
          };
          document.querySelector("#handle-attention").onclick = async (
            event,
          ) => {
            event.target.disabled = true;
            try {
              await api("/api/items/" + item.id, { ...item, resolved: true });
              document.querySelector("#dialog").close();
              await refresh();
              notify(
                "Marked handled. The original work and run outcome are retained.",
              );
            } catch (error) {
              notify(error.message);
              event.target.disabled = false;
            }
          };
        }),
    );
  };
  document.querySelectorAll("[data-order]").forEach(
    (button) =>
      (button.onclick = () => {
        state.order =
          button.dataset.order === "priority" ? "priority" : chrono.value;
        redraw();
      }),
  );
  chrono.onchange = () => {
    state.order = chrono.value;
    redraw();
  };
  if (workspaceFilter)
    workspaceFilter.onchange = () => {
      state.workspace = workspaceFilter.value;
      redraw();
    };
  search.oninput = () => {
    state.query = search.value;
    redraw();
  };
  redraw();
}
