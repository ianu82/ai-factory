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
  return `${
    workspace
      ? ""
      : `<div class="page-heading">
<div>
<h1>Inbox<span class="heading-count">${count}</span>
</h1>
</div>
<button class="button primary" data-new-work>${icon("plus")}New work</button>
</div>`
  }
<div class="inbox-toolbar">
<div class="segments" aria-label="Inbox order">
<button class="selected" data-order="priority">${icon("layers")}Priority</button>
<button data-order="oldest">${icon("clock")}Chronological</button>
</div>
<div class="toolbar-filters">${
    workspace || data.workspaces.length < 2
      ? ""
      : `<select id="workspace-filter" aria-label="Filter workspace">
<option value="">All workspaces</option>${data.workspaces.map((w) => `<option value="${e(w.id)}">${e(w.name)}</option>`).join("")}</select>`
  }<select id="chrono-order" aria-label="Chronological order" hidden>
<option value="oldest">Oldest first</option>
<option value="newest">Newest first</option>
</select>
<label class="search-box">${icon("search")}<input data-search placeholder="Search inbox" aria-label="Search inbox">
</label>
</div>
</div>
<section id="attention-list" aria-label="Items needing attention">
</section>
`;
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
    return empty("inbox", search || filter ? "No matches" : "No items", "");
  return `${list
    .map((i) => {
      const w = data.workspaces.find((w) => w.id === i.workspace_id);
      return `<button class="attention-row" data-item="${e(i.id)}">
<span class="row-leading">${priority(i.priority)}</span>
<span class="row-workspace">${e(w?.name || "Workspace")}</span>
<span class="row-content"><span class="row-title">${e(i.title)}</span></span>
${badge(i.status)}<time title="${e(fullDate(i.attention_at))}">${relative(i.attention_at)}</time>
${icon("chevron", "row-chevron")}</button>`;
    })
    .join("")}`;
}
export function workspaces(data) {
  return `<div class="page-heading">
<div>
<h1>Workspaces</h1>
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
${w.description ? `<p>${e(w.description)}</p>` : ""}
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
          showDialog(`<div class="dialog-heading"><h2>${e(item.title)}</h2><button class="icon-button" data-close aria-label="Close">${icon("close")}</button></div>
        <div class="attention-detail-meta">${priority(item.priority)}<span>${e(ws.name)}</span>${badge(item.status)}</div>
        ${item.description && !item.run_id ? `<p class="attention-description">${e(item.description)}</p>` : ""}
        <p class="field-note">Since ${e(fullDate(item.attention_at))}</p>
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
              notify("Marked handled");
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
