import {
  api,
  escape as e,
  icon,
  notify,
  route,
  relative,
  showDialog,
} from "./core.js";
import { inbox, workspaces, bindViews } from "./views.js";
import {
  board,
  runs,
  runDetail,
  settings,
  bindWorkViews,
} from "./workviews.js";
import { chat, bindChat } from "./chat.js";
let data = null;
export const refresh = async () => {
  const nextData = await api("/api/state");
  const hash = location.hash;
  const focused = document.activeElement;
  const focusSelector = focused?.id
    ? "#" + CSS.escape(focused.id)
    : focused?.matches("[data-search]")
      ? "[data-search]"
      : null;
  const selection =
    focused && typeof focused.selectionStart === "number"
      ? [focused.selectionStart, focused.selectionEnd]
      : null;
  const history = document.querySelector("#chat-history");
  const scroll = history?.scrollTop;
  const atBottom = history
    ? history.scrollHeight - history.clientHeight - history.scrollTop < 50
    : true;
  data = nextData;
  render();
  if (location.hash === hash) {
    if (focusSelector) {
      const next = document.querySelector(focusSelector);
      next?.focus({ preventScroll: true });
      if (selection && next?.setSelectionRange)
        next.setSelectionRange(...selection);
    }
    const nextHistory = document.querySelector("#chat-history");
    if (nextHistory && !atBottom && scroll !== undefined)
      nextHistory.scrollTop = scroll;
  }
};
const attention = () => data.items.filter((i) => i.attention_at && !i.resolved);
function render() {
  const r = route(),
    ws =
      r[0] === "workspace" ? data.workspaces.find((w) => w.id === r[1]) : null;
  const view = ws ? r[2] || "chat" : r[0] || "inbox";
  const active = data.runs.filter(
    (x) => !["complete", "blocked"].includes(x.status),
  ).length;
  const title = ws
    ? ws.name
    : {
        inbox: "Inbox",
        workspaces: "Workspaces",
        runs: "Runs",
        chat: "Master chat",
        run: "Run detail",
      }[view] || "Factory";
  document.title = `${title} · Factory`;
  document.querySelector("#app").innerHTML = `<div class="app-shell">
<aside class="sidebar" id="sidebar">
<a class="brand" href="#/inbox">
<img src="/assets/minds-icon.svg" alt="MindsHub" width="42" height="42">
<span>MindsHub<small>FACTORY</small>
</span>
</a>
<button class="icon-button sidebar-close" data-close-nav aria-label="Close navigation">${icon("close")}</button>
<nav aria-label="Master navigation">${nav("inbox", "Inbox", "inbox", attention().length)}${nav("chat", "Master chat", "chat")}${nav("runs", "Runs", "runs", active)}</nav>
<div class="workspace-label">
<a class="nav-label" href="#/workspaces">Workspaces</a>
<button class="icon-button" data-new-workspace aria-label="Add workspace">${icon("plus")}</button>
</div>
<nav aria-label="Workspaces">${data.workspaces
    .map(
      (
        w,
      ) => `<a class="nav-link ${ws?.id === w.id ? "active" : ""}" href="#/workspace/${e(w.id)}/chat">
<span class="workspace-monogram">${e(w.name.slice(0, 2).toUpperCase())}</span>
<span>${e(w.name)}</span>
</a>`,
    )
    .join("")}</nav>
<div class="sidebar-bottom">
<span class="connection-label">${data.connection.stale ? "Updates unavailable" : ""}</span>
<button class="icon-button" id="refresh" aria-label="Refresh Factory state" title="${data.connection.observed_at ? "Last updated " + relative(data.connection.observed_at) : "Refresh Factory state"}">${icon("refresh")}</button>
<button class="icon-button" id="logout" aria-label="Sign out" title="Sign out">${icon("logout")}</button>
</div>
</aside>
<button class="nav-scrim" data-close-nav aria-label="Close navigation"></button>
<div class="main-shell">
<header class="topbar">
<button id="mobile-menu" class="icon-button" aria-label="Toggle navigation" aria-expanded="false" aria-controls="sidebar">${icon("menu")}</button>
<a class="mobile-brand" href="#/inbox"><img src="/assets/minds-icon.svg" alt="" width="24" height="24">Factory</a>
</header>
<main id="main">
<div id="view">
</div>
</main>
</div>
</div>`;
  if (ws) document.querySelector("#view").innerHTML = workspaceHeader(ws, view);
  const container = ws
    ? document.querySelector("#workspace-view")
    : document.querySelector("#view");
  if (view === "inbox") container.innerHTML = inbox(data, ws);
  else if (view === "workspaces") container.innerHTML = workspaces(data);
  else if (view === "board" && ws) container.innerHTML = board(data, ws);
  else if (view === "runs") container.innerHTML = runs(data, ws);
  else if (view === "run") container.innerHTML = runDetail(data, r[1]);
  else if (view === "settings" && ws) container.innerHTML = settings(data, ws);
  else if (view === "chat") container.innerHTML = chat(data, ws);
  else
    container.innerHTML = `<section class="empty-state"><h1>Page not found</h1><a class="button secondary" href="#/inbox">Inbox</a></section>`;
  bindViews(data, ws, refresh);
  bindWorkViews(data, ws);
  bindChat(data, ws, refresh);
  document.querySelector("#refresh").onclick = async (event) => {
    const button = event.currentTarget;
    button.disabled = true;
    try {
      await refresh();
    } catch (error) {
      notify(error.message);
    } finally {
      button.disabled = false;
    }
  };
  document.querySelector("#logout").onclick = async () => {
    await api("/api/logout", {});
    location.reload();
  };
  document.querySelector("#mobile-menu").onclick = (event) =>
    event.currentTarget.setAttribute(
      "aria-expanded",
      document.querySelector("#sidebar").classList.toggle("open"),
    );
  document.querySelectorAll("[data-close-nav]").forEach(
    (button) =>
      (button.onclick = () => {
        document.querySelector("#sidebar").classList.remove("open");
        document
          .querySelector("#mobile-menu")
          .setAttribute("aria-expanded", "false");
      }),
  );
  document
    .querySelectorAll("[data-new-workspace]")
    .forEach((b) => (b.onclick = () => workspaceDialog()));
  document
    .querySelectorAll("[data-new-work]")
    .forEach((b) => (b.onclick = () => itemDialog(ws)));
}
function nav(path, label, name, count = 0) {
  const current = route()[0] === "run" ? "runs" : route()[0] || "inbox";
  return `<a class="nav-link ${current === path ? "active" : ""}" href="#/${path}">${icon(name)}<span>${label}</span>${count ? `<span class="nav-count">${count}</span>` : ""}</a>`;
}
function workspaceHeader(ws, view) {
  const waiting = data.items.filter(
    (item) =>
      item.workspace_id === ws.id && item.attention_at && !item.resolved,
  ).length;
  return `<section class="workspace-heading">
<div>
<h1>${e(ws.name)}</h1>
${ws.description ? `<p>${e(ws.description)}</p>` : ""}
</div>
${["board", "inbox"].includes(view) ? `<button class="button primary" data-new-work>${icon("plus")}New work</button>` : `<a class="workspace-repos" href="#/workspace/${e(ws.id)}/settings">${icon("git")}<span>${ws.repositories.length} repositories</span></a>`}
</section>
<nav class="tabs" aria-label="Workspace views">${[
    ["chat", "Master chat"],
    ["board", "Board"],
    ["inbox", "Attention"],
    ["runs", "Runs"],
    ["settings", "Settings"],
  ]
    .map(
      ([v, l]) =>
        `<a href="#/workspace/${e(ws.id)}/${v}" class="${v === view ? "selected" : ""}">${l}${v === "inbox" && waiting ? ` <span class="tab-count">${waiting}</span>` : ""}</a>`,
    )
    .join("")}</nav>
<div id="workspace-view">
</div>`;
}
export function workspaceDialog(ws = null) {
  showDialog(`<form id="workspace-form">
<div class="dialog-heading">
<h2>${ws ? "Workspace settings" : "New workspace"}</h2>
<button type="button" class="icon-button" data-close aria-label="Close">${icon("close")}</button>
</div>
<label>Name<input name="name" required maxlength="100" value="${e(ws?.name || "")}" placeholder="e.g. MindsHub Cowork">
</label>
<label>Description<textarea name="description" maxlength="1000" rows="2" placeholder="What are we building?">${e(ws?.description || "")}</textarea>
</label>
<label>Repositories<span class="field-note">One owner/repository per line. A workspace can span multiple repositories.</span>
<textarea name="repositories" required rows="3" placeholder="mindsdb/cowork&#10;mindsdb/cowork-server">${e(ws?.repositories.join("\n") || "")}</textarea>
</label>
<label>Default mode<select name="mode">
<option value="engineering" ${ws?.mode === "engineering" ? "selected" : ""}>Engineering</option>
<option value="non-coding" ${ws?.mode === "non-coding" ? "selected" : ""}>Non-coding</option>
</select>
</label>
<div class="dialog-footer">
<button type="button" class="button secondary" data-close>Cancel</button>
<button class="button primary">${ws ? "Save changes" : "Create workspace"}</button>
</div>
</form>`);
  let saving = false;
  document.querySelector("#workspace-form").onsubmit = async (event) => {
    event.preventDefault();
    if (saving) return;
    saving = true;
    const submit = event.target.querySelector(".button.primary");
    submit.disabled = true;
    const b = Object.fromEntries(new FormData(event.target));
    b.repositories = b.repositories
      .split("\n")
      .map((s) => s.trim())
      .filter(Boolean);
    try {
      const w = await api("/api/workspaces" + (ws ? "/" + ws.id : ""), b);
      document.querySelector("#dialog").close();
      await refresh();
      if (!ws) location.hash = `#/workspace/${w.id}/chat`;
      else notify("Workspace saved");
    } catch (err) {
      notify(err.message);
    } finally {
      saving = false;
      submit.disabled = false;
    }
  };
  document
    .querySelectorAll("#dialog [data-close]")
    .forEach(
      (b) => (b.onclick = () => document.querySelector("#dialog").close()),
    );
}
export function itemDialog(ws = null, item = null) {
  const initialWorkspace =
    data.workspaces.find((w) => w.id === (item?.workspace_id || ws?.id)) ||
    data.workspaces[0];
  showDialog(`<form id="item-form">
<div class="dialog-heading">
<h2>${item ? "Edit work" : "New work"}</h2>
<button type="button" class="icon-button" data-close aria-label="Close">${icon("close")}</button>
</div>
<label>Workspace<select name="workspace_id">${data.workspaces.map((w) => `<option value="${e(w.id)}" ${w.id === (item?.workspace_id || ws?.id) ? "selected" : ""}>${e(w.name)}</option>`).join("")}</select>
</label>
<label>Title<input name="title" required maxlength="500" placeholder="What needs to be done?" value="${e(item?.title || "")}">
</label>
<label>Brief<textarea name="description" rows="4" maxlength="8000" placeholder="Describe the outcome and what good looks like.">${e(item?.description || "")}</textarea>
</label>
<div class="form-row">
<label>Priority<select name="priority">${["urgent", "high", "medium", "low"].map((p) => `<option ${p === (item?.priority || "medium") ? "selected" : ""}>${p}</option>`).join("")}</select>
</label>
<label>Mode<select name="mode">
<option value="engineering">Engineering</option>
<option value="non-coding" ${(item?.mode || initialWorkspace.mode) === "non-coding" ? "selected" : ""}>Non-coding</option>
</select>
</label>
</div>
<label class="check-label">
<input type="checkbox" name="needs_attention" ${item?.attention_at && !item.resolved ? "checked" : ""}>Needs my attention</label>
<div class="dialog-footer">
<button type="button" class="button secondary" data-close>Cancel</button>
<button class="button primary">${item ? "Save changes" : "Save to backlog"}</button>
</div>
</form>`);
  if (!item)
    document.querySelector("#item-form").elements.workspace_id.onchange = (
      event,
    ) => {
      document.querySelector("#item-form").elements.mode.value =
        data.workspaces.find((w) => w.id === event.target.value).mode;
    };
  let saving = false;
  document.querySelector("#item-form").onsubmit = async (event) => {
    event.preventDefault();
    if (saving) return;
    saving = true;
    const submit = event.target.querySelector(".button.primary");
    submit.disabled = true;
    const b = { ...item, ...Object.fromEntries(new FormData(event.target)) };
    b.needs_attention = event.target.elements.needs_attention.checked;
    if (item?.attention_at) b.resolved = !b.needs_attention;
    try {
      await api("/api/items" + (item ? "/" + item.id : ""), b);
      document.querySelector("#dialog").close();
      await refresh();
      notify(item ? "Work updated" : "Added to backlog");
    } catch (err) {
      notify(err.message);
    } finally {
      saving = false;
      submit.disabled = false;
    }
  };
  document
    .querySelectorAll("#dialog [data-close]")
    .forEach(
      (b) => (b.onclick = () => document.querySelector("#dialog").close()),
    );
}
async function access() {
  document.querySelector("#app").innerHTML = `<main class="access-screen">
<div class="access-brand">
<img src="/assets/minds-icon.svg" alt="MindsHub" width="72" height="72">
<span>MindsHub Factory</span>
</div>
<form id="access-form">
<h1>Open Factory</h1>
<label>Preview code<input name="access_key" type="password" autocomplete="current-password" required>
</label>
<p id="access-error" role="alert">
</p>
<button class="button primary">Open Factory ${icon("arrow")}</button>

</form>
</main>`;
  document.querySelector("#access-form").onsubmit = async (event) => {
    event.preventDefault();
    const button = event.target.querySelector("button");
    button.disabled = true;
    try {
      await api("/api/access", Object.fromEntries(new FormData(event.target)));
      await refresh();
    } catch (err) {
      document.querySelector("#access-error").textContent = err.message;
      button.disabled = false;
    }
  };
}
window.addEventListener("hashchange", () => {
  if (document.querySelector("#dialog").open)
    document.querySelector("#dialog").close();
  if (data) render();
});
window.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    document.querySelector("#sidebar")?.classList.remove("open");
    document
      .querySelector("#mobile-menu")
      ?.setAttribute("aria-expanded", "false");
  }
  if (
    event.key === "/" &&
    !["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement.tagName)
  ) {
    event.preventDefault();
    document.querySelector("[data-search]")?.focus();
  }
});
try {
  const session = await api("/api/session");
  if (session.authenticated) await refresh();
  else await access();
} catch (err) {
  notify(err.message);
  await access();
}
setInterval(async () => {
  if (data && !document.querySelector("#dialog").open) {
    try {
      await refresh();
    } catch {
      const wasConnected = data.connection.connected;
      data.connection.connected = false;
      data.connection.stale = Boolean(data.connection.observed_at);
      render();
      if (wasConnected)
        notify("Connection interrupted. Showing the last received state.");
    }
  }
}, 10000);
