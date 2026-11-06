import {
  escape as e,
  icon,
  relative,
  fullDate,
  api,
  notify,
  showDialog,
} from "./core.js";
import { itemDialog } from "./app.js";
let selected = sessionStorage.getItem("factory-chat-workspace") || "factory";
export function chat(data, workspace) {
  const ws =
    workspace ||
    data.workspaces.find((w) => w.id === selected) ||
    data.workspaces[0];
  selected = ws.id;
  const messages = data.messages.filter((m) => m.workspace_id === ws.id);
  const available =
    data.connection.connected && data.connection.controller.chat_available;
  return `${workspace ? "" : '<div class="page-heading chat-page-heading"><h1>Master chat</h1></div>'}
<div class="chat-layout ${workspace ? "scoped" : ""}">
<section class="chat-main" aria-label="Master chat">
${
  workspace
    ? ""
    : `<header class="chat-scope">
<select id="chat-workspace" aria-label="Chat workspace">${data.workspaces.map((w) => `<option value="${e(w.id)}" ${w.id === ws.id ? "selected" : ""}>${e(w.name)}</option>`).join("")}</select>
<button class="context-button" data-chat-context aria-label="Workspace context">${icon("git")}${ws.repositories.length} repositories</button>
</header>`
}
<div class="chat-history" id="chat-history" aria-live="polite">${messages.length ? messages.map((m) => message(m)).join("") : `<section class="chat-welcome"><h2>${available ? "What would you like to work on?" : "Start with a brief"}</h2></section>`}</div>
<div class="composer-area">
${available ? "" : '<p class="chat-policy">Orchestrator replies aren’t connected yet.</p>'}
<form id="chat-form">
<label class="sr-only" for="chat-message">${available ? "Message the Orchestrator" : "Save a brief"}</label>
<textarea id="chat-message" name="content" maxlength="8000" required rows="3" placeholder="${available ? "Message the Orchestrator…" : "Describe what you want to build or change…"}">${e(sessionStorage.getItem("factory-chat-draft-" + ws.id) || "")}</textarea>
<div class="composer-footer"><span>${ws.mode === "non-coding" ? "Non-coding" : "Engineering"}</span>
<button class="button primary" aria-label="${available ? "Send message" : "Save brief"}">${available ? "Send" : "Save brief"}${icon(available ? "send" : "plus")}</button></div>
</form>
</div></section></div>`;
}

function message(m) {
  return `<article class="chat-message ${m.role}">
<div class="message-avatar">${m.role === "user" ? "M" : icon("layers")}</div>
<div class="message-body">
<header>
<strong>${m.role === "user" ? "You" : "Factory Orchestrator"}</strong>
<time title="${e(fullDate(m.created_at))}">${relative(m.created_at)}</time>${m.model ? `<small>${e(m.model)}</small>` : ""}</header>
<div class="message-content">${e(m.content)}</div>${
    m.role === "user"
      ? `<footer>${m.status === "saved" ? `<span>${icon("check")}Saved to workspace</span>` : m.status === "pending" || m.status === "claimed" ? `<span>${icon("clock")}Awaiting Orchestrator reply</span>` : ""}<button class="quiet-link" data-task-from-message="${e(m.id)}">Add to backlog ${icon("arrow")}</button>
</footer>`
      : ""
  }</div>
</article>`;
}
export function bindChat(data, workspace, refresh) {
  const ws =
      workspace ||
      data.workspaces.find((w) => w.id === selected) ||
      data.workspaces[0],
    textarea = document.querySelector("#chat-message");
  if (!textarea) return;
  const workspaceSelect = document.querySelector("#chat-workspace");
  if (workspaceSelect)
    workspaceSelect.onchange = (event) => {
      selected = event.target.value;
      sessionStorage.setItem("factory-chat-workspace", selected);
      location.hash = `#/workspace/${selected}/chat`;
    };
  textarea.oninput = () =>
    sessionStorage.setItem("factory-chat-draft-" + ws.id, textarea.value);
  textarea.onkeydown = (event) => {
    if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
      event.preventDefault();
      document.querySelector("#chat-form").requestSubmit();
    }
  };
  let saving = false;
  document.querySelector("#chat-form").onsubmit = async (event) => {
    event.preventDefault();
    if (saving) return;
    const content = textarea.value.trim();
    if (!content) return;
    saving = true;
    const button = event.target.querySelector("button");
    button.disabled = true;
    try {
      const message = await api("/api/messages", {
        workspace_id: ws.id,
        content,
      });
      sessionStorage.removeItem("factory-chat-draft-" + ws.id);
      await refresh();
      document.querySelector("#chat-message")?.focus({ preventScroll: true });
      const history = document.querySelector("#chat-history");
      history.scrollTop = history.scrollHeight;
      if (message.status === "saved") notify("Brief saved");
    } catch (err) {
      notify(err.message);
    } finally {
      saving = false;
      button.disabled = false;
    }
  };
  document.querySelectorAll("[data-task-from-message]").forEach(
    (b) =>
      (b.onclick = () => {
        const m = data.messages.find((x) => x.id === b.dataset.taskFromMessage);
        itemDialog(ws);
        const form = document.querySelector("#item-form");
        form.elements.title.value = m.content.split("\n")[0].slice(0, 180);
        form.elements.description.value = m.content;
      }),
  );
  const contextButton = document.querySelector("[data-chat-context]");
  if (contextButton)
    contextButton.onclick = () =>
      showDialog(`<div class="dialog-heading">
<h2>${e(ws.name)}</h2>
<button class="icon-button" data-close aria-label="Close">${icon("close")}</button>
</div>
<p>${e(ws.description)}</p>
<div class="repo-list">${ws.repositories.map((r) => `<span>${icon("git")}${e(r)}</span>`).join("")}</div>
<div class="dialog-footer">
<a class="button secondary" href="#/workspace/${e(ws.id)}/settings" id="open-context-settings">Workspace settings</a>
</div>`);
  const history = document.querySelector("#chat-history");
  history.scrollTop = history.scrollHeight;
}
