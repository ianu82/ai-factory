import {
  escape as e,
  icon,
  badge,
  relative,
  fullDate,
  scopedRuns,
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
  const active = scopedRuns(data, ws).filter(
    (r) => !["complete", "blocked"].includes(r.status),
  );
  const available =
    data.connection.connected && data.connection.controller.chat_available;
  return `${
    workspace
      ? ""
      : `<div class="page-heading chat-page-heading">
<div>
<span class="eyebrow">MASTER VIEW</span>
<h1>Master chat</h1>
<p>A place to shape the work and keep it moving.</p>
</div>
</div>`
  }<div class="chat-layout ${workspace ? "scoped" : ""}">
<section class="chat-main" aria-label="Master chat">
<header class="chat-scope">
<div>${icon("chat")}<span>Orchestrator</span>
<span class="chat-availability">${available ? "Available" : "Requests saved"}</span>
</div>
<label class="scope-select">
<span>Workspace</span>
<select id="chat-workspace" aria-label="Chat workspace">${data.workspaces.map((w) => `<option value="${e(w.id)}" ${w.id === ws.id ? "selected" : ""}>${e(w.name)}</option>`).join("")}</select>
</label>
<button class="icon-button" data-chat-context aria-label="Show workspace context">${icon("info")}</button>
</header>
<div class="chat-history" id="chat-history" aria-live="polite">${
    messages.length
      ? messages.map((m) => message(m, available)).join("")
      : `<section class="chat-welcome">
<div class="orchestrator-mark">${icon("layers")}</div>
<h2>What are we building next?</h2>
<p>Bring a goal, a bug or a question.<br>Keep the conversation and its context together.</p>
<div class="suggestions">
<button data-suggestion="Summarise the current state of this workspace and what needs attention.">${icon("inbox")}What needs attention? ${icon("arrow")}</button>
<button data-suggestion="Help me break the next product goal into small, verifiable pieces of work.">${icon("board")}Plan the next piece of work ${icon("arrow")}</button>
<button data-suggestion="Help me define a non-coding deliverable with clear acceptance criteria and independent verification.">${icon("file")}Shape a non-coding task ${icon("arrow")}</button>
</div>
</section>`
  }</div>
<div class="composer-area">${
    available
      ? ""
      : `<div class="chat-policy">${icon("info")}<span>Requests are saved to this workspace. Live Orchestrator replies are not enabled yet.</span>
</div>`
  }<form id="chat-form">
<label class="sr-only" for="chat-message">Message the Orchestrator</label>
<textarea id="chat-message" name="content" maxlength="8000" required rows="3" placeholder="Describe the outcome you want…">${e(sessionStorage.getItem("factory-chat-draft-" + ws.id) || "")}</textarea>
<div class="composer-footer">
<span>${icon(ws.mode === "non-coding" ? "file" : "git")}${ws.mode === "non-coding" ? "Non-coding" : "Engineering"}<span class="composer-repos">· ${ws.repositories.length} repositories</span>
</span>
<button class="send-button" aria-label="Send message">${icon("send")}</button>
</div>
</form>
<div class="composer-hint">
<span>Enter to send · Shift + Enter for a new line</span>
<span>Execution follows workspace permissions.</span>
</div>
</div>
</section>
<aside class="chat-context">
<div>
<span class="eyebrow">WORKSPACE CONTEXT</span>
<h2>${e(ws.name)}</h2>
<p>${e(ws.description)}</p>
</div>
<section>
<h3>Repositories <span>${ws.repositories.length}</span>
</h3>
<div class="repo-list">${ws.repositories.map((r) => `<span>${icon("git")}${e(r)}</span>`).join("")}</div>
<a class="quiet-link" href="#/workspace/${e(ws.id)}/settings">Workspace settings ${icon("arrow")}</a>
</section>
<section>
<h3>Work in progress <span>${active.length}</span>
</h3>${
    active.length
      ? active
          .slice(0, 4)
          .map(
            (r) => `<a class="chat-run" href="#/run/${e(r.id)}">
<strong>${e(r.title)}</strong>${badge(r.status)}</a>`,
          )
          .join("")
      : `<p class="context-empty">${data.connection.connected ? "No open runs in the current feed." : "Awaiting the Factory feed."}</p>`
  }</section>
<section class="context-permissions">${icon("checkcircle")}<p>Plans, verification and delivery decisions stay connected to their recorded evidence.</p>
</section>
</aside>
</div>`;
}
function message(m, available) {
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
  document.querySelector("#chat-workspace").onchange = (event) => {
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
  document.querySelector("#chat-form").onsubmit = async (event) => {
    event.preventDefault();
    const content = textarea.value.trim();
    if (!content) return;
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
      if (message.status === "saved")
        notify("Saved to workspace. You can add this request to the backlog.");
    } catch (err) {
      notify(err.message);
      button.disabled = false;
    }
  };
  document.querySelectorAll("[data-suggestion]").forEach(
    (b) =>
      (b.onclick = () => {
        textarea.value = b.dataset.suggestion;
        textarea.dispatchEvent(new Event("input"));
        textarea.focus();
      }),
  );
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
  document.querySelector("[data-chat-context]").onclick = () =>
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
