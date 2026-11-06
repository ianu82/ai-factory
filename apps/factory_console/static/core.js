export const escape = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const paths = {
  inbox: "M4 4h16v12h-5l-2 3h-2l-2-3H4z M4 10h16",
  chat: "M21 11a8 8 0 0 1-8 8H8l-5 3V11a9 9 0 0 1 18 0Z",
  grid: "M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z",
  board: "M3 4h5v16H3z M10 4h5v11h-5z M17 4h5v14h-5z",
  runs: "M12 3a9 9 0 1 1-9 9 M3 3v6h6 M12 7v5l3 2",
  settings:
    "m9 3-1 3-3 1-2 3 2 2v3l3 2 1 3h6l1-3 3-2v-3l2-2-2-3-3-1-1-3z M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0",
  plus: "M12 5v14 M5 12h14",
  arrow: "M5 12h14 m-5-5 5 5-5 5",
  chevron: "m9 5 7 7-7 7",
  down: "m6 9 6 6 6-6",
  close: "m6 6 12 12 M6 18 18 6",
  check: "m5 12 4 4L19 6",
  checkcircle: "M22 11a10 10 0 1 1-6-9 M8 11l4 4 9-10",
  alert: "m12 3 10 18H2z M12 9v4 M12 17h.01",
  clock: "M22 12a10 10 0 1 1-20 0 10 10 0 0 1 20 0 M12 6v6l4 2",
  git: "M6 3v12a6 6 0 0 0 12 0V9 M9 6a3 3 0 1 1-6 0 3 3 0 0 1 6 0 M21 6a3 3 0 1 1-6 0 3 3 0 0 1 6 0",
  search: "M17 10a7 7 0 1 1-14 0 7 7 0 0 1 14 0 m-2 5 6 6",
  send: "m3 3 19 9-19 9 4-9z M7 12h15",
  pause: "M8 5v14 M16 5v14",
  file: "M5 3h10l4 4v14H5z M14 3v6h5 M8 13h8 M8 17h5",
  external: "M14 3h7v7 M21 3l-9 9 M10 3H3v18h18v-7",
  menu: "M3 6h18 M3 12h18 M3 18h18",
  layers: "m12 3 10 5-10 5L2 8z M2 12l10 5 10-5 M2 16l10 5 10-5",
  sort: "M7 3v18 m-4-4 4 4 4-4 M17 21V3 m-4 4 4-4 4 4",
  logout: "M9 3H3v18h6 M12 12h10 m-4-4 4 4-4 4",
  refresh: "M20 7a9 9 0 1 0 1 8 M20 3v5h-5",
  info: "M22 12a10 10 0 1 1-20 0 10 10 0 0 1 20 0 M12 11v6 M12 7h.01",
};
export const icon = (
  name,
  cls = "",
) => `<svg class="icon ${cls}" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.65" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
<path d="${paths[name] || paths.file}"/>
</svg>`;
export const labels = {
  backlog: "Backlog",
  ready: "Ready",
  planning: "Planning",
  building: "Building",
  verifying: "Verifying",
  ci: "Waiting for CI",
  smoking: "Smoke checks",
  complete: "Delivered",
  blocked: "Blocked",
  unknown: "Unverified",
  review: "Review",
};
export const badge = (status, label) => `<span class="badge ${escape(status)}">
<span class="status-dot">
</span>${escape(label || labels[status] || status)}</span>`;
export function relative(stamp) {
  if (!stamp) return "Not observed";
  const secs = Math.max(0, (Date.now() - new Date(stamp).getTime()) / 1000);
  if (secs < 60) return "Just now";
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}
export const fullDate = (stamp) =>
  stamp
    ? new Intl.DateTimeFormat("en-GB", {
        dateStyle: "medium",
        timeStyle: "short",
      }).format(new Date(stamp))
    : "Not recorded";
export function notify(message) {
  const node = document.querySelector("#toast");
  node.textContent = message;
  node.classList.add("show");
  clearTimeout(node.timer);
  node.timer = setTimeout(() => node.classList.remove("show"), 5000);
}
export async function api(path, body) {
  const response = await fetch(path, {
    headers: body ? { "Content-Type": "application/json" } : {},
    method: body ? "POST" : "GET",
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(15000),
  });
  const value = await response.json();
  if (!response.ok)
    throw new Error(
      value.error || "Factory is unavailable. Try again shortly.",
    );
  return value;
}
export const empty = (
  name,
  title,
  copy,
  action = "",
) => `<section class="empty-state">
<div class="empty-symbol">${icon(name)}</div>
<h2>${escape(title)}</h2>
${copy ? `<p>${escape(copy)}</p>` : ""}${action}</section>`;
export function showDialog(content) {
  const dialog = document.querySelector("#dialog");
  dialog.innerHTML = content;
  dialog.showModal();
  dialog
    .querySelector("[data-close]")
    ?.addEventListener("click", () => dialog.close());
  dialog.onclick = (event) => {
    if (event.target === dialog) dialog.close();
  };
}
export const priority = (
  p,
) => `<span class="priority ${escape(p)}" title="${escape(p)} priority">${p === "urgent" ? "!!!" : p === "high" ? "↑" : p === "medium" ? "−" : "↓"}<span>${escape(p)}</span>
</span>`;
export const route = () => location.hash.slice(2).split("/").filter(Boolean);
export const scopedRuns = (data, workspace) =>
  workspace
    ? data.runs.filter((r) =>
        workspace.repositories
          .map((s) => s.toLowerCase())
          .includes(r.repository.toLowerCase()),
      )
    : data.runs;
