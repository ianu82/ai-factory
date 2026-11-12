import type { MindCard, MindContext, MindEvent, MindHistory, MindMessage, MindStatus, MindStep, MindThread, Role } from "./api";

/** Incremental UTF-8/SSE decoder; handles CR, LF, CRLF and multiline data. */
export async function readMindSSE(response: Response, signal: AbortSignal, receive: (event: MindEvent) => void): Promise<void> {
  if (!response.ok) throw new Error(`Request failed (${response.status})`);
  if (!response.body) throw new Error("Mind stream has no body");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "", data: string[] = [], eventName = "", terminal = false;
  // An error frame is reported at once but does not end the read: the server still sends usage and done, and the
  // caller needs them to reload the saved turn. The error is thrown once the stream is complete.
  let failure: MindEvent | null = null;
  const abort = () => { void reader.cancel().catch(() => {}); };
  signal.addEventListener("abort", abort, { once: true });
  const dispatch = () => {
    if (!data.length || terminal) { data = []; eventName = ""; return; }
    const raw = data.join("\n");
    data = [];
    let event: MindEvent;
    try {
      const value = JSON.parse(raw);
      event = { ...value.data, ...value, type: value.type || eventName };
    } catch { throw new Error("Invalid Mind stream event"); }
    eventName = "";
    if (!event || typeof event.type !== "string") throw new Error("Invalid Mind stream event");
    receive(event);
    if (event.type === "error") failure = failure || event;
    if (event.type === "done") terminal = true;
  };
  const line = (value: string) => {
    if (!value) { dispatch(); return; }
    if (value.startsWith(":")) return;
    const colon = value.indexOf(":");
    const key = colon < 0 ? value : value.slice(0, colon);
    let valuePart = colon < 0 ? "" : value.slice(colon + 1);
    if (valuePart.startsWith(" ")) valuePart = valuePart.slice(1);
    if (key === "data") data.push(valuePart);
    if (key === "event") eventName = valuePart;
  };
  const drain = (end: boolean) => {
    for (;;) {
      const index = buffer.search(/[\r\n]/);
      if (index < 0 || (!end && buffer[index] === "\r" && index === buffer.length - 1)) break;
      const width = buffer[index] === "\r" && buffer[index + 1] === "\n" ? 2 : 1;
      line(buffer.slice(0, index)); buffer = buffer.slice(index + width);
      if (terminal) return;
    }
    if (end && buffer) { line(buffer); buffer = ""; }
    if (end) dispatch();
  };
  try {
    if (signal.aborted) throw new DOMException("Cancelled", "AbortError");
    while (!terminal) {
      const { value, done } = await reader.read();
      if (signal.aborted) throw new DOMException("Cancelled", "AbortError");
      buffer += decoder.decode(value, { stream: !done });
      drain(done);
      if (done) break;
    }
    if (failure) throw new Error((failure as { message?: string }).message || "Mind request failed");
    if (!terminal) throw new Error("Mind stream ended before completion. Reload history before retrying.");
  } finally {
    signal.removeEventListener("abort", abort);
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export interface MindTransport {
  list(signal: AbortSignal, workspaceId?: string): Promise<MindThread[]>;
  create(workspace: string, signal: AbortSignal): Promise<MindHistory>;
  history(thread: string, signal: AbortSignal): Promise<MindHistory>;
  /** Whether Mind can answer for this workspace now (no thread needed). */
  status?(workspace: string, signal: AbortSignal): Promise<MindStatus>;
  send(thread: string, body: { content: string; context: MindContext | null }, signal: AbortSignal, receive: (event: MindEvent) => void): Promise<void>;
  revise(thread: string, card: MindCard, fields: Record<string, string>, signal: AbortSignal): Promise<MindCard>;
  dismiss(thread: string, card: MindCard, signal: AbortSignal): Promise<MindCard>;
  confirm(thread: string, card: MindCard, signal: AbortSignal): Promise<MindCard>;
}
export interface MindState {
  userId: string | null; open: boolean; threads: MindThread[]; active: MindThread | null;
  messages: MindMessage[]; status: MindStatus | null; context: MindContext | null;
  busy: boolean; pendingCards: string[]; draft: string; progress: string; error: string; announcement: string; announcementId: number;
  /** The current (or last) turn's steps and when it started, for the live "Working for" line and the rail. */
  steps: MindStep[]; startedAt: number | null;
}
const initial = (userId: string | null): MindState => ({ userId, open: false, threads: [], active: null, messages: [], status: null, context: null, busy: false, pendingCards: [], draft: "", progress: "", error: "", announcement: "", announcementId: 0, steps: [], startedAt: null });
export const mindPrivateKey = (userId: string) => ["mind", userId] as const;
export function canConfirm(card: MindCard, roles: Record<string, Role>, pending: string[] = []): boolean {
  const role = roles[card.workspace_id];
  return card.status === "proposed" && (role === "owner" || role === "admin" || role === "member") && !pending.includes(card.id);
}
export function validMindBudget(value: string): boolean {
  return value.trim() !== "" && Number.isFinite(Number(value)) && Number(value) >= 0;
}
export function mindCost(thread: MindThread): string {
  return thread.cost_known && thread.cost_usd !== null ? `$${thread.cost_usd.toFixed(4)}` : "Unknown (accounting pending)";
}

/** One instance per provider, outside routes. All private state belongs to exactly one user. */
export class MindController {
  private state: MindState = initial(null);
  private listeners = new Set<() => void>();
  private controllers = new Set<AbortController>();
  private generation = 0;
  private roles: Record<string, Role> = {};
  private routeKey = "";
  private listFailure = ""; // the error a failed thread list left; only a later successful list clears it
  private listWorkspaceId: string | undefined;
  private listRequest = 0; // only the newest thread-list request updates the list and its error
  constructor(privateTransport: MindTransport) { this.transport = privateTransport; }
  private transport: MindTransport;
  getSnapshot = () => this.state;
  subscribe = (listener: () => void) => { this.listeners.add(listener); return () => { this.listeners.delete(listener); }; };
  private update(patch: Partial<MindState>) { this.state = { ...this.state, ...patch }; this.listeners.forEach((fn) => fn()); }
  setIdentity(userId: string | null, roles: Record<string, Role> = {}) {
    this.roles = roles;
    if (userId === this.state.userId) return;
    this.cancel(); this.routeKey = ""; this.listFailure = ""; this.listWorkspaceId = undefined; this.state = initial(userId); this.update({});
  }
  cancel() {
    this.generation++;
    this.controllers.forEach((c) => c.abort()); this.controllers.clear();
    this.update({ busy: false, pendingCards: [], draft: "", progress: "" });
  }
  setOpen(open: boolean) { this.update({ open }); }
  navigate(key: string, context: MindContext | null) {
    if (key === this.routeKey) return;
    this.routeKey = key; this.update({ context });
  }
  setContext(context: MindContext | null) { this.update({ context }); }
  removeContext() { this.update({ context: null }); }
  private async request<T>(work: (signal: AbortSignal) => Promise<T>, apply: (value: T) => void, failed?: (message: string) => void,
                           current: () => boolean = () => true) {
    if (!this.state.userId) return;
    const controller = new AbortController(), generation = this.generation;
    this.controllers.add(controller);
    try {
      const result = await work(controller.signal);
      if (generation === this.generation && !controller.signal.aborted && current()) apply(result);
    } catch (error) {
      if (generation === this.generation && !controller.signal.aborted && current()) {
        const message = (error as Error).message;
        this.update({ error: message }); failed?.(message);
      }
    } finally { this.controllers.delete(controller); }
  }
  async list(workspaceId?: string) {
    const request = ++this.listRequest;
    if (workspaceId !== this.listWorkspaceId) {
      this.listWorkspaceId = workspaceId;
      // Do not leave another factory's conversations visible while the new list loads (or if it fails).
      this.update({ threads: this.state.threads.filter((t) => workspaceId === undefined || t.workspace_id === workspaceId) });
    }
    await this.request((s) => this.transport.list(s, workspaceId), (threads) => {
      threads = threads.filter((t) => workspaceId === undefined || t.workspace_id === workspaceId);
      // Clear only the error an earlier failed list left; a newer error from another action (send, confirm) stays.
      const stale = this.listFailure !== "" && this.state.error === this.listFailure;
      this.listFailure = "";
      this.update(stale ? { threads, error: "" } : { threads });
    }, (message) => { this.listFailure = message; }, () => request === this.listRequest);
  }
  private load(history: MindHistory) {
    const threads = this.listWorkspaceId === undefined || history.thread.workspace_id === this.listWorkspaceId
      ? [history.thread, ...this.state.threads.filter((t) => t.id !== history.thread.id)] : this.state.threads;
    this.update({ messages: history.messages, status: history.status, active: history.thread, threads, busy: false, error: "", draft: "", progress: "" });
  }
  async create(workspace: string) {
    if (!this.state.userId) return;
    this.cancel(); const generation = this.generation;
    this.update({ busy: true, error: "" });
    await this.request((s) => this.transport.create(workspace, s), (h) => this.load(h));
    if (generation === this.generation) this.update({ busy: false });
  }
  async select(thread: string) {
    if (!this.state.userId) return;
    this.cancel(); const generation = this.generation;
    this.update({ busy: true, active: null, messages: [], status: null, error: "" });
    await this.request((s) => this.transport.history(thread, s), (h) => this.load(h));
    if (generation === this.generation) this.update({ busy: false });
  }
  /** Load whether Mind can answer for a workspace, before any conversation exists. */
  /** The error a failed status check left, so a later successful check clears it (and nothing else). */
  private statusError = "";
  async checkStatus(workspace: string) {
    const status = this.transport.status;
    if (!status) return;
    // A check that fails (offline, timed out, access changed) is a definite answer too: Mind can't answer now.
    await this.request((s) => status.call(this.transport, workspace, s), (value) => {
      const stale = !!this.statusError && this.state.error === this.statusError;
      this.statusError = "";
      this.update({ status: value, ...(stale ? { error: "" } : {}) });
    }, (message) => {
      this.statusError = message;
      this.update({ status: { model: null, available: false, reason: `Couldn't check whether Mind can answer (${message}).`, budget_state: "ok" } });
    });
  }
  /** Leave the current conversation (a new one starts on the next send). */
  leave() { this.cancel(); this.update({ active: null, messages: [], error: "", steps: [], startedAt: null }); }
  /** A new conversation that opens with the person's first message. Returns the new thread's id. */
  async start(workspace: string, content: string): Promise<string | null> {
    if (!content.trim()) return null;
    // Check first, so a refused first message never leaves an empty conversation behind.
    await this.checkStatus(workspace);
    const ready = this.state.status;
    if (this.transport.status && (!ready?.available || ready.budget_state !== "ok")) return null;
    await this.create(workspace);
    const { active, status } = this.state;
    // Open the conversation only when its first message can actually be sent.
    if (!active || !status?.available || status.budget_state !== "ok") return null;
    void this.send(content);
    return active.id;
  }
  async reload() { if (this.state.active) await this.select(this.state.active.id); else await this.list(this.listWorkspaceId); }
  async send(content: string) {
    const { active, context, status, busy } = this.state;
    if (!content.trim() || !active || busy || !status?.available || status.budget_state !== "ok") return;
    const startedAt = Date.now();
    this.update({ busy: true, draft: "", progress: "Thinking…", error: "", steps: [], startedAt, messages: [...this.state.messages, { id: `local-${startedAt}`, role: "user", content: content.trim() }] });
    const generation = this.generation;
    await this.request((signal) => this.transport.send(active.id, { content: content.trim(), context }, signal, (event) => {
      if (generation !== this.generation || signal.aborted) return;
      switch (event.type) {
        case "text_delta": this.update({ draft: this.state.draft + event.text }); break;
        case "tool_start": this.update({ progress: stepLabel(event.name, false), steps: [...this.state.steps, { name: event.name, done: false }] }); break;
        case "tool_result": {
          const steps = [...this.state.steps];
          const open = steps.findIndex((s) => s.name === event.name && !s.done);
          const done = { name: event.name, done: true, citations: event.citations?.length || 0 };
          if (open >= 0) steps[open] = done; else steps.push(done);
          this.update({ progress: stepLabel(event.name, true), steps }); break;
        }
        case "status": this.update({ status: event.status }); break;
        case "action_card": {
          // A proposal is inert. Only confirm() can call the action endpoint.
          this.update({ messages: [...this.state.messages, { id: `card-${event.card.id}`, role: "assistant", content: "", cards: [event.card] }] }); break;
        }
        case "done": {
          // The saved reply, with the steps and duration this browser watched.
          const message = { ...event.message, steps: this.state.steps.length ? this.state.steps : event.message.steps, durationMs: Date.now() - startedAt };
          const kept = this.state.status;
          const status = event.status ?? (kept && event.budget ? { ...kept, budget_state: event.budget } : kept);
          this.update({ active: event.thread, status, messages: [...this.state.messages.filter((m) => !m.id.startsWith("card-")), message], draft: "", announcement: `Mind: ${event.message.content}`, announcementId: this.state.announcementId + 1 }); break;
        }
        case "error": this.update({ error: event.message, ...(event.reason === "budget_exceeded" || event.reason === "pending_accounting" ? { status: { ...this.state.status!, budget_state: event.reason } } : {}) }); break;
      }
    }), () => {});
    if (generation === this.generation) this.update({ busy: false, progress: "" });
  }
  private async cardAction(card: MindCard, action: "revise" | "dismiss" | "confirm", fields?: Record<string, string>) {
    const active = this.state.active;
    if (!active || this.state.busy || this.state.pendingCards.includes(card.id) || card.status !== "proposed") return;
    const current = this.state.messages.flatMap((m) => m.cards || []).find((c) => c.id === card.id);
    if (!current || current.revision !== card.revision) return;
    if (action === "confirm" && !canConfirm(current, this.roles, this.state.pendingCards)) return;
    const generation = this.generation;
    this.update({ pendingCards: [...this.state.pendingCards, card.id], error: "" });
    await this.request((s) => action === "revise" ? this.transport.revise(active.id, current, fields!, s) : this.transport[action](active.id, current, s), (result) => {
      this.update({ messages: this.state.messages.map((m) => ({ ...m, cards: m.cards?.map((c) => c.id === result.id ? result : c) })), announcement: action === "confirm" ? result.result?.text || "Action confirmed" : "", announcementId: action === "confirm" ? this.state.announcementId + 1 : this.state.announcementId });
    });
    if (generation === this.generation) this.update({ pendingCards: this.state.pendingCards.filter((id) => id !== card.id) });
  }
  revise(card: MindCard, fields: Record<string, string>) { return this.cardAction(card, "revise", fields); }
  dismiss(card: MindCard) { return this.cardAction(card, "dismiss"); }
  /** Call ONLY from the explicit Confirm button, never from send or lifecycle code. */
  confirm(card: MindCard) { return this.cardAction(card, "confirm"); }
}

export function mindRouteContext(pathname: string): MindContext | null {
  const parts = pathname.split("/").filter(Boolean);
  if (parts[0] !== "w" || !parts[1]) return null;
  const [workspace_id, kind = "inbox", id] = parts.slice(1);
  const object = id && (kind === "outcomes" || kind === "inbox");
  return { workspace_id, kind: object ? kind === "outcomes" ? "outcome" : "decision" : kind, id: object ? id : workspace_id, ...(object && kind === "outcomes" ? { outcome_id: id } : {}), label: object ? `${kind === "outcomes" ? "Outcome" : "Decision"} ${id}` : kind[0].toUpperCase() + kind.slice(1) };
}
export function mindSuggestions(context: MindContext | null): string[] {
  switch (context?.kind) {
    case "outcome": return ["What is blocking this outcome?", "Summarize progress and the next decision.", "Why did the latest attempt fail?"];
    case "outcomes": return ["Which outcomes need attention?", "What changed across our outcomes?"];
    case "inbox": case "decision": case "decisions": return ["Which decisions should I handle first?", "Explain the risks behind pending approvals."];
    case "runs": return ["Why are recent runs failing?", "Which runs account for the most spending?"];
    case "pulls": return ["Which pull requests are ready?", "What is blocking our merges?"];
    case "signals": return ["Which signals are recurring?", "What evidence supports the highest priority signal?"];
    case "settings": return ["Explain this workspace’s autonomy policy.", "How does my Mind daily budget work?"];
    case "onboarding": return ["What setup is still missing?", "Explain which permissions this workspace needs."];
    default: return ["What needs my attention across workspaces?", "What changed and why?"];
  }
}
/** Only trusted in-app route shapes become router links. No protocol-relative URLs. */
export function mindLink(url: string): "internal" | "external" | null {
  if (/^\/w\/[^/?#]+\/(?:inbox|outcomes|runs|pulls|signals|decisions|settings|mind)(?:[/?#]|$)/.test(url) && !/[\\\u0000-\u0020]/.test(url)) return "internal";
  try { const parsed = new URL(url); return parsed.protocol === "https:" || parsed.protocol === "http:" ? "external" : null; } catch { return null; }
}

/** What a Factory tool does, in the words a person uses. Unknown tools keep their own name. */
const STEP_LABELS: Record<string, [string, string]> = {
  overview: ["Checking the workspace overview", "Checked the workspace overview"],
  list_outcomes: ["Looking through outcomes", "Looked through outcomes"],
  list_tasks: ["Looking through tasks", "Looked through tasks"],
  outcome_detail: ["Reading an outcome", "Read an outcome"],
  task_detail: ["Reading a task", "Read a task"],
  open_decisions: ["Checking open decisions", "Checked open decisions"],
  runs: ["Reading recent runs", "Read recent runs"],
  recent_events: ["Reading recent activity", "Read recent activity"],
  pull_requests: ["Checking pull requests", "Checked pull requests"],
  propose_send_guidance: ["Drafting guidance for your approval", "Drafted guidance for your approval"],
  propose_answer_question: ["Drafting an answer for your approval", "Drafted an answer for your approval"],
  propose_retry_task: ["Preparing a retry for your approval", "Prepared a retry for your approval"],
  propose_resolve_decision: ["Preparing a decision for your approval", "Prepared a decision for your approval"],
  propose_create_outcome: ["Drafting a new outcome for your approval", "Drafted a new outcome for your approval"],
};
export function stepLabel(name: string, done: boolean): string {
  const known = STEP_LABELS[name];
  if (known) return known[done ? 1 : 0];
  const words = name.replace(/^propose_/, "").replace(/_/g, " ");
  return name.startsWith("propose_") ? `${done ? "Prepared" : "Preparing"} ${words} for your approval` : `${done ? "Used" : "Using"} ${words}`;
}
export function formatDuration(ms: number | null | undefined): string | null {
  if (ms == null || ms < 0) return null;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return m < 60 ? `${m}m ${s % 60}s` : `${Math.floor(m / 60)}h ${m % 60}m`;
}
