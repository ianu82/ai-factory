import type { MindCard, MindContext, MindEvent, MindHistory, MindMessage, MindStatus, MindThread, Role } from "./api";

/** Incremental UTF-8/SSE decoder; handles CR, LF, CRLF and multiline data. */
export async function readMindSSE(response: Response, signal: AbortSignal, receive: (event: MindEvent) => void): Promise<void> {
  if (!response.ok) throw new Error(`Request failed (${response.status})`);
  if (!response.body) throw new Error("Mind stream has no body");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "", data: string[] = [], eventName = "", terminal = false;
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
    if (event.type === "error") { terminal = true; throw new Error(event.message || "Mind request failed"); }
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
    if (!terminal) throw new Error("Mind stream ended before completion. Reload history before retrying.");
  } finally {
    signal.removeEventListener("abort", abort);
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export interface MindTransport {
  list(signal: AbortSignal): Promise<MindThread[]>;
  create(workspace: string, signal: AbortSignal): Promise<MindHistory>;
  history(thread: string, signal: AbortSignal): Promise<MindHistory>;
  send(thread: string, body: { content: string; context: MindContext | null }, signal: AbortSignal, receive: (event: MindEvent) => void): Promise<void>;
  revise(thread: string, card: MindCard, fields: Record<string, string>, signal: AbortSignal): Promise<MindCard>;
  dismiss(thread: string, card: MindCard, signal: AbortSignal): Promise<MindCard>;
  confirm(thread: string, card: MindCard, signal: AbortSignal): Promise<MindCard>;
}
export interface MindState {
  userId: string | null; open: boolean; threads: MindThread[]; active: MindThread | null;
  messages: MindMessage[]; status: MindStatus | null; context: MindContext | null;
  busy: boolean; pendingCards: string[]; draft: string; progress: string; error: string; announcement: string; announcementId: number;
}
const initial = (userId: string | null): MindState => ({ userId, open: false, threads: [], active: null, messages: [], status: null, context: null, busy: false, pendingCards: [], draft: "", progress: "", error: "", announcement: "", announcementId: 0 });
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
  private listRequest = 0; // only the newest thread-list request updates the list and its error
  constructor(privateTransport: MindTransport) { this.transport = privateTransport; }
  private transport: MindTransport;
  getSnapshot = () => this.state;
  subscribe = (listener: () => void) => { this.listeners.add(listener); return () => { this.listeners.delete(listener); }; };
  private update(patch: Partial<MindState>) { this.state = { ...this.state, ...patch }; this.listeners.forEach((fn) => fn()); }
  setIdentity(userId: string | null, roles: Record<string, Role> = {}) {
    this.roles = roles;
    if (userId === this.state.userId) return;
    this.cancel(); this.routeKey = ""; this.listFailure = ""; this.state = initial(userId); this.update({});
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
  async list() {
    const request = ++this.listRequest;
    await this.request((s) => this.transport.list(s), (threads) => {
      // Clear only the error an earlier failed list left; a newer error from another action (send, confirm) stays.
      const stale = this.listFailure !== "" && this.state.error === this.listFailure;
      this.listFailure = "";
      this.update(stale ? { threads, error: "" } : { threads });
    }, (message) => { this.listFailure = message; }, () => request === this.listRequest);
  }
  private load(history: MindHistory) {
    this.update({ messages: history.messages, status: history.status, active: history.thread, threads: [history.thread, ...this.state.threads.filter((t) => t.id !== history.thread.id)], busy: false, error: "", draft: "", progress: "" });
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
  async reload() { if (this.state.active) await this.select(this.state.active.id); else await this.list(); }
  async send(content: string) {
    const { active, context, status, busy } = this.state;
    if (!content.trim() || !active || busy || !status?.available || status.budget_state !== "ok") return;
    this.update({ busy: true, draft: "", progress: "Thinking…", error: "", messages: [...this.state.messages, { id: `local-${Date.now()}`, role: "user", content: content.trim() }] });
    const generation = this.generation;
    await this.request((signal) => this.transport.send(active.id, { content: content.trim(), context }, signal, (event) => {
      if (generation !== this.generation || signal.aborted) return;
      switch (event.type) {
        case "text_delta": this.update({ draft: this.state.draft + event.text }); break;
        case "tool_start": this.update({ progress: `Reading ${event.name}…` }); break;
        case "tool_result": this.update({ progress: `Read ${event.name}` }); break;
        case "status": this.update({ status: event.status }); break;
        case "action_card": {
          // A proposal is inert. Only confirm() can call the action endpoint.
          this.update({ messages: [...this.state.messages, { id: `card-${event.card.id}`, role: "assistant", content: "", cards: [event.card] }] }); break;
        }
        case "done": this.update({ active: event.thread, status: event.status, messages: [...this.state.messages.filter((m) => !m.id.startsWith("card-")), event.message], draft: "", announcement: `Mind: ${event.message.content}`, announcementId: this.state.announcementId + 1 }); break;
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
/** What the dock shows for its context: the outcome's number and title, or the decision's title, from what the page
 * already loaded; never a raw database ID. */
export function mindContextLabel(context: MindContext, loaded: { outcome?: { number: number; title: string } | null;
                                                                  decision?: { title: string } | null } = {}): string {
  const clip = (text: string) => text.length > 60 ? `${text.slice(0, 59).trimEnd()}…` : text;
  if (context.kind === "outcome") return loaded.outcome ? clip(`#${loaded.outcome.number} ${loaded.outcome.title}`) : "This outcome";
  if (context.kind === "decision") return loaded.decision ? clip(loaded.decision.title) : "This decision";
  return context.label;
}
/** The dock header's model line: the model the status reports, "Unavailable" when it reports none can run. */
export function mindModelLine(status: MindStatus | null): string {
  if (status && !status.available) return "Unavailable";
  return status?.model || "Claude Opus 5.5";
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
  if (/^\/w\/[^/?#]+\/(?:inbox|outcomes|runs|pulls|signals|decisions|settings)(?:[/?#]|$)/.test(url) && !/[\\\u0000-\u0020]/.test(url)) return "internal";
  try { const parsed = new URL(url); return parsed.protocol === "https:" || parsed.protocol === "http:" ? "external" : null; } catch { return null; }
}
