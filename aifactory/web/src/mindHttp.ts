import type { api, apiResponse, MindCard, MindCitation, MindHistory, MindStatus, MindThread, MindWireCard, MindWireHistory, MindWireMessage, MindWireStatus, MindWireThread, MindWireUsage } from "./api";
import type { readMindSSE, MindTransport } from "./mind";

export const mindThread = (t: MindWireThread): MindThread => ({ id: t.id, title: t.title, workspace_id: t.billing_workspace_id, cost_usd: t.cost.usd, cost_known: t.cost.known, updated_at: t.updated_at });
const citations = (items: { label: string; href: string }[] = []): MindCitation[] => items.map((item) => ({ label: item.label, url: item.href }));
export function mindCard(c: MindWireCard): MindCard {
  return {
    id: c.id, revision: c.revision, kind: c.kind, workspace_id: c.workspace.id,
    title: c.object.label, fields: c.fields, effect: c.effect,
    status: c.status === "pending" ? "proposed" : c.status === "succeeded" ? "confirmed" : c.status,
    ...(c.result ? { result: { text: c.result.ok ? `Completed: ${c.effect}` : `Not completed: ${c.result.reason || "target changed"}`, citations: citations([c.object]) } } : {}),
  };
}
export const mindMessage = (m: MindWireMessage) => ({ id: m.id, role: m.role, content: m.text, citations: citations(m.citations), cards: (m.action_cards || []).map(mindCard),
  steps: (m.steps || []).map((s) => ({ name: s.name, done: true })), failed: m.status === "failed" || m.status === "interrupted" });
export function mindStatus(s: MindWireStatus): MindStatus {
  return { model: s.model_id, available: s.status === "ready", reason: s.reason, budget_state: "ok" };
}
export function mindBudget(code?: string): MindStatus["budget_state"] {
  return code === "budget_exceeded" ? "budget_exceeded" : code === "accounting_pending" || code === "accounting_busy" ? "pending_accounting" : "ok";
}
const usageBudget = (usage: MindWireUsage): MindStatus["budget_state"] =>
  usage.daily_spend === null ? "pending_accounting" : usage.daily_spend >= usage.daily_limit ? "budget_exceeded" : "ok";
export function mindUsage(s: MindStatus, usage: MindWireUsage): MindStatus {
  return { ...s, budget_state: usageBudget(usage) };
}

/** Dependencies are injected so fixtures exercise the real HTTP/SSE boundary.
 * Readiness is checked on thread load, never on navigation or each token.
 */
export function createMindHttpTransport(request: typeof api, response: typeof apiResponse, readStream: typeof readMindSSE): MindTransport {
  const threadPath = (id: string) => `/api/mind/threads/${encodeURIComponent(id)}`;
  const actionPath = (thread: string, card: MindCard) => `${threadPath(thread)}/actions/${encodeURIComponent(card.id)}`;
  const saved = async (id: string, signal: AbortSignal) => {
    const h = await request<MindWireHistory>(threadPath(id), { signal });
    return { wire: h.thread, thread: mindThread(h.thread), messages: h.messages.filter((m) => m.role === "user" || m.role === "assistant").map(mindMessage) };
  };
  const load = async (id: string, signal: AbortSignal): Promise<MindHistory> => {
    const { wire, thread, messages } = await saved(id, signal);
    const status = mindStatus(await request<MindWireStatus>(`/api/mind/status?workspace_id=${encodeURIComponent(wire.billing_workspace_id)}`, { signal }));
    return { thread, messages, status };
  };
  return {
    list: async (signal) => (await request<MindWireThread[]>("/api/mind/threads", { signal })).map(mindThread),
    create: async (billing_workspace_id, signal) => {
      const t = await request<MindWireThread>("/api/mind/threads", { method: "POST", json: { billing_workspace_id }, signal });
      return load(t.id, signal);
    },
    history: load,
    status: async (workspace, signal) => mindStatus(await request<MindWireStatus>(`/api/mind/status?workspace_id=${encodeURIComponent(workspace)}`, { signal })),
    send: async (thread, body, signal, receive) => {
      let usage: MindWireUsage | undefined;
      let failedBudget: MindStatus["budget_state"] = "ok";
      let terminal = "";
      const res = await response(`${threadPath(thread)}/messages`, {
        method: "POST", headers: { Accept: "text/event-stream" }, signal,
        json: { text: body.content, client_message_id: crypto.randomUUID(), context: body.context },
      });
      let failure: unknown = null;
      await readStream(res, signal, (event) => {
        // T1 envelopes every event; untrusted payloads are rendered as text only.
        const wire = event as unknown as { type: string; data: Record<string, any> };
        const data = wire.data;
        switch (wire.type) {
          case "token": receive({ type: "text_delta", text: data.text }); break;
          // The tool's own name: the page words it (stepLabel); the server summary is a fixed status string.
          case "tool_started": receive({ type: "tool_start", name: data.name || data.summary }); break;
          case "tool_finished": receive({ type: "tool_result", name: data.name || data.summary, citations: citations(data.citations) }); break;
          case "action_card": receive({ type: "action_card", card: mindCard(data.card) }); break;
          case "usage": usage = data as unknown as MindWireUsage; break;
          case "error": {
            const budget = mindBudget(data.code);
            if (budget !== "ok") failedBudget = budget;
            receive({ type: "error", message: data.message, reason: budget === "ok" ? data.code : budget }); break;
          }
          case "done": terminal = data.status; break;
        }
      }).catch((error) => { if (signal.aborted || terminal !== "failed") throw error; failure = error; });
      if (failure) {
        // The server finished and saved the failed turn: show what was saved (any partial reply) and its cost. Only the
        // saved history is read: readiness caches only successes, so checking it again would run and pay for another probe.
        const h = await saved(thread, signal);
        const last = [...h.messages].reverse().find((m) => m.role === "assistant");
        // The failure's own budget state wins over the usage-derived one (a per-turn limit can stop a turn under the daily limit).
        const budget = failedBudget !== "ok" ? failedBudget : usage ? usageBudget(usage) : "ok";
        if (last) receive({ type: "done", message: last, thread: h.thread, ...(budget !== "ok" ? { budget } : {}) });
        throw failure;
      }
      if (terminal !== "complete") throw new Error("Mind reply was interrupted. Reload the thread before retrying.");
      const h = await load(thread, signal);
      const message = [...h.messages].reverse().find((m) => m.role === "assistant");
      if (!message) throw new Error("Mind completed without a reply. Reload the thread.");
      receive({ type: "done", message, thread: h.thread, status: usage ? mindUsage(h.status, usage) : h.status });
    },
    revise: async (thread, card, fields, signal) => mindCard(await request<MindWireCard>(actionPath(thread, card), { method: "PATCH", json: { expected_revision: card.revision, text: fields.text ?? "" }, signal })),
    dismiss: async (thread, card, signal) => mindCard(await request<MindWireCard>(actionPath(thread, card), { method: "PATCH", json: { expected_revision: card.revision, dismiss: true }, signal })),
    confirm: async (thread, card, signal) => mindCard(await request<MindWireCard>(`${actionPath(thread, card)}/confirm`, { method: "POST", json: { expected_revision: card.revision }, signal })),
  };
}
