import type { api, apiResponse, MindCard, MindCitation, MindHistory, MindStatus, MindThread, MindWireCard, MindWireHistory, MindWireMessage, MindWireStatus, MindWireThread, MindWireUsage } from "./api";
import type { readMindSSE, MindTransport } from "./mind";

export const mindThread = (t: MindWireThread): MindThread => ({ id: t.id, title: t.title, workspace_id: t.billing_workspace_id, cost_usd: t.cost.usd, cost_known: t.cost.known });
const citations = (items: { label: string; href: string }[] = []): MindCitation[] => items.map((item) => ({ label: item.label, url: item.href }));
export function mindCard(c: MindWireCard): MindCard {
  return {
    id: c.id, revision: c.revision, kind: c.kind, workspace_id: c.workspace.id,
    title: c.object.label, fields: c.fields, effect: c.effect,
    status: c.status === "pending" ? "proposed" : c.status === "succeeded" ? "confirmed" : c.status,
    ...(c.result ? { result: { text: c.result.ok ? `Completed: ${c.effect}` : `Not completed: ${c.result.reason || "target changed"}`, citations: citations([c.object]) } } : {}),
  };
}
export const mindMessage = (m: MindWireMessage) => ({ id: m.id, role: m.role, content: m.text, citations: citations(m.citations), cards: (m.action_cards || []).map(mindCard) });
export function mindStatus(s: MindWireStatus): MindStatus {
  return { model: s.model_id, available: s.status === "ready", reason: s.reason, budget_state: "ok" };
}
export function mindBudget(code?: string): MindStatus["budget_state"] {
  return code === "budget_exceeded" ? "budget_exceeded" : code === "accounting_pending" || code === "accounting_busy" ? "pending_accounting" : "ok";
}
export function mindUsage(s: MindStatus, usage: MindWireUsage): MindStatus {
  return { ...s, budget_state: usage.daily_spend === null ? "pending_accounting" : usage.daily_spend >= usage.daily_limit ? "budget_exceeded" : "ok" };
}

/** Dependencies are injected so fixtures exercise the real HTTP/SSE boundary.
 * Readiness is checked on thread load, never on navigation or each token.
 */
export function createMindHttpTransport(request: typeof api, response: typeof apiResponse, readStream: typeof readMindSSE): MindTransport {
  const threadPath = (id: string) => `/api/mind/threads/${encodeURIComponent(id)}`;
  const actionPath = (thread: string, card: MindCard) => `${threadPath(thread)}/actions/${encodeURIComponent(card.id)}`;
  const load = async (id: string, signal: AbortSignal): Promise<MindHistory> => {
    const h = await request<MindWireHistory>(threadPath(id), { signal });
    const status = mindStatus(await request<MindWireStatus>(`/api/mind/status?workspace_id=${encodeURIComponent(h.thread.billing_workspace_id)}`, { signal }));
    return { thread: mindThread(h.thread), messages: h.messages.filter((m) => m.role === "user" || m.role === "assistant").map(mindMessage), status };
  };
  return {
    list: async (signal) => (await request<MindWireThread[]>("/api/mind/threads", { signal })).map(mindThread),
    create: async (billing_workspace_id, signal) => {
      const t = await request<MindWireThread>("/api/mind/threads", { method: "POST", json: { billing_workspace_id }, signal });
      return load(t.id, signal);
    },
    history: load,
    send: async (thread, body, signal, receive) => {
      let usage: MindWireUsage | undefined;
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
          case "tool_started": receive({ type: "tool_start", name: data.summary || data.name }); break;
          case "tool_finished": receive({ type: "tool_result", name: data.summary || data.name }); break;
          case "action_card": receive({ type: "action_card", card: mindCard(data.card) }); break;
          case "usage": usage = data as unknown as MindWireUsage; break;
          case "error": receive({ type: "error", message: data.message, reason: mindBudget(data.code) === "ok" ? data.code : mindBudget(data.code) }); break;
          case "done": terminal = data.status; break;
        }
      }).catch((error) => { if (signal.aborted || terminal !== "failed") throw error; failure = error; });
      if (failure) {
        // The server finished and saved the failed turn: show what was saved (any partial reply) and its cost.
        const saved = await load(thread, signal);
        const last = [...saved.messages].reverse().find((m) => m.role === "assistant");
        if (last) receive({ type: "done", message: last, thread: saved.thread, status: usage ? mindUsage(saved.status, usage) : saved.status });
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
