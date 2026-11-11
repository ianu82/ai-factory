/** The Message Log: filters, wording and day grouping for messages between agents and people (see messages.py). */
import type { AgentMessage } from "./api";

/** Parties people filter by; each matches a message it sent or received. Keys are the API's `party` values. */
export const PARTY_FILTERS: { key: string; label: string }[] = [
  { key: "", label: "Everyone" },
  { key: "minds", label: "Minds" },
  { key: "lead", label: "Lead" },
  { key: "people", label: "People" },
  { key: "planning", label: "Planning" },
  { key: "builders", label: "Builders" },
  { key: "verifier", label: "Verifier" },
  { key: "ci", label: "CI" },
  { key: "factory", label: "Factory" },
];

export const INTENT_FILTERS: { key: string; label: string }[] = [
  { key: "", label: "All" },
  { key: "request", label: "Requests" },
  { key: "propose", label: "Proposals" },
  { key: "answer", label: "Answers" },
  { key: "inform", label: "Updates" },
  { key: "handoff", label: "Hand-offs" },
];

/** How a row reads: "<from> asks <to>", so the log reads as sentences. */
const VERBS: Record<AgentMessage["intent"], string> = {
  request: "asks", answer: "answers", propose: "proposes to", inform: "tells", handoff: "hands off to",
};
export function verb(intent: AgentMessage["intent"]): string {
  return VERBS[intent] || intent;
}

export function messagesUrl(workspaceId: string, filters: { party?: string; intent?: string; outcome?: string }, before?: number): string {
  const params = new URLSearchParams();
  if (filters.party) params.set("party", filters.party);
  if (filters.intent) params.set("intent", filters.intent);
  if (filters.outcome) params.set("outcome_id", filters.outcome);
  if (before) params.set("before", String(before));
  const query = params.toString();
  return `/api/workspaces/${workspaceId}/messages${query ? `?${query}` : ""}`;
}

function startOfDay(date: Date): number {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate()).getTime();
}

/** "Today", "Yesterday", or a short date (with the year when it is not this year), in the reader's time zone. */
export function dayLabel(iso: string, now = new Date()): string {
  const date = new Date(iso);
  const day = startOfDay(date);
  if (day === startOfDay(now)) return "Today";
  // The previous calendar date, not 24 hours back: a day with a clock change is 23 or 25 hours long.
  if (day === startOfDay(new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1))) return "Yesterday";
  return date.toLocaleDateString(undefined, { weekday: "short", day: "numeric", month: "short",
    ...(date.getFullYear() === now.getFullYear() ? {} : { year: "numeric" }) });
}

/** The reader's local calendar date, as the identity of a day group. */
function dayKey(iso: string): string {
  const date = new Date(iso);
  return `${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()}`;
}

/** Messages (newest first) grouped under their calendar day, keeping order. */
export function byDay(messages: AgentMessage[], now = new Date()):
  { key: string; day: string; messages: AgentMessage[] }[] {
  const groups: { key: string; day: string; messages: AgentMessage[] }[] = [];
  for (const message of messages) {
    const key = dayKey(message.at);
    const last = groups[groups.length - 1];
    if (last && last.key === key) last.messages.push(message);
    else groups.push({ key, day: dayLabel(message.at, now), messages: [message] });
  }
  return groups;
}
