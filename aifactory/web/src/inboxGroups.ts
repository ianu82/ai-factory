/** How the Inbox shows open decisions: overdue ones first, the same question about different work as one group
 * (`question`, see api.py same_question), and how long each has waited. */
import type { Decision } from "./api";

export interface DecisionGroup { key: string; rows: Decision[] }
export interface GroupResult { id: string; status: "resolved" | "already_resolved" | "refused"; reason?: string }

/** Overdue decisions first; then by priority, oldest first, or newest first. */
export function sortDecisions(decisions: Decision[], order: "priority" | "newest"): Decision[] {
  return [...decisions].sort((a, b) => Number(!!b.overdue) - Number(!!a.overdue) || (order === "newest"
    ? b.created_at.localeCompare(a.created_at)
    : (a.priority ?? 2) - (b.priority ?? 2) || a.created_at.localeCompare(b.created_at)));
}

/** The decisions in the order given, each question once: a group sits where its first member was. A decision that
 * is never grouped (no `question`), or alone with its question, is a group of one. */
export function groupDecisions(decisions: Decision[]): DecisionGroup[] {
  const groups: DecisionGroup[] = [];
  const byQuestion = new Map<string, DecisionGroup>();
  for (const d of decisions) {
    const shared = d.question ? byQuestion.get(d.question) : undefined;
    if (shared) {
      shared.rows.push(d);
      continue;
    }
    const group: DecisionGroup = { key: d.id, rows: [d] };
    groups.push(group);
    if (d.question) byQuestion.set(d.question, group);
  }
  return groups;
}

const TASK_KEYS = /\bT\d+(?:,\s*T\d+)*\b/g;

/** The title the members share: theirs when they read the same, else with the task keys that differ left out. */
export function sharedTitle(rows: Decision[]): string {
  const first = rows[0]?.title || "";
  return rows.every((d) => d.title === first) ? first : first.replace(TASK_KEYS, "…");
}

const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

/** "Waiting 4 hours": minutes under an hour, hours under two days, else days (as the watch says it). */
export function waiting(minutes: number): string {
  const m = Math.max(Math.floor(minutes), 0);
  if (m < 60) return `Waiting ${plural(m, "minute")}`;
  if (m < 48 * 60) return `Waiting ${plural(Math.floor(m / 60), "hour")}`;
  return `Waiting ${Math.floor(m / 1440)} days`;
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** The line a decision's card shows when the Supervisor found its task stuck. */
export function stuckLine(stuck: { since?: string | null } | undefined): string {
  if (!stuck) return "";
  const at = stuck.since ? new Date(stuck.since) : null;
  if (!at || Number.isNaN(at.getTime())) return "The Supervisor flagged it as stuck";
  const two = (n: number) => String(n).padStart(2, "0");
  const day = `${two(at.getUTCDate())} ${MONTHS[at.getUTCMonth()]}`;
  const when = `${day} ${two(at.getUTCHours())}:${two(at.getUTCMinutes())} UTC`;
  return `Waiting since ${when}; the Supervisor flagged it as stuck`;
}

/** What answering a group did, member by member, in one line; `failed` when any was refused. */
export function groupReport(results: GroupResult[]): { text: string; failed: boolean } {
  const done = results.filter((r) => r.status === "resolved").length;
  const already = results.filter((r) => r.status === "already_resolved").length;
  const refused = results.filter((r) => r.status === "refused");
  const parts = [`${done} answered`];
  if (already) parts.push(`${already} already answered`);
  if (refused.length) {
    const reasons = Array.from(new Set(refused.map((r) => r.reason || "refused")));
    parts.push(`${refused.length} refused: ${reasons.join("; ")}`);
  }
  return { text: parts.join(", "), failed: refused.length > 0 };
}
