export function ago(iso: string | null | undefined): string {
  if (!iso) return "";
  const t = new Date(iso).getTime();
  const s = Math.max(0, (Date.now() - t) / 1000);
  if (s < 45) return "just now";
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${Math.round(s / 3600)}h ago`;
  if (s < 86400 * 7) return `${Math.round(s / 86400)}d ago`;
  return new Date(iso).toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

export function clock(iso: string | null | undefined): string {
  if (!iso) return "";
  return new Date(iso).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
}

export function money(usd: number, known = true): string {
  if (!known && !usd) return "unknown";
  const value = usd < 0.01 && usd > 0 ? "<$0.01" : `$${usd.toFixed(usd >= 100 ? 0 : 2)}`;
  return known ? value : `≥${value}`;
}

export function tokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1000) return `${Math.round(n / 1000)}k`;
  return String(n);
}

export function duration(start: string | null, end: string | null): string {
  if (!start) return "";
  const ms = (end ? new Date(end).getTime() : Date.now()) - new Date(start).getTime();
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

export const OUTCOME_STATUS: Record<string, { label: string; tone: Tone }> = {
  planning: { label: "Planning", tone: "active" },
  awaiting_approval: { label: "Plan needs approval", tone: "human" },
  needs_input: { label: "Needs input", tone: "human" },
  running: { label: "In progress", tone: "active" },
  attention: { label: "Needs attention", tone: "human" },
  blocked: { label: "Blocked", tone: "bad" },
  completed: { label: "Delivered", tone: "good" },
  partially_completed: { label: "Partly delivered", tone: "warn" },
  cancelled: { label: "Cancelled", tone: "idle" },
};

export const TASK_STATUS: Record<string, { label: string; tone: Tone }> = {
  pending: { label: "Waiting on dependencies", tone: "idle" },
  queued: { label: "Queued", tone: "idle" },
  running: { label: "Running", tone: "active" },
  waiting_ci: { label: "Waiting for CI", tone: "idle" },
  needs_input: { label: "Needs input", tone: "human" },
  awaiting_approval: { label: "Merge needs approval", tone: "human" },
  failed: { label: "Did not pass", tone: "bad" },
  blocked: { label: "Blocked", tone: "bad" },
  smoke_failed: { label: "Merged · smoke failed", tone: "bad" },
  reverted: { label: "Reverted after smoke failed", tone: "warn" },
  handed_over: { label: "Handed to a person", tone: "idle" },
  completed: { label: "Delivered", tone: "good" },
  abandoned: { label: "Abandoned", tone: "idle" },
  cancelled: { label: "Cancelled", tone: "idle" },
};

export type Tone = "good" | "bad" | "warn" | "human" | "active" | "idle";

export const HARNESS_LABEL: Record<string, string> = { native: "Native", codex: "Codex", anton: "Anton" };

export const DECISION_KIND: Record<string, string> = {
  approve_plan: "Approve plan",
  approve_merge: "Approve merge",
  question: "Decide",
  unblock: "Unblock",
  budget: "Budget",
  provider_hold: "Provider hold",
};
