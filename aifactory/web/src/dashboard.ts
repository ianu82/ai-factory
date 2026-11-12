// The Dashboard's series (GET /api/workspaces/:id/dashboard, see aifactory/dashboard.py) and the arithmetic the page
// draws with. No imports: node's test runner loads this file directly.

export type Period = "day" | "week" | "month";
export const PERIODS: { value: Period; label: string }[] = [
  { value: "day", label: "Daily" }, { value: "week", label: "Weekly" }, { value: "month", label: "Monthly" },
];

export const STAGES = [
  { key: "plan", label: "Plan" }, { key: "build", label: "Build" }, { key: "verify", label: "Verify" },
  { key: "review", label: "Review & CI" }, { key: "merge_queue", label: "Merge queue" }, { key: "release", label: "Release" },
] as const;
export type Stage = (typeof STAGES)[number]["key"];

type Median = { median_hours: number | null; sample_count: number };

export type Figures = {
  days: number;
  tasks_delivered: number;
  outcomes_completed: number;
  prs_opened: number;
  prs_opened_merged: number;
  merges: number;
  runs: { total: number; by_role: Record<string, number> };
  cost: { usd: number; known: boolean };
  cost_per_delivered_task: { usd: number | null; known: boolean };
  outcome_cost: { median_usd: number | null; known: boolean; sample_count: number };
  autonomy: { unattended: number; director: number; assisted: number; unverified: number };
  cycle_time: Median & { unknown_count: number };
  stages: Record<Stage, Median>;
  lead_time: Median;
  first_attempt: { merged: number; passed: number };
  change_failure: { settled: number; failed: number; pending: number };
  decisions: { asked: number; person: number; director: number; lead: number; operator: number; factory: number };
  guidance: { person: number; director: number; lead: number };
};

export type Bucket = Figures & { start: string; end: string; partial: boolean };

export type Series = {
  period: Period;
  generated_at: string;
  definitions: Record<string, string>;
  buckets: Bucket[];
  range: Figures & { start: string; end: string };
  undated: Record<string, number>;
  expensive_outcomes: { id: string; number: number; title: string; cost_usd: number; cost_known: boolean;
    completed_at: string; tasks_delivered: number }[];
};

export function asPeriod(value: string | null | undefined): Period {
  return value === "day" || value === "month" ? value : "week";
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** A bucket's name, in UTC like the buckets themselves: "Nov 9" (a day, or the week starting then), "Nov 2026". */
export function bucketLabel(iso: string, period: Period, long = false): string {
  const at = new Date(iso);
  const month = MONTHS[at.getUTCMonth()];
  if (period === "month") return long ? `${month} ${at.getUTCFullYear()}` : month;
  const day = `${month} ${at.getUTCDate()}`;
  return period === "week" && long ? `Week of ${day}` : day;
}

/** The bucket a figure is "now": the current one, partial, said so by the caller. */
export function current(buckets: Bucket[]): { now?: Bucket; before?: Bucket } {
  return { now: buckets[buckets.length - 1], before: buckets[buckets.length - 2] };
}

/** Hours as people say them: 45m, 18h, 2d 5h. */
export function hours(h: number | null | undefined): string {
  if (h === null || h === undefined || !Number.isFinite(h)) return "—";
  if (h < 1) return `${Math.max(1, Math.round(h * 60))}m`;
  if (h < 48) return `${Math.round(h)}h`;
  const d = Math.floor(h / 24);
  const rest = Math.round(h - d * 24);
  return rest === 24 ? `${d + 1}d` : rest ? `${d}d ${rest}h` : `${d}d`;
}

export function ratio(n: number, d: number): number | null {
  return d > 0 ? n / d : null;
}

export function percent(value: number | null): string {
  return value === null ? "—" : `${Math.round(value * 100)}%`;
}

/** Merges per day over a bucket; a partial bucket counts the days it has had, and never less than one. */
export function perDay(merges: number, days: number): number {
  return merges / Math.max(1, days);
}

/** No person took part: unattended outcomes, with or without the Director standing in, over those the evidence decides. */
export function noPerson(a: Figures["autonomy"]): { share: number | null; alone: number | null } {
  const known = a.unattended + a.director + a.assisted;
  return { share: ratio(a.unattended + a.director, known), alone: ratio(a.unattended, known) };
}

export type Direction = "up" | "down";

/** The change from the previous bucket, toned by whether that direction is good for this measure. */
export function change(now: number | null, before: number | null, better: Direction,
                       format: (v: number) => string): { text: string; tone: "good" | "bad" | "flat" } | null {
  if (now === null || before === null) return null;
  const diff = now - before;
  if (Math.abs(diff) < 1e-9) return { text: "no change", tone: "flat" };
  const up = diff > 0;
  return { text: `${up ? "+" : "−"}${format(Math.abs(diff))}`, tone: up === (better === "up") ? "good" : "bad" };
}

/** A clean axis top: the next 1, 2 or 5 × 10ⁿ at or above the largest value. */
export function niceMax(value: number): number {
  if (!(value > 0)) return 1;
  const step = 10 ** Math.floor(Math.log10(value));
  for (const m of [1, 2, 5, 10]) if (m * step >= value - 1e-9) return m * step;
  return 10 * step;
}

/** Points for a sparkline in a w × h box; a missing value breaks the line rather than reading as zero. */
export function sparkline(values: (number | null)[], w: number, h: number, pad = 3): string {
  const known = values.filter((v): v is number => v !== null && Number.isFinite(v));
  if (!known.length) return "";
  const lo = Math.min(...known), hi = Math.max(...known);
  const x = (i: number) => (values.length === 1 ? w / 2 : pad + (i * (w - 2 * pad)) / (values.length - 1));
  const y = (v: number) => (hi === lo ? h / 2 : pad + (1 - (v - lo) / (hi - lo)) * (h - 2 * pad));
  let path = "", open = false;
  values.forEach((v, i) => {
    if (v === null || !Number.isFinite(v)) { open = false; return; }
    path += `${open ? "L" : "M"}${x(i).toFixed(1)} ${y(v).toFixed(1)}`;
    open = true;
  });
  return path;
}

/** Each stage's share of the phase bar, by its median; stages without a median take no room. */
export function phaseShares(stages: Figures["stages"]): { key: Stage; label: string; hours: number | null; share: number }[] {
  const total = STAGES.reduce((sum, s) => sum + (stages[s.key]?.median_hours ?? 0), 0);
  return STAGES.map((s) => {
    const h = stages[s.key]?.median_hours ?? null;
    return { key: s.key, label: s.label, hours: h, share: total > 0 && h ? h / total : 0 };
  });
}

/** Which x labels to print so they never collide: about `room` of them, always the first and the last. */
export function tickIndexes(count: number, room: number): number[] {
  if (count <= room) return Array.from({ length: count }, (_, i) => i);
  const every = Math.ceil((count - 1) / Math.max(1, room - 1));
  const out: number[] = [];
  for (let i = 0; i < count - 1; i += every) out.push(i);
  if (count - 1 - out[out.length - 1] < every / 2) out.pop();
  out.push(count - 1);
  return out;
}

/** Roles by runs in the range, largest first. */
export function roles(byRole: Record<string, number>): string[] {
  return Object.keys(byRole).sort((a, b) => byRole[b] - byRole[a] || a.localeCompare(b));
}
