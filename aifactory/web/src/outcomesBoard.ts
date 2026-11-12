/** The Outcomes board: which column each outcome belongs in, and the order within a column. */

export type BoardColumn = "planning" | "queued" | "building" | "qa" | "needs" | "done";

export const BOARD_COLUMNS: { key: BoardColumn; label: string; hint: string }[] = [
  { key: "planning", label: "Planning", hint: "The planner is reading the code and drafting tasks" },
  { key: "queued", label: "Queued", hint: "Planned; tasks start as delivery slots free up (Settings → Throughput)" },
  { key: "building", label: "Building", hint: "Tasks are being built, reworked or merged" },
  { key: "qa", label: "In QA", hint: "Every active task is being tested: the configured checks, the independent verifier or CI" },
  { key: "needs", label: "Needs you", hint: "Waiting on a decision, an approval or a stuck task" },
  { key: "done", label: "Delivered", hint: "Completed, or completed in part" },
];

type Progress = { total: number; started?: number };
type Track = { status: string; stage: string }[];
type Card = { status: string; open_decisions: number; priority: number; updated_at: string; completed_at?: string | null; progress?: Progress; track?: Track };

/** The QA stages of a task: the candidate's configured checks, independent verification (and publishing the
 * verified change), and waiting for CI. Building, rework, CI repair, merging and smoke checks are delivery. */
const QA_STAGES = ["checks", "verify", "publish", "ci", "review"];
const FINISHED = ["completed", "abandoned", "cancelled", "handed_over", "reverted"];

/** Tasks moving through delivery right now: started (they have a stage) and not finished or waiting to start. */
function moving(track: Track | undefined): Track {
  return (track || []).filter((t) => t.stage && t.stage !== "done" && t.status !== "pending" && !FINISHED.includes(t.status));
}

/** How many of an outcome's tasks are in QA. */
export function qaCount(o: { track?: Track }): number {
  return moving(o.track).filter((t) => QA_STAGES.includes(t.stage)).length;
}

/** Every task that is moving is in QA: nothing is being built or reworked, the outcome waits on its tests. */
export function inQA(o: { track?: Track }): boolean {
  const active = moving(o.track);
  return active.length > 0 && active.every((t) => QA_STAGES.includes(t.stage));
}

const NEEDS = ["needs_input", "awaiting_approval", "attention", "blocked"];
const DONE = ["completed", "partially_completed"];

/** Planned, but no task has left the queue yet: every task waits for a free delivery slot or for its dependencies.
 * A missing `started` count is not evidence of waiting, so it never reads as queued. */
export function isQueued(o: { status: string; progress?: Progress }): boolean {
  return o.status === "running" && !!o.progress && o.progress.total > 0 && o.progress.started === 0;
}

/** Cancelled outcomes are left off the board. Anything waiting on a person goes to "Needs you", even mid-build. */
export function boardColumn(o: Pick<Card, "status" | "open_decisions" | "progress" | "track">): BoardColumn | null {
  if (o.status === "cancelled") return null;
  if (DONE.includes(o.status)) return "done";
  if (o.open_decisions > 0 || NEEDS.includes(o.status)) return "needs";
  if (o.status === "planning") return "planning";
  if (isQueued(o)) return "queued";
  return inQA(o) ? "qa" : "building";
}

/** Group outcomes into the board's columns: open decisions, then priority, then most recent activity; delivered
 * outcomes newest first. */
export function boardColumns<T extends Card>(outcomes: T[]): Record<BoardColumn, T[]> {
  const columns: Record<BoardColumn, T[]> = { planning: [], queued: [], building: [], qa: [], needs: [], done: [] };
  for (const o of outcomes) {
    const column = boardColumn(o);
    if (column) columns[column].push(o);
  }
  const active = (a: T, b: T) => (b.open_decisions > 0 ? 1 : 0) - (a.open_decisions > 0 ? 1 : 0)
    || a.priority - b.priority || b.updated_at.localeCompare(a.updated_at);
  for (const key of ["planning", "queued", "building", "qa", "needs"] as const) columns[key].sort(active);
  columns.done.sort((a, b) => (b.completed_at || b.updated_at).localeCompare(a.completed_at || a.updated_at));
  return columns;
}
