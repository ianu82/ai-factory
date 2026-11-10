/** The Outcomes board: which column each outcome belongs in, and the order within a column. */

export type BoardColumn = "planning" | "building" | "needs" | "done";

export const BOARD_COLUMNS: { key: BoardColumn; label: string; hint: string }[] = [
  { key: "planning", label: "Planning", hint: "The planner is reading the code and drafting tasks" },
  { key: "building", label: "Building", hint: "Tasks are being built, verified and merged" },
  { key: "needs", label: "Needs you", hint: "Waiting on a decision, an approval or a stuck task" },
  { key: "done", label: "Delivered", hint: "Completed, or completed in part" },
];

type Card = { status: string; open_decisions: number; priority: number; updated_at: string; completed_at?: string | null };

const NEEDS = ["needs_input", "awaiting_approval", "attention", "blocked"];
const DONE = ["completed", "partially_completed"];

/** Cancelled outcomes are left off the board. Anything waiting on a person goes to "Needs you", even mid-build. */
export function boardColumn(o: Pick<Card, "status" | "open_decisions">): BoardColumn | null {
  if (o.status === "cancelled") return null;
  if (DONE.includes(o.status)) return "done";
  if (o.open_decisions > 0 || NEEDS.includes(o.status)) return "needs";
  if (o.status === "planning") return "planning";
  return "building";
}

/** Group outcomes into the board's columns: open decisions, then priority, then most recent activity; delivered
 * outcomes newest first. */
export function boardColumns<T extends Card>(outcomes: T[]): Record<BoardColumn, T[]> {
  const columns: Record<BoardColumn, T[]> = { planning: [], building: [], needs: [], done: [] };
  for (const o of outcomes) {
    const column = boardColumn(o);
    if (column) columns[column].push(o);
  }
  const active = (a: T, b: T) => (b.open_decisions > 0 ? 1 : 0) - (a.open_decisions > 0 ? 1 : 0)
    || a.priority - b.priority || b.updated_at.localeCompare(a.updated_at);
  for (const key of ["planning", "building", "needs"] as const) columns[key].sort(active);
  columns.done.sort((a, b) => (b.completed_at || b.updated_at).localeCompare(a.completed_at || a.updated_at));
  return columns;
}
