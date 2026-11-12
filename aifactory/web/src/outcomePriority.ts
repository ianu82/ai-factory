/** Priority values shared by the detail editor and outcome list. */
export const OUTCOME_PRIORITIES = [
  { value: 0, label: "Urgent" },
  { value: 1, label: "High" },
  { value: 2, label: "Normal" },
  { value: 3, label: "Low" },
] as const;

export function priorityLabel(priority: number): string {
  return OUTCOME_PRIORITIES.find((p) => p.value === priority)?.label ?? "Unknown priority";
}

export type OutcomeSort = "default" | "priority";
type SortableOutcome = { id: string; priority: number; open_decisions: number; updated_at: string };

/** Default keeps needs-you first; explicit priority never lets decisions override urgency.
 * Both modes break priority ties by most recent activity, then ascending id. */
export function compareOutcomes(a: SortableOutcome, b: SortableOutcome, sort: OutcomeSort = "default"): number {
  const needsYou = sort === "default" ? Number(b.open_decisions > 0) - Number(a.open_decisions > 0) : 0;
  return needsYou || a.priority - b.priority || b.updated_at.localeCompare(a.updated_at) || a.id.localeCompare(b.id);
}
