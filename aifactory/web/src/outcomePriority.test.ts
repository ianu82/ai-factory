import { test } from "node:test";
import assert from "node:assert/strict";
import { compareOutcomes, OUTCOME_PRIORITIES, priorityLabel } from "./outcomePriority.ts";

const outcome = (id: string, priority: number, extra: Partial<{ open_decisions: number; updated_at: string }> = {}) =>
  ({ id, priority, open_decisions: 0, updated_at: "2026-11-10T10:00:00Z", ...extra });

test("all four numeric priorities have the same labels in the editor and list", () => {
  assert.deepEqual([0, 1, 2, 3].map(priorityLabel), ["Urgent", "High", "Normal", "Low"]);
  assert.deepEqual(OUTCOME_PRIORITIES.map((p) => [p.value, p.label]), [
    [0, "Urgent"], [1, "High"], [2, "Normal"], [3, "Low"],
  ]);
  assert.equal(priorityLabel(4), "Unknown priority");
});

test("explicit Priority sorts numerically, even when lower priorities have open decisions", () => {
  const rows = [
    outcome("low", 3, { open_decisions: 5 }),
    outcome("normal", 2, { open_decisions: 1 }),
    outcome("urgent", 0),
    outcome("high", 1),
  ];
  assert.deepEqual([...rows].sort((a, b) => compareOutcomes(a, b, "priority")).map((o) => o.id),
    ["urgent", "high", "normal", "low"]);
});

test("priority ties use newest updated_at, then ascending id regardless of decisions or input order", () => {
  const rows = [
    outcome("b", 1, { open_decisions: 4 }),
    outcome("old", 1, { updated_at: "2026-11-09T10:00:00Z", open_decisions: 1 }),
    outcome("a", 1),
  ];
  for (const input of [rows, [...rows].reverse()]) {
    assert.deepEqual([...input].sort((a, b) => compareOutcomes(a, b, "priority")).map((o) => o.id), ["a", "b", "old"]);
  }
  assert.equal(compareOutcomes(rows[0], rows[0], "priority"), 0);
});

test("default preserves needs-you first, then priority, then most recent activity", () => {
  const rows = [
    outcome("urgent", 0),
    outcome("low-decision", 3, { open_decisions: 1 }),
    outcome("normal-old", 2, { updated_at: "2026-11-09T10:00:00Z" }),
    outcome("normal-new", 2),
    outcome("high-decision", 1, { open_decisions: 1 }),
    outcome("normal-decision", 2, { open_decisions: 9 }),
  ];
  const expected = ["high-decision", "normal-decision", "low-decision", "urgent", "normal-new", "normal-old"];
  assert.deepEqual([...rows].sort(compareOutcomes).map((o) => o.id), expected);
  assert.deepEqual([...rows].sort((a, b) => compareOutcomes(a, b, "default")).map((o) => o.id), expected);
});

test("default resolves otherwise identical rows by id for a deterministic order", () => {
  const rows = [outcome("b", 2, { open_decisions: 1 }), outcome("a", 2, { open_decisions: 3 })];
  assert.deepEqual(rows.sort(compareOutcomes).map((o) => o.id), ["a", "b"]);
});
