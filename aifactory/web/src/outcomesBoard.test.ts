import { test } from "node:test";
import assert from "node:assert/strict";
import { BOARD_COLUMNS, boardColumn, boardColumns } from "./outcomesBoard.ts";

const outcome = (number: number, status: string, extra: Partial<{ open_decisions: number; priority: number; updated_at: string; completed_at: string | null }> = {}) =>
  ({ number, status, open_decisions: 0, priority: 2, updated_at: "2026-11-10T10:00:00Z", completed_at: null, ...extra });

test("each outcome status has one column, and cancelled outcomes are left off", () => {
  assert.equal(boardColumn(outcome(1, "planning")), "planning");
  assert.equal(boardColumn(outcome(2, "running")), "building");
  for (const status of ["needs_input", "awaiting_approval", "attention", "blocked"]) assert.equal(boardColumn(outcome(3, status)), "needs");
  assert.equal(boardColumn(outcome(4, "completed")), "done");
  assert.equal(boardColumn(outcome(5, "partially_completed")), "done");
  assert.equal(boardColumn(outcome(6, "cancelled")), null);
  assert.deepEqual(BOARD_COLUMNS.map((c) => c.key), ["planning", "building", "needs", "done"]);
});

test("an open decision moves an outcome to Needs you, even while it builds or plans", () => {
  assert.equal(boardColumn(outcome(1, "running", { open_decisions: 2 })), "needs");
  assert.equal(boardColumn(outcome(2, "planning", { open_decisions: 1 })), "needs");
  assert.equal(boardColumn(outcome(3, "completed", { open_decisions: 1 })), "done");
});

test("columns order by open decisions, priority and recent activity; delivered newest first", () => {
  const columns = boardColumns([
    outcome(1, "running", { priority: 2, updated_at: "2026-11-10T09:00:00Z" }),
    outcome(2, "running", { priority: 1, updated_at: "2026-11-10T08:00:00Z" }),
    outcome(3, "running", { priority: 2, updated_at: "2026-11-10T11:00:00Z" }),
    outcome(4, "attention", { open_decisions: 1, priority: 3 }),
    outcome(5, "needs_input", { open_decisions: 0, priority: 0 }),
    outcome(6, "completed", { completed_at: "2026-11-09T10:00:00Z" }),
    outcome(7, "completed", { completed_at: "2026-11-10T10:00:00Z" }),
    outcome(8, "cancelled"),
  ]);
  assert.deepEqual(columns.building.map((o) => o.number), [2, 3, 1]);
  assert.deepEqual(columns.needs.map((o) => o.number), [4, 5]);
  assert.deepEqual(columns.done.map((o) => o.number), [7, 6]);
  assert.deepEqual(columns.planning, []);
  assert.equal(Object.values(columns).flat().length, 7);
});
