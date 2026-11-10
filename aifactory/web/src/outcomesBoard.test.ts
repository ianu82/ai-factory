import { test } from "node:test";
import assert from "node:assert/strict";
import { BOARD_COLUMNS, boardColumn, boardColumns, isQueued } from "./outcomesBoard.ts";

type Progress = { done: number; total: number; started?: number };
const outcome = (number: number, status: string, extra: Partial<{ open_decisions: number; priority: number; updated_at: string; completed_at: string | null; progress: Progress }> = {}) =>
  ({ number, status, open_decisions: 0, priority: 2, updated_at: "2026-11-10T10:00:00Z", completed_at: null, ...extra });

test("each outcome status has one column, and cancelled outcomes are left off", () => {
  assert.equal(boardColumn(outcome(1, "planning")), "planning");
  assert.equal(boardColumn(outcome(2, "running")), "building");
  for (const status of ["needs_input", "awaiting_approval", "attention", "blocked"]) assert.equal(boardColumn(outcome(3, status)), "needs");
  assert.equal(boardColumn(outcome(4, "completed")), "done");
  assert.equal(boardColumn(outcome(5, "partially_completed")), "done");
  assert.equal(boardColumn(outcome(6, "cancelled")), null);
  assert.deepEqual(BOARD_COLUMNS.map((c) => c.key), ["planning", "queued", "building", "needs", "done"]);
});

test("an open decision moves an outcome to Needs you, even while it builds or plans", () => {
  assert.equal(boardColumn(outcome(1, "running", { open_decisions: 2 })), "needs");
  assert.equal(boardColumn(outcome(2, "planning", { open_decisions: 1 })), "needs");
  assert.equal(boardColumn(outcome(3, "completed", { open_decisions: 1 })), "done");
});

test("a running outcome whose tasks have not left the queue is Queued, not Building", () => {
  const waiting = { done: 0, total: 4, started: 0 };
  assert.equal(boardColumn(outcome(1, "running", { progress: waiting })), "queued");
  assert.equal(boardColumn(outcome(2, "running", { progress: { done: 0, total: 4, started: 1 } })), "building");
  // No tasks yet, or no started count from the API, is not evidence of waiting for a slot.
  assert.equal(boardColumn(outcome(3, "running", { progress: { done: 0, total: 0, started: 0 } })), "building");
  assert.equal(boardColumn(outcome(4, "running", { progress: { done: 0, total: 4 } })), "building");
  assert.equal(boardColumn(outcome(5, "running")), "building");
  // Only an approved plan queues: planning stays Planning, and an open decision still wins.
  assert.equal(boardColumn(outcome(6, "planning", { progress: waiting })), "planning");
  assert.equal(boardColumn(outcome(7, "running", { progress: waiting, open_decisions: 1 })), "needs");
  assert.equal(isQueued(outcome(8, "running", { progress: waiting })), true);
  assert.equal(isQueued(outcome(9, "running", { progress: waiting, open_decisions: 1 })), true);
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
    outcome(9, "running", { priority: 2, progress: { done: 0, total: 3, started: 0 }, updated_at: "2026-11-10T07:00:00Z" }),
    outcome(10, "running", { priority: 1, progress: { done: 0, total: 2, started: 0 }, updated_at: "2026-11-10T06:00:00Z" }),
  ]);
  assert.deepEqual(columns.building.map((o) => o.number), [2, 3, 1]);
  assert.deepEqual(columns.queued.map((o) => o.number), [10, 9]);
  assert.deepEqual(columns.needs.map((o) => o.number), [4, 5]);
  assert.deepEqual(columns.done.map((o) => o.number), [7, 6]);
  assert.deepEqual(columns.planning, []);
  assert.equal(Object.values(columns).flat().length, 9);
});
