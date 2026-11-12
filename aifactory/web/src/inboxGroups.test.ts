import assert from "node:assert/strict";
import { test } from "node:test";
import type { Decision } from "./api.ts";
import { groupDecisions, groupReport, sharedTitle, sortDecisions, stuckLine, waiting } from "./inboxGroups.ts";

const STRANDED = JSON.stringify(["unblock", ["continue", "drop"], "T# cannot start: T# ended without being delivered"]);

function decision(id: string, fields: Partial<Decision> = {}): Decision {
  return {
    id, kind: "unblock", title: id, body: "", options: [], status: "open", resolution: "", comment: "",
    resolved_by: null, resolved_at: null, reusable: false, scope: "item", outcome_id: null, task_id: null,
    created_at: "2026-11-12T08:00:00Z", judge: null, ...fields,
  };
}

test("the same question about different work is one group, where its first member was", () => {
  const rows = [
    decision("a", { question: STRANDED, title: "T2, T3 cannot start: T1 ended without being delivered" }),
    decision("b", { kind: "question", question: null }),
    decision("c", { question: STRANDED, title: "T5 cannot start: T4 ended without being delivered" }),
    decision("d", { question: "other" }),
  ];
  const groups = groupDecisions(rows);
  assert.deepEqual(groups.map((g) => g.rows.map((d) => d.id)), [["a", "c"], ["b"], ["d"]]);
  assert.equal(groups[0].key, "a");
  assert.equal(sharedTitle(groups[0].rows), "… cannot start: … ended without being delivered");
  assert.equal(sharedTitle([rows[0], { ...rows[0], id: "x" }]), rows[0].title);
});

test("decisions that are never grouped stay apart even with the same title", () => {
  const groups = groupDecisions([decision("a", { title: "Which queue?" }), decision("b", { title: "Which queue?" })]);
  assert.deepEqual(groups.map((g) => g.rows.length), [1, 1]);
});

test("overdue decisions sort first, in either order", () => {
  const rows = [
    decision("old", { created_at: "2026-11-12T01:00:00Z", priority: 2 }),
    decision("late", { created_at: "2026-11-12T03:00:00Z", priority: 3, overdue: { alert_id: "x", title: "late" } }),
    decision("new", { created_at: "2026-11-12T05:00:00Z", priority: 2 }),
    decision("urgent", { created_at: "2026-11-12T04:00:00Z", priority: 0 }),
  ];
  assert.deepEqual(sortDecisions(rows, "priority").map((d) => d.id), ["late", "urgent", "old", "new"]);
  assert.deepEqual(sortDecisions(rows, "newest").map((d) => d.id), ["late", "new", "urgent", "old"]);
});

test("how long a decision has waited reads in minutes, hours or days", () => {
  assert.equal(waiting(1), "Waiting 1 minute");
  assert.equal(waiting(59), "Waiting 59 minutes");
  assert.equal(waiting(60), "Waiting 1 hour");
  assert.equal(waiting(4 * 60 + 30), "Waiting 4 hours");
  assert.equal(waiting(3 * 1440), "Waiting 3 days");
});

test("a stuck finding is one line on the decision it is about", () => {
  assert.equal(stuckLine({ since: "2026-11-12T09:05:00+00:00" }),
    "Waiting since 12 Nov 09:05 UTC; the Supervisor flagged it as stuck");
  assert.equal(stuckLine({ since: null }), "The Supervisor flagged it as stuck");
  assert.equal(stuckLine(undefined), "");
});

test("answering a group reports each member's result in one line", () => {
  assert.deepEqual(groupReport([{ id: "a", status: "resolved" }, { id: "b", status: "resolved" }]),
    { text: "2 answered", failed: false });
  assert.deepEqual(groupReport([
    { id: "a", status: "resolved" }, { id: "b", status: "already_resolved" },
    { id: "c", status: "refused", reason: "Only a person answers this decision" },
  ]), { text: "1 answered, 1 already answered, 1 refused: Only a person answers this decision", failed: true });
});
