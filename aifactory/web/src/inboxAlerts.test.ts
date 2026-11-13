import assert from "node:assert/strict";
import { test } from "node:test";
import { withoutAlerts } from "./inboxAlerts.ts";

test("removes the matching alert and keeps other alerts and decisions", () => {
  const data = { alerts: [{ id: "a", title: "Waiting" }, { id: "b", title: "Stuck" }], decisions: [{ id: "d" }] };
  const result = withoutAlerts(data, new Set(["a"]));
  assert.deepEqual(result, { alerts: [data.alerts[1]], decisions: data.decisions });
  assert.notEqual(result, data);
  assert.equal(result?.alerts[0], data.alerts[1]);
  assert.equal(result?.decisions, data.decisions);
  assert.equal(data.alerts.length, 2);
});

test("non-matching ids and empty sets preserve the object reference", () => {
  const data = { alerts: [{ id: "a" }] };
  assert.equal(withoutAlerts(data, new Set(["missing"])), data);
  assert.equal(withoutAlerts(data, new Set()), data);
});

test("undefined data, missing alerts and empty alerts are unchanged", () => {
  assert.equal(withoutAlerts(undefined, new Set(["a"])), undefined);
  const missing: { alerts?: { id: string }[]; decisions: unknown[] } = { decisions: [] };
  assert.equal(withoutAlerts(missing, new Set(["a"])), missing);
  const empty = { alerts: [] };
  assert.equal(withoutAlerts(empty, new Set(["a"])), empty);
});

test("removes several ids while preserving the remaining alert order", () => {
  const data = { alerts: [{ id: "a" }, { id: "b" }, { id: "c" }, { id: "d" }] };
  assert.deepEqual(withoutAlerts(data, new Set(["a", "c", "missing"])), { alerts: [data.alerts[1], data.alerts[3]] });
  assert.deepEqual(withoutAlerts(data, new Set(["a", "b", "c", "d"])), { alerts: [] });
});
