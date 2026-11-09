import { test } from "node:test";
import assert from "node:assert/strict";
import { signalAcceptanceReason } from "./signalAcceptance.ts";

const reason = "Acceptance needed: the issue author does not have verified write or admin access.";

test("held proposals explain why acceptance is needed", () => {
  assert.equal(signalAcceptanceReason({ status: "new", data: { acceptance_reason: reason } }), reason);
  assert.equal(signalAcceptanceReason({ status: "new", data: {} }), undefined);
});

for (const status of ["accepted", "ignored", "resolved"]) {
  test(`${status} signals hide stale acceptance prompts`, () => {
    assert.equal(signalAcceptanceReason({ status, data: { acceptance_reason: reason, worker: "codex" } }), undefined);
  });
}
