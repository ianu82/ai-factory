import assert from "node:assert/strict";
import { test } from "node:test";
import { policyPatch, type LearningPolicy } from "./learningPolicy.ts";

test("pending policy cannot produce an autonomy write", () => {
  assert.equal(policyPatch(undefined, "release", "auto"), undefined);
});
test("release edit writes only release, never the approval-only fallback", () => {
  const policy = { before: { plan: "approval", merge: "approval", release: "approval" }, after: { plan: "auto", merge: "auto", release: "approval" } } as LearningPolicy;
  assert.deepEqual(policyPatch(policy, "release", "auto"), { autonomy: { release: "auto" } });
  assert.equal(policy.after.plan, "auto");
  assert.equal(policy.after.merge, "auto");
});
