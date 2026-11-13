import { test } from "node:test";
import assert from "node:assert/strict";
import { supervisorConfidenceLabel, supervisorFindingLabel } from "./supervisor.ts";

test("Supervisor finding labels cover detector kinds and future extensions", () => {
  assert.equal(supervisorFindingLabel("stuck_task"), "Stuck task");
  assert.equal(supervisorFindingLabel("push_protection"), "GitHub push protection");
  assert.equal(supervisorFindingLabel("main_red"), "Main CI failing");
  assert.equal(supervisorFindingLabel("combined_break"), "Combined merge breakage");
  assert.equal(supervisorFindingLabel("unaccepted_issue"), "Unaccepted issue");
  assert.equal(supervisorFindingLabel("untriaged_smoke"), "Untriaged smoke failure");
  assert.equal(supervisorFindingLabel("unrelayed_review"), "Review findings awaiting relay");
  assert.equal(supervisorFindingLabel("new_detector"), "new detector");
  assert.equal(supervisorFindingLabel(""), "Unknown finding");
});

test("inferred findings are suspected, never presented as observations", () => {
  assert.equal(supervisorConfidenceLabel("inferred"), "suspected");
  assert.equal(supervisorConfidenceLabel("observed"), "observed");
  for (const value of [null, undefined, "future"]) {
    assert.equal(supervisorConfidenceLabel(value), "confidence unknown");
  }
});
