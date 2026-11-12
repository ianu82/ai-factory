import assert from "node:assert/strict";
import { test } from "node:test";
import type { MergeFacts } from "./api.ts";
import { mergeFactRows, withoutFacts } from "./decisionFacts.ts";

const facts: MergeFacts = {
  pr: 7, head: "a".repeat(40),
  change: { files: 10, additions: 12, deletions: 4, paths: "abcdefgh".split("").map((n) => `${n}.py`) },
  verifier: { verdict: "pass", summary: "Greeting verified." },
  checks: { passed: 2, total: 3, preexisting: 1 },
  ci: { state: "success", checks: 1 },
  review: { state: "reviewed", reviewers: ["codex"], round: 2, open: 1, blocking: 1 },
  cost: { usd: 1.5, attempts: 1 },
  asked: { because: "workspace", text: "Asked because merges in this workspace wait for a person." },
  text: "Change: …\nAsked because …",
};

test("a merge approval's facts read as one row each: change, verifier, checks, review, cost", () => {
  assert.deepEqual(mergeFactRows(facts), [
    { label: "Change", text: "10 files, +12 −4 · a.py, b.py, c.py, d.py, e.py, f.py, g.py, h.py, and 2 more" },
    { label: "Verifier", text: "pass — Greeting verified." },
    { label: "Checks",
      text: "2 of 3 repository checks passed (1 failure also on the base commit); CI success on 1 check" },
    { label: "Review", text: "reviewed by codex; 1 finding open (1 blocking); round 2" },
    { label: "Cost", text: "$1.50 so far · 1 charged attempt" },
  ]);
});

test("facts that were not recorded are left out, and counts read in the singular or plural", () => {
  assert.deepEqual(mergeFactRows({ pr: 1, head: "b", ci: { state: "pending", checks: 2 },
    review: { state: "unavailable", reviewers: [], round: 0, open: 0, blocking: 0 }, cost: { usd: 0, attempts: 2 } }), [
    { label: "Checks", text: "CI pending on 2 checks" },
    { label: "Review", text: "no review of this head; 0 findings open; round 0" },
    { label: "Cost", text: "$0.00 so far · 2 charged attempts" },
  ]);
  assert.deepEqual(mergeFactRows({ pr: 1, head: "b" }), []);
  // A session with no recorded cost makes the sum a floor, and the row says so.
  assert.deepEqual(mergeFactRows({ pr: 1, head: "b", cost: { usd: 2, attempts: 3, unpriced: 2 } }), [
    { label: "Cost", text: "at least $2.00 so far · 3 charged attempts · 2 sessions with no recorded cost" }]);
});

test("the text drops the lines its rows repeat, and a decision without facts is shown as it is", () => {
  const head = "PR #7 passed independent verification and CI at aaaaaaaaaa. url";
  const body = `${head}\n${facts.text}\n\nReviewed at aaaaaaaaaa.`;
  assert.equal(withoutFacts(body, facts), `${head}\n\nReviewed at aaaaaaaaaa.`);
  assert.equal(withoutFacts(body, undefined), body);
  assert.equal(withoutFacts(body, { pr: 7, head: "a" }), body);
});
