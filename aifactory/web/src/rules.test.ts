import assert from "node:assert/strict";
import { test } from "node:test";
import { ruleText, rulesInForce } from "./rules.ts";

const read = (settings: any, webhook = false): Record<string, string> =>
  Object.fromEntries(rulesInForce(settings, webhook).map((rule) => [rule.id, ruleText(rule)]));

test("an older workspace with keys missing reads as the defaults, never undefined", () => {
  const rules = read({});
  for (const line of Object.values(rules)) assert.doesNotMatch(line, /undefined|NaN|null/);
  assert.equal(rules.plans,
    "Plans are approved automatically by the plan reviewer; one it cannot approve comes to a person.");
  assert.match(rules.merges,
    /^Pull requests are merged automatically once checks, verification and review are clear\./);
  assert.equal(rules.releases,
    "In repositories that deploy when main changes, a merge waits for a person before it releases.");
  assert.equal(rules["review-wait"],
    "A requested review is waited for up to 10 minutes; when none arrives, the merge goes ahead without one. "
    + "A repository can set its own.");
  assert.equal(rules["review-block"],
    "P0, P1 and unrated findings block a merge and go back to the builder for at most 3 rounds of fixes; "
    + "then a person decides.");
  assert.equal(rules.attempts, "A task gets 3 attempts per try and 1 automatic retry before it asks a person.");
  assert.equal(rules.lifetime, "A task has no lifetime limit in attempts or dollars.");
  assert.equal(rules.budgets, "Each agent session may spend up to $8; an outcome pauses for a decision at $40.");
  assert.equal(rules.open, "Open outcomes: no limit.");
  assert.equal(rules["alert-decision"], "A decision that has waited 120 minutes raises an alert.");
  assert.match(rules["alert-stopped"], /after 20 minutes .* in the app only \(no webhook is set\)\.$/);
  assert.match(rules.hold, /every 5 minutes and lifts the hold when it answers\.$/);
  assert.equal(rules.director, "The Director is off: decisions wait for a person.");
  assert.match(rules.lead, /^The Lead is on:/);
  assert.match(rules.rails, /\.factory\/rails always wait for a person, whatever the merge rule says\.$/);
});

test("approval and automatic read differently, and learning mode says why", () => {
  const rules = read({ autonomy: { plan: "approval", merge: "approval", release: "auto" } });
  assert.equal(rules.plans, "Plans wait for a person before building starts.");
  assert.match(rules.merges, /^Pull requests wait for a person before merging\. A repository can override this/);
  assert.equal(rules.releases, "In repositories that deploy when main changes, merges release automatically.");
  const learning = read({ learning: { status: "learning" }, autonomy: { plan: "approval", merge: "approval" } });
  assert.equal(learning.plans, "Plans wait for a person (learning mode) before building starts.");
  assert.match(learning.merges, /^Pull requests wait for a person \(learning mode\) before merging\./);
});

test("0 means no limit; set limits show their values", () => {
  const none = read({ limits: { task_attempts_total: 0, open_outcomes: 0 }, budgets: { task_usd: 0 },
    holds: { test_minutes: 0 }, auto_retry_rounds: 0 });
  assert.equal(none.lifetime, "A task has no lifetime limit in attempts or dollars.");
  assert.equal(none.open, "Open outcomes: no limit.");
  assert.equal(none.hold, "A provider hold is never tested: it stays until a person lifts it.");
  assert.match(none.attempts, /and no automatic retries before/);
  const set = read({ limits: { task_attempts_total: 12, open_outcomes: 10 }, budgets: { run_usd: 7.5, outcome_usd: 40,
    task_usd: 15 }, max_attempts: 2, auto_retry_rounds: 2, holds: { test_minutes: 1 },
    alerts: { after_minutes: 30, stopped_minutes: 5 } }, true);
  assert.equal(set.lifetime,
    "Over its whole life a task stops and asks a person at 12 attempts or $15, whichever comes first.");
  assert.equal(set.open, "At most 10 outcomes are open at once; new ones wait unplanned.");
  assert.equal(set.budgets, "Each agent session may spend up to $7.50; an outcome pauses for a decision at $40.");
  assert.equal(set.attempts, "A task gets 2 attempts per try and 2 automatic retries before it asks a person.");
  assert.match(set.hold, /every 1 minute and/);
  assert.equal(set["alert-decision"], "A decision that has waited 30 minutes raises an alert.");
  assert.match(set["alert-stopped"], /after 5 minutes .* in the app and to the webhook\.$/);
  assert.match(read({ limits: { task_attempts_total: 12 } }).lifetime, /at 12 attempts; there is no dollar limit\.$/);
  assert.match(read({ budgets: { task_usd: 15 } }).lifetime, /at \$15; there is no attempt limit\.$/);
});

test("the review policy's values read as what the Factory does", () => {
  const rules = read({ review: { wait_seconds: 90, max_rounds: 1, block_on: "all", on_timeout: "ask",
    reviewers: "people", trust: "anyone", address: "changes_requested", ignore: ["bot-a", "bob"] } });
  assert.equal(rules["review-wait"], "A requested review is waited for up to 90 seconds; when none arrives, the merge "
    + "waits for a person. A repository can set its own.");
  assert.equal(rules["review-block"],
    "Every finding blocks a merge and goes back to the builder for at most 1 round of fixes; then a person decides.");
  assert.equal(rules["review-counts"],
    "Findings are threads in reviews that request changes from people; trusted: anyone; ignored: bot-a, bob.");
  assert.equal(read({ review: { block_on: "none" } })["review-block"], "Review findings never block a merge.");
  assert.equal(read({ review: { max_rounds: 0 } })["review-block"],
    "P0, P1 and unrated findings block a merge; with no rounds of fixes, a person decides.");
  assert.equal(read({ review: { reviewers: "all", trust: "collaborators" } })["review-counts"],
    "Findings are unresolved threads and change requests from people and bots, and comments from people; "
    + "trusted: collaborators and installed bots.");
  assert.equal(read({ review: { reviewers: "bots" } })["review-counts"],
    "Findings are unresolved threads and change requests from bots; trusted: collaborators and installed bots.");
  assert.equal(read({ review: { reviewers: "people" } })["review-counts"],
    "Findings are unresolved threads, change requests and comments from people; "
    + "trusted: collaborators and installed bots.");
  const off = read({ review: { address: "off", wait_seconds: 600 } });
  assert.equal(off.review,
    "Reviews are not waited for: a green pull request goes on to merge. A repository can set its own.");
  assert.equal(off["review-wait"], undefined);
  assert.equal(off["review-block"], undefined);
});
