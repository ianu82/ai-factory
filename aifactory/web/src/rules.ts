// The rules in force, in plain words, from a workspace's settings (Settings → Autonomy & limits). A missing key reads
// as the default the Settings page shows. Only a type import: node's test runner loads this file directly.
import type { ReviewPolicy, Settings } from "./api";

/** A sentence: plain text, with the values in force in bold. */
export type RulePart = string | { b: string };
/** ``target``: the id of the control that changes the rule, if it has one. */
export interface Rule { id: string; target: string | null; parts: RulePart[] }

export type RuleSettings = Omit<Partial<Settings>, "review"> & { review?: Partial<ReviewPolicy> };

// The review policy's defaults (aifactory/review.py DEFAULT_REVIEW), except the request comment, which the page
// never needs a default for.
export const REVIEW_DEFAULTS = {
  address: "unresolved", reviewers: "all", trust: "collaborators", block_on: "p1", on_timeout: "proceed",
  wait_seconds: 600, max_rounds: 3,
} as const;

const num = (value: unknown, fallback: number) =>
  typeof value === "number" && Number.isFinite(value) ? value : fallback;
const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;
export const usd = (n: number) => `$${Number.isInteger(n) ? n : n.toFixed(2)}`;
export const duration = (seconds: number) =>
  seconds > 0 && seconds % 60 === 0 ? plural(seconds / 60, "minute") : plural(seconds, "second");
const b = (text: string) => ({ b: text });
const fixes = (rounds: number, go: string): RulePart[] => rounds === 0
  ? ["; with ", b("no rounds of fixes"), ", a person decides."]
  : [` and ${go} back to the builder for at most `, b(plural(rounds, "round")), " of fixes; then a person decides."];
export const ruleText = (rule: Rule) => rule.parts.map((p) => typeof p === "string" ? p : p.b).join("");

export function rulesInForce(s: RuleSettings, webhookSet: boolean): Rule[] {
  const autonomy = { plan: "auto", merge: "auto", release: "approval", ...s.autonomy };
  const learning = s.learning?.status === "learning" ? " (learning mode)" : "";
  const review = { ...REVIEW_DEFAULTS, ...s.review };
  const rounds = num(review.max_rounds, REVIEW_DEFAULTS.max_rounds);
  const wait = num(review.wait_seconds, REVIEW_DEFAULTS.wait_seconds);
  const attempts = num(s.max_attempts, 3);
  const retries = num(s.auto_retry_rounds, 1);
  const lifeAttempts = num(s.limits?.task_attempts_total, 0);
  const lifeUsd = num(s.budgets?.task_usd, 0);
  const open = num(s.limits?.open_outcomes, 0);
  const holdMinutes = num(s.holds?.test_minutes, 5);
  const ignored = Array.isArray(review.ignore) ? review.ignore.filter((x) => typeof x === "string" && x) : [];
  const rules: Rule[] = [
    { id: "plans", target: "rules-autonomy", parts: autonomy.plan === "approval"
      ? ["Plans ", b(`wait for a person${learning}`), " before building starts."]
      : ["Plans are ", b("approved automatically"),
        " by the plan reviewer; one it cannot approve comes to a person."] },
    { id: "merges", target: "rules-autonomy", parts: [
      ...(autonomy.merge === "approval"
        ? ["Pull requests ", b(`wait for a person${learning}`), " before merging."]
        : ["Pull requests are ", b("merged automatically"), " once checks, verification and review are clear."]),
      " A repository can override this on its card under Repositories."] },
    { id: "releases", target: "rules-autonomy", parts: autonomy.release === "auto"
      ? ["In repositories that deploy when main changes, merges ", b("release automatically"), "."]
      : ["In repositories that deploy when main changes, a merge ", b("waits for a person"), " before it releases."] },
  ];
  if (review.address === "off") {
    rules.push({ id: "review", target: "rules-review", parts: [
      "Reviews are ", b("not waited for"), ": a green pull request goes on to merge. A repository can set its own."] });
  } else {
    rules.push(
      { id: "review-wait", target: "rules-review", parts: [
        "A requested review is waited for up to ", b(duration(wait)), "; when none arrives, the merge ",
        b(review.on_timeout === "ask" ? "waits for a person" : "goes ahead without one"),
        ". A repository can set its own."] },
      { id: "review-block", target: "rules-review", parts: review.block_on === "none"
        ? ["Review findings ", b("never block"), " a merge."]
        : review.block_on === "all"
          ? [b("Every finding"), " blocks a merge", ...fixes(rounds, "goes")]
          : [b("P0, P1 and unrated findings"), " block a merge", ...fixes(rounds, "go")] },
      { id: "review-counts", target: "rules-review", parts: [
        "Findings are ",
        // A comment on the pull request itself counts only from a person; threads and change requests count from
        // the reviewers chosen (review.py normalize_feedback).
        ...(review.address === "changes_requested"
          ? [b("threads in reviews that request changes"), " from ",
            b(review.reviewers === "people" ? "people" : review.reviewers === "bots" ? "bots" : "people and bots")]
          : review.reviewers === "people"
            ? [b("unresolved threads, change requests and comments"), " from ", b("people")]
            : review.reviewers === "bots"
              ? [b("unresolved threads and change requests"), " from ", b("bots")]
              : [b("unresolved threads and change requests"), " from ", b("people and bots"), ", and ",
                b("comments"), " from ", b("people")]),
        "; trusted: ", b(review.trust === "anyone" ? "anyone" : "collaborators and installed bots"),
        ...(ignored.length ? ["; ignored: ", b(ignored.join(", "))] : []), "."] },
    );
  }
  rules.push(
    { id: "rails", target: null, parts: [
      "Changes to files listed in a repository's ", b(".factory/rails"),
      " always wait for a person, whatever the merge rule says."] },
    { id: "attempts", target: "rules-limits", parts: [
      "A task gets ", b(plural(attempts, "attempt")), " per try and ",
      b(retries === 0 ? "no automatic retries" : retries === 1 ? "1 automatic retry" : `${retries} automatic retries`),
      " before it asks a person."] },
    { id: "lifetime", target: "rules-limits", parts: lifeAttempts <= 0 && lifeUsd <= 0
      ? ["A task has ", b("no lifetime limit"), " in attempts or dollars."]
      : lifeAttempts > 0 && lifeUsd > 0
        ? ["Over its whole life a task stops and asks a person at ", b(plural(lifeAttempts, "attempt")), " or ",
          b(usd(lifeUsd)), ", whichever comes first."]
        : lifeAttempts > 0
          ? ["Over its whole life a task stops and asks a person at ", b(plural(lifeAttempts, "attempt")),
            "; there is ", b("no dollar limit"), "."]
          : ["Over its whole life a task stops and asks a person at ", b(usd(lifeUsd)), "; there is ",
            b("no attempt limit"), "."] },
    { id: "budgets", target: "rules-limits", parts: [
      "Each agent session may spend up to ", b(usd(num(s.budgets?.run_usd, 8))),
      "; an outcome pauses for a decision at ",
      b(usd(num(s.budgets?.outcome_usd, 40))), "."] },
    { id: "open", target: "rules-limits", parts: open > 0
      ? ["At most ", b(plural(open, "outcome")), " are open at once; new ones wait unplanned."]
      : ["Open outcomes: ", b("no limit"), "."] },
    { id: "alert-decision", target: "rules-alerts", parts: [
      "A decision that has waited ", b(plural(num(s.alerts?.after_minutes, 120), "minute")), " raises an alert."] },
    { id: "alert-stopped", target: "rules-alerts", parts: [
      "\"Nothing is running\" is raised after ", b(plural(num(s.alerts?.stopped_minutes, 20), "minute")),
      " with work waiting and no agent at work, ",
      b(webhookSet ? "in the app and to the webhook" : "in the app only (no webhook is set)"), "."] },
    { id: "hold", target: "rules-alerts", parts: holdMinutes > 0
      ? ["While a provider hold is open the Factory tests the provider every ", b(plural(holdMinutes, "minute")),
        " and lifts the hold when it answers."]
      : ["A provider hold is ", b("never tested"), ": it stays until a person lifts it."] },
    { id: "director", target: "rules-director", parts: s.director?.mode === "on"
      ? ["The Director is ", b("on"), ": it decides what would otherwise wait for a person."]
      : ["The Director is ", b("off"), ": decisions wait for a person."] },
    { id: "lead", target: "rules-director", parts: s.lead?.enabled === false
      ? ["The Lead is ", b("off"), ": no agent looks after each outcome as a whole."]
      : ["The Lead is ", b("on"), ": one agent is responsible for each outcome, from acceptance to delivery."] },
  );
  return rules;
}
