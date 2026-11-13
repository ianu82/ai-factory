import { test } from "node:test";
import assert from "node:assert/strict";
import {
  generalRepositoryPatch, newWebhook, parseTarget, parseWorkflowInputs, releaseDraft, serializeRelease,
  type ReleaseDraft, type TrainReleaseConfig, type RepositoryReleaseConfig,
} from "./releaseTypes.ts";
import { reviewedConfig } from "./repositoryConnect.ts";

function train(): TrainReleaseConfig {
  return {
    mode: "train", schedule: { weekdays: ["sat", "sun"], time: "23:45", timezone: "America/New_York" },
    cut: { mode: "version_pr", tag_prefix: "release-", initial_version: "12.34.56", increment: "patch",
      version_file: { path: "packages/app/package.json", format: "json", key: "app.version" } },
    staging: { kind: "workflow", workflow: "staging.yml", inputs: { environment: "staging", flag: "false", count: "42" }, correlation_input: "operation_id" },
    production: { kind: "webhook", endpoint: "https://deploy.example.test/production", secret_configured: true, timeout_seconds: 90,
      completion: { mode: "poll", status_url: "https://deploy.example.test/status", handle_key: "deployment_id", state_key: "status",
        success_values: ["done", "healthy"], failure_values: ["failed", "cancelled"], interval_seconds: 25, timeout_seconds: 2500 } },
    rollback: { kind: "webhook", endpoint: "https://deploy.example.test/rollback", secret_configured: true, timeout_seconds: 120,
      completion: { mode: "synchronous", success_statuses: [201, 204] } },
    staging_smoke: { commands: ["npm test", "./smoke.sh"], urls: ["https://staging.example.test/health"], timeout_seconds: 123 },
    production_smoke: { commands: ["./smoke.sh --production"], urls: ["https://example.test/health", "https://example.test/ready"], timeout_seconds: 456 },
    approval: "approval",
  };
}
function publicWithoutFlags(config: TrainReleaseConfig) {
  const result = structuredClone(config);
  for (const key of ["staging", "production", "rollback"] as const) {
    if (result[key].kind === "webhook") delete result[key].secret_configured;
  }
  return result;
}
function validDraft() { return releaseDraft(train()); }

test("every train field round-trips through editing and serialization without mutation", () => {
  const saved = train(), before = structuredClone(saved), draft = releaseDraft(saved);
  assert.equal(draft.staging.kind, "workflow");
  const serialized = serializeRelease(draft, saved, true);
  assert.deepEqual(serialized, publicWithoutFlags(saved));
  assert.deepEqual(saved, before);
  assert.deepEqual(serializeRelease(releaseDraft(serialized), serialized, true), serialized);
  assert.ok(!JSON.stringify(serialized).includes("secret_ref"));
  assert.ok(!JSON.stringify(serialized).includes("secret_configured"));
});

test("tag and plain version-file cutting plus all approval policies can be set", () => {
  const draft = validDraft();
  draft.cut = { mode: "tag", tag_prefix: "", initial_version: "0.0.0", increment: "patch", version_file: null };
  for (const approval of ["auto", "approval", "inherit"] as const) {
    draft.approval = approval;
    const serialized = serializeRelease(draft, null, true) as TrainReleaseConfig;
    assert.deepEqual(serialized.cut, draft.cut); assert.equal(serialized.approval, approval);
  }
  draft.cut.mode = "version_pr"; draft.cut.version_file = { path: "VERSION", format: "plain" };
  assert.deepEqual((serializeRelease(draft, null, true) as TrainReleaseConfig).cut, draft.cut);
});

test("backend defaults produce editable complete values", () => {
  const workflow = { kind: "workflow" as const, workflow: "deploy.yml", correlation_input: "release_key" };
  const minimal = { mode: "train" as const, staging: workflow, production: workflow, rollback: workflow,
    staging_smoke: { commands: ["test"] }, production_smoke: { urls: ["https://example.test"] } };
  const result = serializeRelease(releaseDraft(minimal), minimal, false) as TrainReleaseConfig;
  assert.deepEqual(result.schedule, { weekdays: ["mon", "tue", "wed", "thu", "fri"], time: "09:00", timezone: "UTC" });
  assert.equal(result.cut?.increment, "patch"); assert.equal(result.approval, "inherit");
  assert.deepEqual(result.staging_smoke, { commands: ["test"], urls: [], timeout_seconds: 60 });
});

test("workflow inputs reject malformed JSON, nonobjects, nonstrings and reserved correlation without echoing input", () => {
  for (const input of ['{"private-paste":', 'null', '[]', 'true', '{"a":1}', '{"a":null}', '{"a":{}}', '{"release_key":"abc"}', '{"api-key":"private-paste"}']) {
    assert.throws(() => parseWorkflowInputs(input, "release_key"), (error: Error) => !error.message.includes("private-paste"));
  }
  assert.deepEqual(parseWorkflowInputs('{"a":"", "b":"false", "c":"{\\"d\\":1}"}', "key"), { a: "", b: "false", c: '{"d":1}' });
  assert.throws(() => parseWorkflowInputs(JSON.stringify(Object.fromEntries(Array.from({ length: 31 }, (_, n) => [n, "a"]))), "key"), /30/);
  assert.throws(() => parseWorkflowInputs(JSON.stringify({ a: "x".repeat(2001) }), "key"), /2000/);
});

test("invalid release fields fail before any serialized update is available", () => {
  const changes: [(draft: ReleaseDraft) => void, RegExp][] = [
    [(d) => { d.schedule.weekdays = []; }, /weekday/],
    [(d) => { d.schedule.time = "24:00"; }, /HH:MM/],
    [(d) => { d.schedule.timezone = "Moon/City"; }, /IANA/],
    [(d) => { d.cut.initial_version = "01.2.3"; }, /major.minor.patch/],
    [(d) => { d.cut.tag_prefix = "a/b"; }, /Tag prefix/],
    [(d) => { d.cut.version_file!.path = "../version"; }, /relative path/],
    [(d) => { d.cut.version_file!.key = ""; }, /JSON version key/],
    [(d) => { d.staging_smoke.commands = ""; d.staging_smoke.urls = ""; }, /at least one/],
    [(d) => { d.production_smoke.timeout_seconds = 0; }, /smoke timeout/],
    [(d) => { d.production_smoke.urls = "http://example.test"; }, /HTTPS/],
    [(d) => { d.staging = { kind: "workflow", workflow: "test", inputs: "{bad}", correlation_input: "key" }; }, /Staging:.*JSON/],
  ];
  for (const [change, expected] of changes) {
    const draft = validDraft(); change(draft);
    assert.throws(() => serializeRelease(draft, train(), true), expected);
  }
});

test("webhook completion requires a configured secret and bounded final evidence", () => {
  const target = { ...newWebhook(), endpoint: "https://example.test/hook" };
  assert.throws(() => parseTarget(target, "Rollback", false), /Rollback:.*secret/);
  for (const statuses of [[202], [500], [], [200.5], [NaN]]) {
    assert.throws(() => parseTarget({ ...target, completion: { mode: "synchronous", success_statuses: statuses } }, "Staging", true), /never 202/);
  }
  for (const endpoint of ["http://example.test", "https://user:pass@example.test", "https://example.test/?token=foo", "https://example.test/#foo", "https://example.test/white space"]) {
    assert.throws(() => parseTarget({ ...target, endpoint }, "Production", true), /HTTPS/);
  }
  const poll = train().production;
  assert.equal(poll.kind, "webhook");
  if (poll.kind !== "webhook" || poll.completion.mode !== "poll") throw new Error("fixture");
  assert.throws(() => parseTarget({ ...poll, completion: { ...poll.completion, success_values: ["failed"] } }, "Production", true), /overlap/);
  assert.throws(() => parseTarget({ ...poll, completion: { ...poll.completion, interval_seconds: 301 } }, "Production", true), /interval/);
  assert.throws(() => parseTarget({ ...poll, completion: { ...poll.completion, timeout_seconds: 86401 } }, "Production", true), /timeout/);
});

test("continuous and legacy on_merge editing preserves health-check and arbitrary fields", () => {
  for (const mode of [undefined, null, "on_merge", "continuous"] as const) {
    const saved = { mode, health_url: "https://legacy.test/health?deep=1#ok", expect_text: "healthy", custom: { keep: [1, 2] } };
    const draft = releaseDraft(saved), result = serializeRelease(draft, saved, false);
    assert.deepEqual(result, { ...saved, mode: mode ?? null });
    draft.health_url = "https://new.test";
    assert.deepEqual(serializeRelease(draft, saved, false), { ...saved, mode: mode ?? null, health_url: "https://new.test" });
  }
});

test("unrelated settings saves omit release entirely and retain server-owned configuration", () => {
  for (const release of [train(), { mode: "on_merge", health_url: "https://example.test", expect_text: "ok", extra: true }] as RepositoryReleaseConfig[]) {
    const stored = { release, qa: { start: "serve" }, build: { tokens: 123 }, unrelated: { nested: [true] } };
    const patch = generalRepositoryPatch({ context: "new", release });
    assert.deepEqual(patch, { context: "new" });
    assert.deepEqual({ ...stored, ...patch }, { ...stored, context: "new" });
    for (const deploys of [true, false]) {
      const review = reviewedConfig({ release }, { setup: "test", checks: "test", start: "serve", deploys });
      if (release.mode === "train") assert.deepEqual(review.release, release);
    }
  }
});
