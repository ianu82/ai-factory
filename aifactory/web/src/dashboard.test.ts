import { test } from "node:test";
import assert from "node:assert/strict";
import {
  asPeriod, bucketLabel, change, hours, niceMax, noPerson, perDay, percent, phaseShares, roles, sparkline, tickIndexes,
} from "./dashboard.ts";

test("periods default to weekly and buckets are named in UTC", () => {
  assert.equal(asPeriod("day"), "day");
  assert.equal(asPeriod("month"), "month");
  assert.equal(asPeriod("year"), "week");
  assert.equal(asPeriod(null), "week");
  assert.equal(bucketLabel("2026-11-09T00:00:00Z", "week"), "Nov 9");
  assert.equal(bucketLabel("2026-11-09T00:00:00Z", "week", true), "Week of Nov 9");
  assert.equal(bucketLabel("2026-11-01T00:00:00Z", "month", true), "Nov 2026");
  assert.equal(bucketLabel("2026-01-01T00:00:00Z", "day"), "Jan 1");
});

test("durations read as people say them", () => {
  assert.equal(hours(null), "—");
  assert.equal(hours(0.004), "1m");
  assert.equal(hours(0.75), "45m");
  assert.equal(hours(18.4), "18h");
  assert.equal(hours(53), "2d 5h");
  assert.equal(hours(72), "3d");
  assert.equal(hours(71.9), "3d");
});

test("rates never divide by zero and merges per day count at least a day", () => {
  assert.equal(percent(null), "—");
  assert.equal(percent(0.256), "26%");
  assert.equal(perDay(6, 3), 2);
  assert.equal(perDay(2, 0.25), 2);
});

test("the Director standing in is no person, and the share says how much it did", () => {
  assert.deepEqual(noPerson({ unattended: 1, director: 2, assisted: 1, unverified: 5 }), { share: 0.75, alone: 0.25 });
  assert.deepEqual(noPerson({ unattended: 0, director: 0, assisted: 0, unverified: 3 }), { share: null, alone: null });
});

test("changes are toned by whether that direction is good for the measure", () => {
  const f = (v: number) => v.toFixed(0);
  assert.deepEqual(change(5, 3, "up", f), { text: "+2", tone: "good" });
  assert.deepEqual(change(5, 3, "down", f), { text: "+2", tone: "bad" });
  assert.deepEqual(change(1, 3, "down", f), { text: "−2", tone: "good" });
  assert.deepEqual(change(3, 3, "up", f), { text: "no change", tone: "flat" });
  assert.equal(change(null, 3, "up", f), null);
});

test("axes end on clean numbers", () => {
  assert.equal(niceMax(0), 1);
  assert.equal(niceMax(7), 10);
  assert.equal(niceMax(13), 20);
  assert.equal(niceMax(41), 50);
  assert.equal(niceMax(100), 100);
  assert.equal(niceMax(0.3), 0.5);
});

test("a missing value breaks the sparkline instead of reading as zero", () => {
  assert.equal(sparkline([null, null], 100, 20), "");
  assert.equal(sparkline([1, null, 3], 100, 20, 0), "M0.0 20.0M100.0 0.0");
  assert.equal(sparkline([2, 2], 100, 20, 0), "M0.0 10.0L100.0 10.0");
});

test("the phase bar splits by each stage's median and skips unknown stages", () => {
  const median = (h: number | null) => ({ median_hours: h, sample_count: h === null ? 0 : 1 });
  const shares = phaseShares({ plan: median(1), build: median(3), verify: median(null), review: median(4),
    merge_queue: median(0), release: median(null) });
  assert.deepEqual(shares.map((s) => s.share), [0.125, 0.375, 0, 0.5, 0, 0]);
  assert.deepEqual(shares.map((s) => s.label), ["Plan", "Build", "Verify", "Review & CI", "Merge queue", "Release"]);
});

test("x labels keep the first and last and never crowd", () => {
  assert.deepEqual(tickIndexes(4, 6), [0, 1, 2, 3]);
  assert.deepEqual(tickIndexes(30, 6), [0, 6, 12, 18, 24, 29]);
  assert.deepEqual(tickIndexes(12, 6), [0, 3, 6, 9, 11]);
});

test("roles are ordered by runs", () => {
  assert.deepEqual(roles({ verifier: 3, builder: 9, planner: 3 }), ["builder", "planner", "verifier"]);
});
