import { test } from "node:test";
import assert from "node:assert/strict";
import type { AgentMessage } from "./api.ts";
import { INTENT_FILTERS, PARTY_FILTERS, byDay, dayLabel, messagesUrl, verb } from "./messageLog.ts";

const message = (id: number, at: string): AgentMessage => ({
  id, at, kind: "verified", level: "success", intent: "answer", says: "Verified",
  from: { role: "verifier", label: "Verifier · T1" }, to: { role: "builder", label: "Builder · T1" },
  about: { outcome: null, task: null, pr: null },
});

test("rows read as sentences: sender, verb, receiver", () => {
  assert.equal(verb("request"), "asks");
  assert.equal(verb("answer"), "answers");
  assert.equal(verb("inform"), "tells");
  assert.equal(verb("handoff"), "hands off to");
  assert.equal(verb("propose"), "proposes to");
});

test("filters map to the API's party and intent values, with 'everyone' and 'all' first", () => {
  assert.equal(PARTY_FILTERS[0].key, "");
  assert.deepEqual(PARTY_FILTERS.slice(1).map((f) => f.key), ["minds", "people", "planning", "builders", "verifier", "ci", "factory"]);
  assert.deepEqual(INTENT_FILTERS.map((f) => f.key), ["", "request", "propose", "answer", "inform", "handoff"]);
});

test("the request URL carries only the filters that are set, and the cursor", () => {
  assert.equal(messagesUrl("ws1", {}), "/api/workspaces/ws1/messages");
  assert.equal(messagesUrl("ws1", { party: "minds", intent: "", outcome: "o1" }, 42),
    "/api/workspaces/ws1/messages?party=minds&outcome_id=o1&before=42");
});

test("messages group under Today, Yesterday or their date, in order", () => {
  const now = new Date(2026, 10, 11, 12, 0);
  const today = new Date(2026, 10, 11, 9, 30).toISOString();
  const earlier = new Date(2026, 10, 11, 0, 5).toISOString();
  const yesterday = new Date(2026, 10, 10, 23, 50).toISOString();
  const older = new Date(2026, 10, 8, 10, 0).toISOString();
  assert.equal(dayLabel(today, now), "Today");
  assert.equal(dayLabel(yesterday, now), "Yesterday");
  assert.notEqual(dayLabel(older, now), "Yesterday");
  const groups = byDay([message(4, today), message(3, earlier), message(2, yesterday), message(1, older)], now);
  assert.deepEqual(groups.map((g) => [g.day === "Today" || g.day === "Yesterday" ? g.day : "date", g.messages.map((m) => m.id)]),
    [["Today", [4, 3]], ["Yesterday", [2]], ["date", [1]]]);
});

test("yesterday is the previous calendar date, even the day after a clock change", () => {
  const zone = process.env.TZ;
  process.env.TZ = "America/New_York"; // clocks went forward on 8 March 2026, so that day was 23 hours long
  try {
    const now = new Date(2026, 2, 9, 12, 0);
    assert.equal(dayLabel(new Date(2026, 2, 8, 0, 30).toISOString(), now), "Yesterday");
    assert.equal(dayLabel(new Date(2026, 2, 8, 23, 30).toISOString(), now), "Yesterday");
    assert.notEqual(dayLabel(new Date(2026, 2, 7, 23, 30).toISOString(), now), "Yesterday");
  } finally {
    if (zone === undefined) delete process.env.TZ; else process.env.TZ = zone;
  }
});
