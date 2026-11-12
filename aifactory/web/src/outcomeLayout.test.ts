import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const styles = await readFile(new URL("./styles.css", import.meta.url), "utf8");
const rule = (selector: string) => {
  const body = styles.split(`${selector} {`)[1]?.split("}")[0];
  assert.ok(body, `${selector} must have a shared layout rule`);
  return body;
};

test("outcome conversation and full activity scroll inside the main pane", () => {
  const outcome = rule(".othread, .othread-full");
  assert.match(outcome, /overflow(?:-y)?:\s*auto\s*;/);
  assert.match(outcome, /min-height:\s*0\s*;/);
  assert.match(outcome, /flex:\s*1\s*;/);
  assert.doesNotMatch(outcome, /min-height:\s*100%/);
});

test("the sidebar owns its scrolling independently of Mind styles", () => {
  assert.match(rule(".sidebar"), /overflow-y:\s*auto\s*;/);
});
