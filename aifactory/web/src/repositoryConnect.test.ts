import { test } from "node:test";
import assert from "node:assert/strict";
import { connectReviewedRepository, previewRepository, repositoryDiscoveryUrl } from "./repositoryConnect.ts";
import type { api } from "./api.ts";

test("Settings previews without connecting, then explicitly submits accepted or edited runtime QA", async () => {
  const calls: { path: string; json: unknown }[] = [];
  const detected = { full_name: "Acme/App", default_branch: "main", config: {
    setup: ["npm ci"], checks: ["npm test"], qa: { start: "npm run dev -- --port $PORT" },
  } };
  const request = (async (path, init) => {
    calls.push({ path, json: init?.json });
    return detected;
  }) as typeof api;
  const preview = await previewRepository("workspace", "acme/app", request);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].path, "/api/workspaces/workspace/repositories/preview");
  assert.deepEqual(calls[0].json, { full_name: "acme/app" });
  for (const start of [preview.config.qa!.start!, "custom --port $PORT", ""]) {
    await connectReviewedRepository("workspace", preview, {
      setup: " npm ci \n\n", checks: " npm test ", start, deploys: true,
    }, request);
    assert.equal(calls.at(-1)!.path, "/api/workspaces/workspace/repositories");
    assert.deepEqual(calls.at(-1)!.json, { full_name: "Acme/App", config: {
      setup: ["npm ci"], checks: ["npm test"], qa: { start }, release: { mode: "on_merge" },
    } });
  }
});

test("repository discovery passes the server search and its next page without a local cap", async () => {
  const urls: string[] = [];
  const discover = async (page: number) => {
    urls.push(repositoryDiscoveryUrl("workspace", "team/project & web", page));
    return { items: [], next_page: page === 1 ? 3 : null };
  };
  const first = await discover(1);
  if (first.next_page) await discover(first.next_page);
  assert.deepEqual(urls, [
    "/api/workspaces/workspace/github/repos?q=team%2Fproject%20%26%20web&page=1",
    "/api/workspaces/workspace/github/repos?q=team%2Fproject%20%26%20web&page=3",
  ]);
});
