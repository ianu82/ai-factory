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

test("revisit reviews use saved commands and preserve unrelated nested configuration", async () => {
  const { commandsFromRepository, reviewedConfig, repositoryStack, saveRepositoryReview } = await import("./repositoryConnect.ts");
  const saved = { id: "saved", full_name: "Acme/App", default_branch: "main", description: "", config: {
    setup: ["custom install"], checks: ["custom test"], smoke: ["finite smoke"],
    detected: ["Vite", "Node", "Vite"], context: "keep", protected_paths: ["auth/**"],
    qa: { start: "custom start", pages: ["/login"], env: { A: "B" } },
    release: { mode: "on_merge", health_url: "https://example.test" },
  } };
  const commands = commandsFromRepository(saved);
  assert.equal(commands.start, "custom start");
  assert.equal(commands.deploys, true);
  assert.deepEqual(repositoryStack(saved), ["Node", "Vite"]);
  const config = reviewedConfig(saved.config, { ...commands, start: "", checks: "new test", deploys: false });
  assert.deepEqual(config.smoke, ["finite smoke"]);
  assert.deepEqual(config.qa, { start: "", pages: ["/login"], env: { A: "B" } });
  assert.deepEqual(config.release, { health_url: "https://example.test" });
  assert.equal(config.context, "keep");
  const calls: string[] = [];
  const request = (async (path, init) => {
    calls.push(`${init?.method || "GET"} ${path}`);
    return init ? { ...saved, config } : [saved];
  }) as typeof api;
  await saveRepositoryReview("workspace", saved, commands, "saved", request);
  assert.deepEqual(calls, ["GET /api/workspaces/workspace/repositories", "PATCH /api/workspaces/workspace/repositories/saved"]);
});

test("partial retry reconciles a successful connection whose response was lost without reconnecting", async () => {
  const { saveRepositoryReview } = await import("./repositoryConnect.ts");
  const preview = { full_name: "Acme/App", default_branch: "main", config: { detected: ["Node"] } };
  const commands = { setup: "edited install", checks: "edited test", start: "", deploys: true };
  let connected = false;
  let posts = 0;
  const request = (async (_path, init) => {
    if (init?.method === "POST") {
      posts++;
      connected = true;
      throw new Error("Lost response");
    }
    if (init?.method === "PATCH") return { ...preview, id: "one", config: init.json };
    return connected ? [{ ...preview, id: "one" }] : [];
  }) as typeof api;
  await assert.rejects(saveRepositoryReview("workspace", preview, commands, undefined, request), /Lost response/);
  const result = await saveRepositoryReview("workspace", preview, commands, undefined, request);
  assert.equal(posts, 1);
  assert.deepEqual(result.config, { config: { detected: ["Node"], setup: ["edited install"], checks: ["edited test"], qa: { start: "" }, release: { mode: "on_merge" } } });
});
