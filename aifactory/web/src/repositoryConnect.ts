import type { api, Repo } from "./api";

type Request = typeof api;
export interface RepositoryPreview {
  full_name: string;
  default_branch: string;
  config: Repo["config"];
}
export interface ReviewedCommands {
  setup: string;
  checks: string;
  start: string;
  deploys: boolean;
}

export function repositoryDiscoveryUrl(workspace: string, search: string, page: number) {
  return `/api/workspaces/${workspace}/github/repos?q=${encodeURIComponent(search)}&page=${page}`;
}

export async function previewRepository(workspace: string, fullName: string, request: Request) {
  return request<RepositoryPreview>(`/api/workspaces/${workspace}/repositories/preview`, {
    method: "POST", json: { full_name: fullName },
  });
}

export async function connectReviewedRepository(workspace: string, preview: RepositoryPreview, commands: ReviewedCommands, request: Request) {
  const lines = (value: string) => value.split("\n").map((line) => line.trim()).filter(Boolean);
  return request<Repo>(`/api/workspaces/${workspace}/repositories`, {
    method: "POST",
    json: {
      full_name: preview.full_name,
      config: {
        setup: lines(commands.setup), checks: lines(commands.checks),
        qa: { start: commands.start.trim() },
        release: { mode: commands.deploys ? "on_merge" : null },
      },
    },
  });
}

export function commandsFromRepository(repo: RepositoryPreview): ReviewedCommands {
  return {
    setup: (repo.config.setup || []).join("\n"),
    checks: (repo.config.checks || []).join("\n"),
    start: repo.config.qa?.start || "",
    deploys: repo.config.release?.mode === "on_merge",
  };
}

/** Stable, server-detected data only; never infer a stack from editable commands. */
export function repositoryStack(repo: RepositoryPreview): string[] {
  return [...new Set(repo.config.detected || [])].sort();
}

export function reviewedConfig(config: Repo["config"], commands: ReviewedCommands) {
  const lines = (value: string) => value.split("\n").map((line) => line.trim()).filter(Boolean);
  // The onboarding command review does not own scheduled release configuration.
  const release: NonNullable<Repo["config"]["release"]> = { ...config.release };
  if (config.release?.mode !== "train") {
    if (commands.deploys) release.mode = "on_merge";
    else delete release.mode;
  }
  return {
    ...config,
    setup: lines(commands.setup), checks: lines(commands.checks),
    qa: { ...config.qa, start: commands.start.trim() },
    release,
  };
}

/** Reconcile a lost POST response (or another tab's connect) before retrying.
 * Existing repositories are updated, never connected again. Merge against their
 * latest config because PATCH deliberately replaces nested settings objects.
 */
export async function saveRepositoryReview(
  workspace: string, preview: RepositoryPreview, commands: ReviewedCommands,
  existingId: string | undefined, request: Request,
): Promise<Repo> {
  const base = `/api/workspaces/${encodeURIComponent(workspace)}/repositories`;
  const repos = await request<Repo[]>(base);
  const existing = repos.find((repo) => repo.id === existingId || repo.full_name.toLowerCase() === preview.full_name.toLowerCase());
  if (existing) {
    return request<Repo>(`${base}/${encodeURIComponent(existing.id)}`, {
      method: "PATCH", json: { config: reviewedConfig(existing.config, commands) },
    });
  }
  if (existingId) throw new Error("This repository was disconnected. Reload before connecting it again.");
  return connectReviewedRepository(workspace, preview, commands, request);
}
