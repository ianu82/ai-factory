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
