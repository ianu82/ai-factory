export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export async function api<T = any>(path: string, init: RequestInit & { json?: unknown } = {}): Promise<T> {
  const { json, ...rest } = init;
  const res = await fetch(path, {
    credentials: "same-origin",
    ...rest,
    headers: { ...(json !== undefined ? { "Content-Type": "application/json" } : {}), ...(rest.headers || {}) },
    body: json !== undefined ? JSON.stringify(json) : rest.body,
  });
  if (!res.ok) {
    let message = res.statusText;
    try {
      const body = await res.json();
      message = typeof body.detail === "string" ? body.detail : Array.isArray(body.detail) ? body.detail.map((d: any) => d.msg).join("; ") : message;
    } catch {
      /* not json */
    }
    throw new ApiError(res.status, message || `Request failed (${res.status})`);
  }
  if (res.status === 204) return undefined as T;
  return res.json();
}

export type Role = "owner" | "admin" | "member" | "viewer";

export interface Settings {
  provider: "mindshub" | "openai";
  models: { default?: string; planner?: string; builder?: string; verifier?: string };
  harness: { builder: string; verifier: string; planner: string };
  concurrency: number;
  max_attempts: number;
  autonomy: { plan: "auto" | "approval"; merge: "auto" | "approval"; release: "auto" | "approval" };
  budgets: { run_usd: number; outcome_usd: number; run_tokens: number };
  limits: Record<string, number>;
  reasoning_effort?: string;
}

export interface Workspace {
  id: string;
  name: string;
  slug: string;
  description: string;
  org_id: string;
  org_name: string;
  role: Role;
  paused: boolean;
  settings: Settings;
}

export interface Me {
  user: { id: string; email: string | null; name: string };
  workspaces: Workspace[];
  organizations: { id: string; name: string }[];
}

export interface Meta {
  auth: { oidc: boolean; api_key: boolean; dev: boolean };
  harnesses: { name: string; label: string; available: boolean; detail: string }[];
  sandbox: { isolation: string };
  version: string;
}

export interface Repo {
  id: string;
  full_name: string;
  default_branch: string;
  description: string;
  config: {
    setup?: string[];
    checks?: string[];
    smoke?: string[];
    protected_paths?: string[];
    context?: string;
    detected?: string[];
    merge?: { mode?: string; method?: string; revalidate?: boolean };
    release?: { mode?: string; health_url?: string; expect_text?: string };
    require_ci?: boolean;
  };
}

export interface Outcome {
  id: string;
  number: number;
  title: string;
  brief: string;
  acceptance: string[];
  repo_ids: string[];
  harness: string | null;
  status: string;
  priority: number;
  source: string;
  summary: string;
  blocked_reason: string;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  progress: { done: number; total: number };
  cost_usd: number;
  cost_known: boolean;
  open_decisions: number;
  prs: { number: number; url: string; task: string }[];
  track: { key: string; status: string; stage: string; evidence: Task["evidence"] }[];
  created_by_name?: string;
}

export interface Attempt {
  n: number;
  result: string;
  checks?: { command: string; ok: boolean; exit_code: number; duration: number }[];
  verdict?: VerifierReport;
  app_run?: {
    started: boolean;
    ready_seconds: number;
    pages: { path: string; status?: number; title?: string; error?: string }[];
    probes: { method: string; path: string; status?: number; ok?: boolean }[];
    screenshots: number;
    verifier_screenshots: number;
  } | null;
  detail?: string;
  error?: string;
  reason?: string;
}

export interface VerifierReport {
  verdict: "pass" | "fail";
  summary: string;
  criteria: { criterion: string; status: "pass" | "fail" | "unverified"; evidence: string }[];
  findings?: { severity: string; description: string; location?: string }[];
}

export interface Task {
  id: string;
  outcome_id: string;
  key: string;
  title: string;
  spec: string;
  acceptance: string[];
  repo: string;
  depends_on: string[];
  harness: string;
  verifier_harness: string;
  status: string;
  stage: string;
  attempt: number;
  branch: string;
  head_sha: string;
  pr_number: number | null;
  pr_url: string;
  merge_sha: string;
  summary: string;
  blocked_reason: string;
  evidence: {
    attempts?: Attempt[];
    ci?: { state: string; checks?: { name: string; conclusion: string; url: string }[] };
    merge?: { sha: string; pr: number; at: string };
    smoke?: { state: string; checks?: { command: string; ok: boolean }[] };
    release?: { state: string; url?: string; note?: string };
  };
  cost_usd: number;
  cost_known: boolean;
  tokens: number;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  updated_at: string;
}

export interface Run {
  id: string;
  task_id: string | null;
  outcome_id: string;
  role: string;
  harness: string;
  provider: string;
  model: string;
  attempt: number;
  status: string;
  turns: number;
  input_tokens: number;
  output_tokens: number;
  cost_usd: number;
  cost_known: boolean;
  error: string;
  started_at: string;
  finished_at: string | null;
  outcome_number?: number;
  outcome_title?: string;
  task_key?: string | null;
}

export interface Decision {
  id: string;
  kind: string;
  title: string;
  body: string;
  options: { value: string; label: string }[];
  status: string;
  resolution: string;
  comment: string;
  resolved_by: string | null;
  resolved_by_name?: string;
  resolved_at: string | null;
  reusable: boolean;
  scope: string;
  outcome_id: string | null;
  task_id: string | null;
  created_at: string;
  outcome_number?: number | null;
  outcome_title?: string;
  task_key?: string | null;
  pr_url?: string;
  pr_number?: number | null;
  priority?: number;
}

export interface FactoryEvent {
  id: number;
  outcome_id: string | null;
  task_id: string | null;
  run_id: string | null;
  actor: string;
  level: "info" | "success" | "warn" | "error";
  kind: string;
  message: string;
  data: Record<string, any>;
  created_at: string;
}

export interface Message {
  id: string;
  author_kind: "user" | "factory";
  author: string;
  body: string;
  delivered: boolean;
  task_id: string | null;
  created_at: string;
}

export interface Plan {
  id: string;
  version: number;
  status: string;
  summary: string;
  content: { summary?: string; tasks?: any[]; risks?: string[]; questions?: any[] };
  approved_by: string | null;
  approved_at: string | null;
  created_at: string;
}

export interface OutcomeDetail {
  outcome: Outcome;
  repositories: { id: string; full_name: string }[];
  plans: Plan[];
  tasks: Task[];
  runs: Run[];
  decisions: Decision[];
  messages: Message[];
  events: FactoryEvent[];
}

export interface Credential {
  kind: string;
  label: string;
  configured: boolean;
  status: string;
  hint?: string;
  detail?: string;
  tested_at?: string;
  metadata?: Record<string, any>;
}

export interface Signal {
  id: string;
  source: string;
  title: string;
  body: string;
  url: string;
  count: number;
  status: string;
  outcome_id: string | null;
  repo_id: string | null;
  data: {
    rule?: string;
    action?: { type: "settings" | "repo_config" | "team_note" | "review"; label?: string; note?: string; repo_id?: string; set?: Record<string, unknown> };
    evidence?: { label: string; detail?: string; outcome_id?: string; task_id?: string; url?: string }[];
  };
  first_seen_at: string;
  last_seen_at: string;
}

export interface Artifact {
  id: string;
  kind: string;
  name: string;
  task_id: string | null;
  run_id: string | null;
  data: Record<string, any>;
  size: number;
  created_at: string;
  content?: string;
}

export interface PullRow {
  repo: string;
  number: number;
  title: string;
  url: string;
  author: { login: string; avatar: string };
  draft: boolean;
  created_at: string;
  updated_at: string;
  merged_at: string | null;
  base: string;
  group: "ready" | "attention" | "checks" | "draft" | "merged";
  reason: string;
  detail: { additions?: number; deletions?: number; changed_files?: number; mergeable_state?: string };
  ci?: { state: string; total?: number; failing?: string[]; pending?: string[] };
  factory: { outcome_id: string; outcome_number: number | null; task_key: string; status: string; decision_id: string | null } | null;
}

export interface PullsResponse {
  items: PullRow[];
  counts: Partial<Record<PullRow["group"], number>>;
  errors: string[];
  notes?: string[];
  fetched_at: number;
  repositories?: string[];
}
