import type { RepositoryReleaseConfig } from "./releaseTypes";

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export async function apiResponse(path: string, init: RequestInit & { json?: unknown } = {}): Promise<Response> {
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
  return res;
}

export async function api<T = any>(path: string, init: RequestInit & { json?: unknown } = {}): Promise<T> {
  const res = await apiResponse(path, init);
  if (res.status === 204) return undefined as T;
  return res.json();
}

/** JSON uploads keep the existing API/error contract, with actual transport progress. */
export function uploadJson<T>(path: string, json: unknown, signal: AbortSignal, onProgress: (loaded: number, total: number) => void): Promise<T> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const abort = () => xhr.abort();
    const clean = () => signal.removeEventListener("abort", abort);
    xhr.open("POST", path);
    xhr.withCredentials = true;
    xhr.setRequestHeader("Content-Type", "application/json");
    xhr.upload.onprogress = (e) => { if (e.lengthComputable) onProgress(e.loaded, e.total); };
    xhr.onerror = () => { clean(); reject(new ApiError(0, "Upload failed. Check your connection and retry.")); };
    xhr.onabort = () => { clean(); reject(new DOMException("Upload cancelled", "AbortError")); };
    xhr.onload = () => {
      clean();
      let body: any;
      try { body = JSON.parse(xhr.responseText); } catch { /* handled below */ }
      if (xhr.status >= 200 && xhr.status < 300) {
        if (body === undefined) reject(new ApiError(xhr.status, "Invalid upload response"));
        else resolve(body as T);
      } else {
        const detail = body?.detail;
        const message = typeof detail === "string" ? detail : Array.isArray(detail) ? detail.map((d: any) => d.msg).join("; ") : xhr.statusText;
        reject(new ApiError(xhr.status, message || `Request failed (${xhr.status})`));
      }
    };
    if (signal.aborted) { reject(new DOMException("Upload cancelled", "AbortError")); return; }
    signal.addEventListener("abort", abort, { once: true });
    xhr.send(JSON.stringify(json));
  });
}

export type Role = "owner" | "admin" | "member" | "viewer";

export type LinearStage = "queued" | "building" | "verifying" | "merging" | "done" | "blocked";

export interface LinearConfig {
  enabled: boolean;
  team_id: string | null;
  intake_label: string;
  repo_ids: string[];
  states: Record<LinearStage, string | null>;
}

export interface LinearTeam {
  id: string;
  name: string;
  key?: string;
}

export interface LinearState {
  id: string;
  name: string;
  type: string;
}

export interface LinearDiscovery<T> {
  items: T[];
}

export interface GitHubPermission {
  permission: string;
  label: string;
  required: "read" | "write";
}

export interface CredentialsResponse {
  github_permissions: GitHubPermission[];
  items: Credential[];
  server_fallbacks: Record<string, boolean>;
}

export interface Settings {
  learning?: { status: "learning" | "applied"; preset: Settings["autonomy"] };
  provider: "mindshub" | "openai";
  decision_judges: DecisionJudgeMode;
  models: { default?: string; planner?: string; plan_reviewer?: string; builder?: string; verifier?: string; judge?: string };
  harness: { builder: string; verifier: string; planner: string };
  concurrency: number;
  planning_concurrency?: number;
  max_attempts: number;
  auto_retry_rounds?: number;
  autonomy: { plan: "auto" | "approval"; merge: "auto" | "approval"; release: "auto" | "approval" };
  /** task_usd and limits.task_attempts_total: a task's lifetime limit; 0 or missing: no limit. */
  budgets: { run_usd: number; outcome_usd: number; run_tokens: number; task_usd?: number | null };
  limits: {
    open_outcomes?: number | null; task_attempts_total?: number | null; [key: string]: number | null | undefined;
  };
  reasoning_effort?: string;
  linear?: LinearConfig;
  mind?: { daily_budget_usd: number };
  supervisor?: SupervisorPolicy;
  /** Slack and email aren't available yet and are always off; alerts show in the Inbox. The webhook is write-only:
   * set it with webhook_url ("" clears it); the workspace's alert_webhook says whether it is set and its host. */
  alerts?: { after_minutes: number; slack: boolean; email: boolean; stopped_minutes?: number; webhook_url?: string };
  /** The stand-in for people while they're away (pipeline/director.py). Off by default. */
  director?: DirectorSettings;
  /** The workspace's review policy (aifactory/review.py); a repository's own review config overrides it key by key. */
  review?: ReviewPolicy;
  /** While a provider hold is open, how often the provider is tested (0: never). */
  holds?: { test_minutes: number };
  lead?: { enabled: boolean; run_usd: number; turns: number };
}

export interface ReviewPolicy {
  address: "unresolved" | "changes_requested" | "off";
  reviewers: "people" | "bots" | "all";
  trust: "collaborators" | "anyone";
  block_on: "p1" | "all" | "none";
  on_timeout: "proceed" | "ask";
  wait_seconds: number;
  max_rounds: number;
  /** The comment that asks for a review of each head after the pull request's first; "" never asks. */
  request: string;
  ignore: string[];
}

export interface DirectorSettings {
  mode: "off" | "on";
  run_usd: number;
  daily_usd: number;
  max_outcome_usd: number;
  after_minutes: number;
  /** Hours what it left to people waits for them before it decides that itself; 0: always people's. */
  decide_after_hours: number;
}

/** Why the model provider refused, on a provider hold (pipeline/holds.py). */
export interface HoldDetails {
  cause: "credit" | "credential" | "no_provider" | "limit" | "rate_limit" | "overloaded" | "unknown";
  label: string;
  advice: string;
  source: string | null;
  error: string;
  /** While the hold is open: how often the Factory tests the provider (0: never), and when it last did. */
  tests_every_minutes?: number;
  last_tested_at?: string | null;
}

export interface SupervisorPolicy {
  mode: "off" | "observe" | "fix";
  interval_minutes: number;
  stuck_minutes: number;
  unaccepted_issue_hours: number;
}

/** Open Inbox decision enriched with the latest sweep's classification, when available. */
export interface SupervisorFinding extends Decision {
  finding_kind: string;
  subject: string | null;
  confidence: "observed" | "inferred" | null;
}

export interface SupervisorStatus {
  policy: SupervisorPolicy;
  last_sweep_at: string | null;
  /** Counts from the latest sweep, not the number of outstanding Inbox decisions. */
  counts: Record<string, number>;
  findings: SupervisorFinding[];
  next_offset: number | null;
}

export interface ModelCheckResult {
  provider: string | null;
  roles: Record<"planner" | "plan_reviewer" | "builder" | "verifier", {
    model: string;
    status: "supported" | "unsupported" | "unknown";
    explanation: string;
  }>;
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
  /** The alert webhook is a secret: only whether it is set, and its host. */
  alert_webhook?: { set: boolean; host: string };
}

export interface Me {
  user: { id: string; email: string | null; name: string };
  workspaces: Workspace[];
  organizations: { id: string; name: string }[];
}

export interface Meta {
  capabilities?: { reverify?: boolean };
  auth: { oidc: boolean; api_key: boolean; dev: boolean };
  harnesses: { name: string; label: string; available: boolean; detail: string }[];
  sandbox: { isolation: string };
  version: string;
}

export interface Repo {
  id: string;
  steward_policy_invalid?: boolean;
  full_name: string;
  default_branch: string;
  description: string;
  config: {
    [key: string]: unknown;
    setup?: string[];
    checks?: string[];
    smoke?: string[];
    protected_paths?: string[];
    context?: string;
    detected?: string[];
    qa?: { start?: string; [key: string]: unknown };
    start_detection?: { available: boolean; reason: string };
    merge?: { mode?: string; method?: string; revalidate?: boolean; reverify?: "overlap" | "always" | "never" };
    release?: RepositoryReleaseConfig | null;
    require_ci?: boolean | "auto";
    require_trusted_issue_author?: boolean;
    steward?: { mode?: "off" | "propose" | "fix"; fix_minutes?: number };
    review?: { mode?: "off" | "all" | "labelled"; allow_untrusted_local?: boolean; [key: string]: unknown };
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
  linear_issue: { id: string; identifier: string | null; url: string } | null;
  summary: string;
  blocked_reason: string;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  progress: { done: number; total: number; started?: number; active?: number };
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
  /** The unfinished tasks that wait for this one, directly or through others: what cancelling it strands. */
  dependents?: { id: string; key: string; title: string }[];
}

export interface ScorerResult {
  id: string; scorer_id: string; definition_version: string; status: string;
  classification: string | null; score: number | null; passed: boolean | null;
  reasoning: string; error: string;
  cost_usd: number; cost_known: boolean;
}

export interface Run {
  scores?: Omit<ScorerResult, "reasoning" | "error">[];
  score_summary?: { count: number; cost_usd: number; cost_known: boolean };
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

/** Stuck work or a repeated failure the watch found (pipeline/watch.py), shown at the top of the Inbox. */
export interface Alert {
  id: string;
  kind: "overdue" | "unasked" | "stalled" | "quiet" | "repeated" | "stopped";
  title: string;
  body: string;
  since: string;
  opened_at: string | null;
  outcome_id: string | null;
  decision_id: string | null;
  outcome_number: number | null;
  outcome_title: string;
  task_key: string | null;
}

export type DecisionJudgeMode = "off" | "suggest" | "decide";
export type DecisionJudgeVerdict = "decide" | "send_back" | "hold";
export interface DecisionJudgeCitation {
  decision_id: string;
  passage: string;
}
export interface DecisionJudgeReport {
  verdict: DecisionJudgeVerdict;
  resolution: string;
  answer: string;
  summary: string;
  citations: DecisionJudgeCitation[];
  closest_decisions: string[];
}
export interface DecisionReview {
  id: string;
  state: string;
  question_revision: number;
  question_fingerprint: string;
  run_id: string | null;
  mode: DecisionJudgeMode;
  proposed_verdict: DecisionJudgeVerdict | null;
  effective_verdict: DecisionJudgeVerdict | null;
  answer: string;
  resolution: string;
  summary: string;
  citations: DecisionJudgeCitation[];
  closest_decisions: string[];
  report: Partial<DecisionJudgeReport>;
  correction_decision_id: string | null;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface Decision {
  actionable?: boolean;
  supersedes?: string | null;
  correction?: { id: string; answer: string; resolved_by_name: string; resolved_at: string; attribution: { reason?: string } } | null;
  judge: DecisionReview | null;
  id: string;
  kind: string;
  title: string;
  body: string;
  options: DecisionOption[];
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
  lead_note?: { text: string; created_at: string; run_id?: string | null } | null;
  director_note?: { text: string; created_at: string } | null;
  hold?: HoldDetails;
  merge?: MergeFacts;
  /** Inbox only: how long it has waited, and the watch's alert when it has waited too long. */
  waited_minutes?: number;
  overdue?: { alert_id: string; title: string } | null;
  /** Inbox only: the same question about different work shares this key; null is never grouped. */
  question?: string | null;
  /** The Supervisor found its task stuck. */
  stuck?: { since?: string | null; status?: string | null; flagged_at?: string };
}

/** One answer to a decision; `effect` says what choosing it does (absent on older decisions). */
export interface DecisionOption { value: string; label: string; effect?: string }

/** What a merge approval records for a person (pipeline/delivery.py TaskRunner._merge_facts). */
export interface MergeFacts {
  pr: number | null;
  head: string;
  change?: { files: number; additions: number; deletions: number; paths: string[] };
  verifier?: { verdict: string; summary: string };
  checks?: { passed: number; total: number; preexisting: number };
  ci?: { state: string; checks: number };
  review?: { state: string; reviewers: string[]; round: number; open: number; blocking: number };
  cost?: { usd: number; attempts: number; unpriced?: number };
  asked?: { because: string; text: string };
  /** The same facts as lines of the decision's text. */
  text?: string;
}

/** An outcome as a conversation (GET …/outcomes/{id}/conversation). */
export interface ConversationStep {
  at: string;
  who: string;
  text: string;
  level: string;
  event: string;
  run_id?: string | null;
  url?: string;
  cost_usd?: number;
}

interface StepGroup {
  steps: ConversationStep[];
  steps_omitted: number;
}

export type ConversationItem =
  | { kind: "request"; at: string; title: string; brief: string; acceptance: string[]; from: string; by: string }
  | ({ kind: "lead"; id: string; at: string; text: string; task_key: string | null; update: boolean; decision_id: string | null } & StepGroup)
  | { kind: "lead_guidance"; id: string; at: string; text: string; task_key: string | null; delivered: boolean }
  | { kind: "person"; id: string; at: string; text: string; task_key: string | null; author: string; operator: boolean; delivered: boolean }
  | ({ kind: "factory"; id: string; at: string; text: string; task_key: string | null; digest?: boolean }
    & Partial<StepGroup>)
  | { kind: "decision"; at: string; decision: Decision }
  | ({ kind: "working"; at: string } & StepGroup);

export interface Conversation {
  items: ConversationItem[];
  /** The outcome's status now: a Mind conversation's copy of it is as old as the thread's last load. */
  status: string;
  /** Set when a Mind conversation became this outcome; thread_id only for that conversation's owner. */
  started_in_mind: { thread_id: string | null } | null;
  lead: { model: string | null; running: boolean; last_active: string | null; cost_usd: number; cost_known: boolean };
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
  author_kind: "user" | "factory" | "lead";
  author: string;
  body: string;
  delivered: boolean;
  task_id: string | null;
  created_at: string;
}

export interface PlanReview {
  verdict: "approve" | "revise" | "escalate" | "error";
  summary?: string;
  model?: string;
  findings?: { severity: string; task?: string; description: string }[];
}

export interface Plan {
  id: string;
  version: number;
  status: string;
  summary: string;
  content: { summary?: string; tasks?: any[]; risks?: string[]; questions?: any[]; review?: PlanReview };
  approved_by: string | null;
  approved_at: string | null;
  created_at: string;
}

export interface OutcomeDetail {
  guidance: { template: "readme-contributing"; repo_id: string } | null;
  attachments: Artifact[];
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
  updated_at?: string;
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
    acceptance_reason?: string;
    workstream_assignment_reason?: string;
    proposed_workstream_id?: string | null;
    workstream_labels?: string[];
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

// Mind wire projection. No SDK history, tool transcripts or credentials belong here.
export interface MindContext {
  workspace_id: string; kind: string; id: string; outcome_id?: string; label: string;
}
export interface MindCitation { label: string; url: string }
export interface MindCard {
  id: string; revision: number; kind: string; workspace_id: string; title: string;
  fields: Record<string, string>;
  status: "proposed" | "dismissed" | "confirmed" | "stale" | "executing";
  effect?: string;
  result?: { text: string; citations?: MindCitation[] };
}
/** One thing Mind did during a turn: a Factory read or a proposal, by tool name. */
export interface MindStep { name: string; done: boolean; citations?: number }
export interface MindMessage {
  id: string; role: "user" | "assistant"; content: string;
  citations?: MindCitation[]; cards?: MindCard[];
  /** The turn's steps; durationMs only when this browser timed the turn. */
  steps?: MindStep[]; durationMs?: number; failed?: boolean;
}
/** The outcome a Mind conversation became when its owner confirmed a card that created one. */
export interface MindOutcomeLink { id: string; number: number; title: string; status: string; workspace_id: string }
export interface MindThread {
  id: string; title: string; workspace_id: string;
  cost_usd: number | null; cost_known: boolean; updated_at?: string;
  outcome?: MindOutcomeLink | null;
}
export interface MindStatus {
  model: string | null; available: boolean; reason?: string;
  budget_state: "ok" | "budget_exceeded" | "pending_accounting";
}
export interface MindHistory { thread: MindThread; messages: MindMessage[]; status: MindStatus }
export type MindEvent =
  | { type: "text_delta"; text: string }
  | { type: "tool_start"; name: string }
  | { type: "tool_result"; name: string; citations?: MindCitation[] }
  | { type: "action_card"; card: MindCard }
  | { type: "status"; status: MindStatus }
  /** A failed turn carries no fresh status (no second readiness check), only the budget state its failure implies. */
  | { type: "done"; message: MindMessage; thread: MindThread; status?: MindStatus; budget?: MindStatus["budget_state"] }
  | { type: "error"; message: string; reason?: string };

/** T1/T3 HTTP shapes; the dock uses a small presentation projection above. */
export interface MindWireCitation { label: string; href: string }
export interface MindWireCard {
  id: string; revision: number; kind: string;
  workspace: { id: string; name: string };
  object: MindWireCitation; fields: Record<string, string>; effect: string;
  status: "pending" | "dismissed" | "executing" | "succeeded" | "stale";
  result?: { ok: boolean; reason?: string; partial?: boolean; incomplete?: unknown; [key: string]: unknown };
}
export interface MindWireThread {
  id: string; title: string; billing_workspace_id: string; model_id: string | null;
  status: string; cost: { usd: number | null; known: boolean }; updated_at?: string;
  outcome?: MindOutcomeLink | null;
}
export interface MindWireMessage {
  id: string; role: "user" | "assistant"; text: string;
  citations: MindWireCitation[]; action_cards: MindWireCard[]; status: string;
  steps?: { name: string }[];
}
export interface MindWireHistory { thread: MindWireThread; messages: MindWireMessage[] }
export interface MindWireStatus { status: "ready" | "unavailable"; model_id: string | null; reason?: string; accounting?: "pending" | "settled" }
export interface MindWireUsage {
  thread_cost: { usd: number | null; known: boolean };
  daily_spend: number | null; daily_limit: number;
}

/** The Message Log (messages.py): who told whom what, about which object. */
export interface MessageParty { role: string; label: string; for?: string }
export interface AgentMessage {
  id: number; at: string; kind: string; level: string;
  from: MessageParty; to: MessageParty;
  intent: "request" | "answer" | "propose" | "inform" | "handoff";
  says: string;
  about: { outcome: { id: string; number: number; title: string } | null; task: { id: string; key: string } | null; pr: number | null };
}
export interface MessagePage { items: AgentMessage[]; next_cursor: number | null }
