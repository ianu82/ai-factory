/** Public release configuration. Credentials and internal secret references never belong here. */
export const WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"] as const;
export type Weekday = typeof WEEKDAYS[number];
export interface ReleaseSchedule { weekdays: Weekday[]; time: string; timezone: string }
export interface ReleaseCut {
  mode: "tag" | "version_pr";
  tag_prefix: string;
  initial_version: string;
  increment: "patch";
  version_file?: { path: string; format: "plain" | "json"; key?: string } | null;
}
export interface WorkflowTarget {
  kind: "workflow";
  workflow: string;
  inputs?: Record<string, string>;
  correlation_input: string;
}
export type WebhookCompletion =
  | { mode: "synchronous"; success_statuses?: number[] }
  | { mode: "poll"; status_url: string; handle_key: string; state_key: string;
      success_values: string[]; failure_values: string[]; interval_seconds?: number; timeout_seconds?: number };
export interface WebhookTarget {
  kind: "webhook";
  endpoint: string;
  secret_configured?: boolean;
  timeout_seconds?: number;
  completion: WebhookCompletion;
}
export type DeployTarget = WorkflowTarget | WebhookTarget;
export interface SmokeDefinition { commands?: string[]; urls?: string[]; timeout_seconds?: number }
export interface TrainReleaseConfig {
  mode: "train";
  schedule?: ReleaseSchedule;
  cut?: ReleaseCut;
  staging: DeployTarget;
  production: DeployTarget;
  rollback: DeployTarget;
  staging_smoke: SmokeDefinition;
  production_smoke: SmokeDefinition;
  approval?: "inherit" | "approval" | "auto";
}
export interface ContinuousReleaseConfig {
  mode?: "continuous" | "on_merge" | null;
  health_url?: string | null;
  expect_text?: string;
  [key: string]: unknown;
}
export type RepositoryReleaseConfig = TrainReleaseConfig | ContinuousReleaseConfig;

export type TargetDraft = Omit<WorkflowTarget, "inputs"> & { inputs: string } | WebhookTarget;
export interface SmokeDraft { commands: string; urls: string; timeout_seconds: number }
export interface ReleaseDraft {
  mode: "" | "continuous" | "on_merge" | "train";
  health_url: string;
  schedule: ReleaseSchedule;
  cut: ReleaseCut;
  staging: TargetDraft;
  production: TargetDraft;
  rollback: TargetDraft;
  staging_smoke: SmokeDraft;
  production_smoke: SmokeDraft;
  approval: "inherit" | "approval" | "auto";
}

export function newWorkflow(): TargetDraft {
  return { kind: "workflow", workflow: "", correlation_input: "release_key", inputs: "{}" };
}
export function newWebhook(): WebhookTarget {
  return { kind: "webhook", endpoint: "", timeout_seconds: 30,
    completion: { mode: "synchronous", success_statuses: [200, 204] } };
}
export function releaseDraft(value?: RepositoryReleaseConfig | null): ReleaseDraft {
  const train = value?.mode === "train" ? value : undefined;
  const target = (v?: DeployTarget): TargetDraft => !v ? newWorkflow() : v.kind === "workflow"
    ? { ...v, inputs: JSON.stringify(v.inputs ?? {}, null, 2) } : structuredClone(v);
  const smoke = (v?: SmokeDefinition): SmokeDraft => ({ commands: (v?.commands ?? []).join("\n"),
    urls: (v?.urls ?? []).join("\n"), timeout_seconds: v?.timeout_seconds ?? 60 });
  return {
    mode: value?.mode ?? "", health_url: value?.mode !== "train" ? value?.health_url ?? "" : "",
    schedule: structuredClone(train?.schedule ?? { weekdays: ["mon", "tue", "wed", "thu", "fri"], time: "09:00", timezone: "UTC" }),
    cut: structuredClone(train?.cut ?? { mode: "tag", tag_prefix: "v", initial_version: "0.1.0", increment: "patch" }),
    staging: target(train?.staging), production: target(train?.production), rollback: target(train?.rollback),
    staging_smoke: smoke(train?.staging_smoke), production_smoke: smoke(train?.production_smoke),
    approval: train?.approval ?? "inherit",
  };
}

function requireConfig(ok: unknown, message: string): asserts ok {
  if (!ok) throw new Error(message);
}
function name(value: string, label: string, max = 200) {
  requireConfig(typeof value === "string" && !!value.trim() && value.length <= max, `${label} is required (up to ${max} characters).`);
}
function integer(value: number | undefined, min: number, max: number, label: string) {
  requireConfig(value === undefined || Number.isInteger(value) && value >= min && value <= max,
    `${label} must be a whole number from ${min} to ${max}.`);
}
function https(value: string, label: string) {
  let url: URL;
  try { url = new URL(value); } catch { throw new Error(`${label}: use an HTTPS URL without credentials, query or fragment.`); }
  requireConfig(value.length <= 2000 && url.protocol === "https:" && url.hostname && !url.username && !url.password &&
    !/[\s?#]/.test(value), `${label}: use an HTTPS URL without credentials, query or fragment.`);
}
export function parseWorkflowInputs(text: string, correlation: string): Record<string, string> {
  let parsed: unknown;
  // Never include submitted JSON in errors; it may contain accidentally pasted credentials.
  try { parsed = JSON.parse(text); } catch { throw new Error("Workflow inputs must be valid JSON (an object of string values)."); }
  requireConfig(parsed !== null && typeof parsed === "object" && !Array.isArray(parsed), "Workflow inputs must be a JSON object of string values.");
  const entries = Object.entries(parsed);
  requireConfig(entries.length <= 30, "Workflow inputs allow at most 30 entries.");
  for (const [key, value] of entries) {
    name(key, "Workflow input name");
    requireConfig(typeof value === "string" && value.length <= 2000, "Workflow input values must be strings of at most 2000 characters.");
    requireConfig(key !== correlation, "The correlation input is reserved; remove it from workflow inputs.");
    requireConfig(!/^(secret|secretref|webhooksecret|token|password|authorization|apikey|accesstoken|ciphertext)$/.test(key.toLowerCase().replace(/[^a-z0-9]/g, "")),
      "Keep credentials in CI or the separate webhook secret action, not workflow inputs.");
  }
  return parsed as Record<string, string>;
}

export function parseTarget(draft: TargetDraft, label: string, secretConfigured: boolean): DeployTarget {
  try {
    if (draft.kind === "workflow") {
      name(draft.workflow, "Workflow"); name(draft.correlation_input, "Correlation input");
      return { ...draft, inputs: parseWorkflowInputs(draft.inputs, draft.correlation_input) };
    }
    https(draft.endpoint, "Webhook endpoint");
    requireConfig(secretConfigured, "Save a webhook secret before saving webhook targets.");
    integer(draft.timeout_seconds, 1, 120, "Webhook request timeout");
    const c = draft.completion;
    if (c.mode === "synchronous") {
      const statuses = c.success_statuses ?? [200, 204];
      requireConfig(statuses.length >= 1 && statuses.length <= 10 && statuses.every((n) => Number.isInteger(n) && n >= 200 && n <= 299 && n !== 202),
        "Synchronous success statuses must contain 1–10 final 2xx codes, never 202.");
    } else {
      https(c.status_url, "Polling status URL"); name(c.handle_key, "Handle key"); name(c.state_key, "State key");
      for (const [label, values] of [["Success", c.success_values], ["Failure", c.failure_values]] as const) {
        requireConfig(values.length >= 1 && values.length <= 20, `${label} states require 1–20 values.`);
        values.forEach((v) => name(v, `${label} state`));
      }
      requireConfig(!c.success_values.some((v) => c.failure_values.includes(v)), "Polling success and failure states must not overlap.");
      integer(c.interval_seconds, 1, 300, "Polling interval"); integer(c.timeout_seconds, 1, 86400, "Polling timeout");
    }
    const { secret_configured: _configured, ...target } = draft;
    return structuredClone(target);
  } catch (err) { throw new Error(`${label}: ${(err as Error).message}`); }
}

export function releaseLines(value: string): string[] {
  return value.split("\n").map((line) => line.trim()).filter(Boolean);
}
function parseSmoke(draft: SmokeDraft, label: string): SmokeDefinition {
  const commands = releaseLines(draft.commands), urls = releaseLines(draft.urls);
  requireConfig(commands.length + urls.length > 0, `${label}: enter at least one smoke command or URL.`);
  requireConfig(commands.length <= 20 && urls.length <= 20 && commands.every((c) => c.length <= 4000), `${label}: up to 20 commands (4000 characters each) and 20 URLs are allowed.`);
  urls.forEach((url) => https(url, `${label} smoke URL`)); integer(draft.timeout_seconds, 1, 600, `${label} smoke timeout`);
  return { commands, urls, timeout_seconds: draft.timeout_seconds };
}
/** Serialize only release-owned fields. Legacy continuous extras are retained verbatim. */
export function serializeRelease(draft: ReleaseDraft, previous: RepositoryReleaseConfig | null | undefined, secretConfigured: boolean): RepositoryReleaseConfig {
  if (draft.mode !== "train") {
    const base = previous?.mode !== "train" ? previous ?? {} : {};
    return { ...base, mode: draft.mode || null, health_url: draft.health_url || null };
  }
  const s = draft.schedule, cut = draft.cut;
  requireConfig(s.weekdays.length > 0 && s.weekdays.every((d) => WEEKDAYS.includes(d)) && new Set(s.weekdays).size === s.weekdays.length,
    "Schedule: select at least one weekday, without duplicates.");
  requireConfig(/^(?:[01][0-9]|2[0-3]):[0-5][0-9]$/.test(s.time), "Schedule: use local HH:MM time.");
  try { new Intl.DateTimeFormat("en", { timeZone: s.timezone }).format(); } catch { throw new Error("Schedule: use a valid IANA timezone, such as Europe/London."); }
  requireConfig(!!s.timezone && !/^[+-]/.test(s.timezone), "Schedule: use an IANA timezone, such as Europe/London.");
  requireConfig(/^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$/.test(cut.initial_version), "Initial version must be major.minor.patch, without a prefix.");
  requireConfig(/^[A-Za-z0-9_-]{0,40}$/.test(cut.tag_prefix), "Tag prefix allows up to 40 letters, digits, underscores or hyphens.");
  requireConfig(cut.increment === "patch", "Only patch version increments are supported.");
  if (cut.mode === "version_pr") {
    const file = cut.version_file;
    requireConfig(file, "Version PR requires a version file."); name(file.path, "Version file path");
    requireConfig(!/[\\:\x00]/.test(file.path) && !file.path.split("/").some((p) => ["", ".", "..", ".git"].includes(p)), "Version file must be a safe repository-relative path.");
    if (file.format === "json") name(file.key ?? "", "Literal JSON version key");
    else requireConfig(file.key === undefined, "Plain version files must not have a JSON key.");
  } else requireConfig(!cut.version_file, "Tag cutting must not include a version file.");
  return { mode: "train", schedule: structuredClone(s), cut: structuredClone(cut), approval: draft.approval,
    staging: parseTarget(draft.staging, "Staging", secretConfigured), production: parseTarget(draft.production, "Production", secretConfigured),
    rollback: parseTarget(draft.rollback, "Rollback", secretConfigured),
    staging_smoke: parseSmoke(draft.staging_smoke, "Staging"), production_smoke: parseSmoke(draft.production_smoke, "Production") };
}

/** General settings must not send a stale public release object back to the server. */
export function generalRepositoryPatch<T extends Record<string, unknown>>(settings: T): Omit<T, "release"> {
  const { release: _release, ...patch } = settings;
  return patch;
}
