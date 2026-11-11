import type { Settings } from "./api";
export type Autonomy = Settings["autonomy"];
export interface LearningPolicy {
  learning: boolean;
  eligible: boolean;
  applied: boolean;
  first_completed_outcome_id: string | null;
  before: Autonomy;
  after: Autonomy;
  changes: { policy: keyof Autonomy; before: "auto" | "approval" | "github"; after: "auto" | "approval" | "github" }[];
}
export const policyLabel = (value: "auto" | "approval" | "github") => value === "auto" ? "Automatic" : value === "github" ? "People merge on GitHub" : "Approve first";
export function policyPatch<K extends keyof Autonomy>(policy: LearningPolicy | undefined, key: K, value: Autonomy[K]) {
  // Never submit a fallback policy while the saved preset is unknown.
  if (!policy) return undefined;
  return { autonomy: { [key]: value } };
}
