export type Autonomy = Record<"plan" | "merge" | "release", "auto" | "approval">;
export interface LearningPolicy {
  learning: boolean;
  eligible: boolean;
  applied: boolean;
  first_completed_outcome_id: string | null;
  before: Autonomy;
  after: Autonomy;
  changes: { policy: keyof Autonomy; before: "auto" | "approval"; after: "auto" | "approval" }[];
}
export const policyLabel = (value: "auto" | "approval") => value === "auto" ? "Automatic" : "Approve first";
export function policyPatch(policy: LearningPolicy | undefined, key: keyof Autonomy, value: "auto" | "approval") {
  // Never submit a fallback policy while the saved preset is unknown.
  if (!policy) return undefined;
  return { autonomy: { [key]: value } };
}
