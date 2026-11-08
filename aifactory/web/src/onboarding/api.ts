import { api, type Workspace } from "../api";

export const ONBOARDING_STEPS = ["models", "github", "repositories", "autonomy", "team", "outcome"] as const;
export type OnboardingStep = (typeof ONBOARDING_STEPS)[number];

export interface OnboardingState {
  current_step: OnboardingStep;
  completed_steps: OnboardingStep[];
  deferred: boolean;
  first_outcome_id: string | null;
  /** Operational readiness, independent of acknowledgments and first outcome. */
  ready: boolean;
  /** Missing operational prerequisites, not unfinished tutorial steps. */
  missing_steps: OnboardingStep[];
  /** Configured and usable by runtime, but validation has not proved capability. */
  validation_uncertain_steps: OnboardingStep[];
}

export type OnboardingAction =
  | { action: "navigate"; step: OnboardingStep }
  | { action: "defer"; deferred: boolean }
  | { action: "complete"; step: "models" | "github" | "repositories" }
  | { action: "complete"; step: "autonomy" | "team"; acknowledged: true }
  | { action: "complete"; step: "outcome"; first_outcome_id: string };

export const onboardingQueryKey = (workspaceId: string) => ["onboarding", workspaceId] as const;
const path = (workspaceId: string) => `/api/workspaces/${encodeURIComponent(workspaceId)}/onboarding`;

export const getOnboarding = (workspaceId: string) => api<OnboardingState>(path(workspaceId));
export const patchOnboarding = (workspaceId: string, action: OnboardingAction) =>
  api<OnboardingState>(path(workspaceId), { method: "PATCH", json: action });

/** Shared contract for the step views; credentials use the existing credential API. */
export interface OnboardingStepProps {
  workspace: Workspace;
  progress: OnboardingState;
  readOnly: boolean;
  onNavigate: (step: OnboardingStep) => Promise<void>;
  onComplete: (action: Extract<OnboardingAction, { action: "complete" }>) => Promise<void>;
  onDefer: () => Promise<void>;
}
