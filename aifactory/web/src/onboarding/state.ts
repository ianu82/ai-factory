import { useQuery } from "@tanstack/react-query";
import { getOnboarding, onboardingQueryKey, type OnboardingStep } from "./api";

export const STEP_LABELS: Record<OnboardingStep, string> = {
  models: "AI models",
  github: "GitHub",
  repositories: "Repositories",
  autonomy: "How autonomous",
  team: "Team",
  outcome: "First outcome",
};

export function useOnboarding(workspaceId: string) {
  return useQuery({
    queryKey: onboardingQueryKey(workspaceId),
    queryFn: () => getOnboarding(workspaceId),
    enabled: !!workspaceId,
  });
}

// Members cannot change workspace-wide progress. Keep their opt-out personal,
// including when storage is disabled, full, or inaccessible in a sandbox.
const deferredMembers = new Set<string>();
const memberKey = (userId: string, workspaceId: string) =>
  `factory:onboarding:deferred:${JSON.stringify([userId, workspaceId])}`;

export function memberHasDeferred(userId: string, workspaceId: string): boolean {
  const key = memberKey(userId, workspaceId);
  if (deferredMembers.has(key)) return true;
  try {
    return localStorage.getItem(key) === "1";
  } catch {
    return false;
  }
}

export function deferMember(userId: string, workspaceId: string): void {
  const key = memberKey(userId, workspaceId);
  deferredMembers.add(key);
  try {
    localStorage.setItem(key, "1");
  } catch {
    // In-memory deferral still permits Inbox and resume in this page session.
  }
}
