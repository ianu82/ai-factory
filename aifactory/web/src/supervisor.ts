const FINDING_LABELS: Record<string, string> = {
  stuck_task: "Stuck task",
  push_protection: "GitHub push protection",
  main_red: "Main CI failing",
  combined_break: "Combined merge breakage",
  unaccepted_issue: "Unaccepted issue",
  untriaged_smoke: "Untriaged smoke failure",
  unrelayed_review: "Review findings awaiting relay",
};

export function supervisorFindingLabel(kind: string): string {
  return FINDING_LABELS[kind] ?? (kind.replace(/_/g, " ") || "Unknown finding");
}

/** Never imply direct observation when the sweep only inferred a condition. */
export function supervisorConfidenceLabel(confidence: string | null | undefined): string {
  if (confidence === "inferred") return "suspected";
  if (confidence === "observed") return "observed";
  return "confidence unknown";
}
