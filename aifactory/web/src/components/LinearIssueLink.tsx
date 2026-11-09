import { type Outcome } from "../api";

export function LinearIssueLink({ issue }: { issue: Outcome["linear_issue"] }) {
  if (!issue?.identifier || !/^https?:\/\//i.test(issue.url)) return null;
  return <a className="linkish" href={issue.url} target="_blank" rel="noopener noreferrer" aria-label={`Open ${issue.identifier} in Linear`} onClick={(event) => event.stopPropagation()} onKeyDown={(event) => event.stopPropagation()}>{issue.identifier}</a>;
}
