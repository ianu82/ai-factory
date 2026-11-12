/** A merge approval's facts (`metadata.merge`, see pipeline/delivery.py) as the rows its decision card shows. */
import type { MergeFacts } from "./api";

export interface FactRow { label: string; text: string }

const count = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

export function mergeFactRows(m: MergeFacts): FactRow[] {
  const rows: FactRow[] = [];
  if (m.change) {
    const { files, additions, deletions, paths } = m.change;
    const more = files - paths.length;
    const listed = [...paths, ...(more > 0 ? [`and ${more} more`] : [])];
    const size = `${count(files, "file")}, +${additions} −${deletions}`;
    rows.push({ label: "Change", text: listed.length ? `${size} · ${listed.join(", ")}` : size });
  }
  if (m.verifier) {
    const { verdict, summary } = m.verifier;
    rows.push({ label: "Verifier", text: `${verdict || "no verdict"}${summary ? ` — ${summary}` : ""}` });
  }
  const checks: string[] = [];
  if (m.checks) {
    const pre = m.checks.preexisting;
    checks.push(`${m.checks.passed} of ${m.checks.total} repository checks passed`
      + (pre ? ` (${count(pre, "failure")} also on the base commit)` : ""));
  }
  if (m.ci) checks.push(`CI ${m.ci.state} on ${count(m.ci.checks, "check")}`);
  if (checks.length) rows.push({ label: "Checks", text: checks.join("; ") });
  if (m.review) {
    const r = m.review;
    const who = r.state === "reviewed" ? `reviewed by ${r.reviewers.join(", ")}` : "no review of this head";
    const open = `${count(r.open, "finding")} open${r.blocking ? ` (${r.blocking} blocking)` : ""}`;
    rows.push({ label: "Review", text: `${who}; ${open}; round ${r.round}` });
  }
  if (m.cost) {
    const attempts = count(m.cost.attempts, "charged attempt");
    const unpriced = m.cost.unpriced || 0;  // sessions with no recorded cost: the sum is a floor
    rows.push({ label: "Cost", text: `${unpriced ? "at least " : ""}$${m.cost.usd.toFixed(2)} so far · ${attempts}`
      + (unpriced ? ` · ${count(unpriced, "session")} with no recorded cost` : "") });
  }
  return rows;
}

/** The decision's text without the lines that repeat its facts, for a card that shows the facts as rows. */
export function withoutFacts(body: string, m: MergeFacts | undefined): string {
  return m?.text ? body.replace(`\n${m.text}`, "") : body;
}
