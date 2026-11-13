import { useState } from "react";
import { DEFAULT_REVIEW, type ReviewPolicy } from "../api";
import type { ReviewPatch } from "../reviewPolicy";
import { useToast } from "../ui";

type ReviewField = "address" | "reviewers" | "ignore" | "wait_seconds" | "max_rounds";

const REVIEW_LABELS: Record<ReviewField, string> = {
  address: "Feedback to address", reviewers: "Counted reviewers", ignore: "Ignored logins",
  wait_seconds: "Wait for reviews (seconds)", max_rounds: "Maximum review rounds",
};

export function ReviewControls({ value, defaults, admin, supported, onSave }: {
  value: Partial<ReviewPolicy>; defaults?: ReviewPolicy; admin: boolean; supported: boolean;
  onSave: (patch: ReviewPatch) => Promise<void>;
}) {
  const [form, setForm] = useState<Partial<ReviewPolicy>>(value);
  const [ignoreText, setIgnoreText] = useState((value.ignore || defaults?.ignore || []).join(", "));
  const [busy, setBusy] = useState(false);
  const toast = useToast();
  const effective = { ...DEFAULT_REVIEW, ...defaults, ...form };
  const submit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!supported || !admin || busy) return;
    setBusy(true);
    try {
      // Missing repository fields are saved as explicit inheritance removals.
      const patch = Object.fromEntries((Object.keys(REVIEW_LABELS) as ReviewField[])
        .map((key) => [key, form[key] ?? (defaults ? null : effective[key])])) as ReviewPatch;
      if (!defaults || form.ignore !== undefined) patch.ignore = ignoreText.split(",").map((v) => v.trim()).filter(Boolean);
      await onSave(patch);
      toast("Review policy saved");
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(false);
    }
  };
  if (!supported) return <p className="hint">Review-driven revisions are coming.</p>;
  return (
    <form onSubmit={submit}>
      <p className="hint">Review revisions have their own round budget, separate from build attempts. Ignored logins are never counted.</p>
      <div className="s-grid">
        {(Object.keys(REVIEW_LABELS) as ReviewField[]).map((key) => {
          const inherited = defaults && form[key] === undefined;
          const disabled = !admin || busy || !!inherited;
          return <div key={key}>
            {defaults && <label className="check"><input type="checkbox" checked={!!inherited} disabled={!admin || busy} onChange={(e) => {
              const next = { ...form };
              if (e.target.checked) delete next[key];
              else {
                Object.assign(next, { [key]: effective[key] });
                if (key === "ignore") setIgnoreText(effective.ignore.join(", "));
              }
              setForm(next);
            }} />Inherit workspace: {REVIEW_LABELS[key]}</label>}
            <label className="field"><span>{REVIEW_LABELS[key]}</span>
              {key === "address" || key === "reviewers" ? <select className="select" value={effective[key]} disabled={disabled} onChange={(e) => setForm({ ...form, [key]: e.target.value })}>
                {(key === "address" ? [["unresolved", "All unresolved feedback"], ["changes_requested", "Changes requested only"], ["off", "Off"]] : [["all", "People and bots"], ["people", "People"], ["bots", "Bots"]]).map(([v, label]) => <option key={v} value={v}>{label}</option>)}
              </select> : key === "ignore" ? <input className="input" value={inherited ? effective.ignore.join(", ") : ignoreText} disabled={disabled} onChange={(e) => setIgnoreText(e.target.value)} placeholder="login, bot[bot]" /> :
                <input className="input num" type="number" min={0} step={1} required value={effective[key]} disabled={disabled} onChange={(e) => setForm({ ...form, [key]: e.target.value === "" ? NaN : Number(e.target.value) })} />}
              <span className="hint">{defaults ? inherited ? "Inherited from workspace" : "Explicit repository override" : "Workspace default"}{key === "ignore" ? " · comma-separated logins; blank means none" : ""}</span>
            </label>
          </div>;
        })}
      </div>
      {admin && <button className="btn primary" disabled={busy}>{busy ? "Saving…" : "Save review policy"}</button>}
    </form>
  );
}
