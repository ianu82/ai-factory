import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, type Decision } from "../api";
import { useWs } from "../App";
import { ago } from "../format";
import { useToast } from "../ui";

/** Shown on every page while a provider hold stops the workspace: why the provider refused, what lifts the hold,
 * and the button that resumes work. Nothing new starts while it is open, so it must never be easy to miss. */
export function HoldBanner() {
  const { ws } = useWs();
  const qc = useQueryClient();
  const navigate = useNavigate();
  const toast = useToast();
  const [busy, setBusy] = useState(false);
  // The sidebar's Inbox query, which refreshes it every 30 s: one request serves both.
  const inbox = useQuery({
    queryKey: ["inbox", ws.id],
    queryFn: () => api<{ decisions: Decision[] }>(`/api/workspaces/${ws.id}/inbox`),
  });
  const hold = inbox.data?.decisions.find((d) => d.kind === "provider_hold");
  if (!hold) return null;
  const why = hold.hold;
  // The Factory's own test of the provider: it resumes work itself once every model answers (pipeline/holds.py).
  const tests = why?.tests_every_minutes
    ? ` The Factory tests the provider every ${why.tests_every_minutes} min and resumes work once it answers`
      + (why.last_tested_at ? ` (last tested ${ago(why.last_tested_at)})` : "") + "."
    : "";
  const resume = async () => {
    setBusy(true);
    try {
      await api(`/api/workspaces/${ws.id}/decisions/${hold.id}/resolve`, { method: "POST", json: { resolution: "retry", comment: "" } });
      toast("Work resumed");
      qc.invalidateQueries({ queryKey: ["inbox", ws.id] });
      qc.invalidateQueries({ queryKey: ["outcomes", ws.id] });
    } catch (err) {
      toast((err as Error).message, true);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="banner hold" role="alert">
      <span>
        <b>Work is on hold</b> ({ago(hold.created_at)}): {why ? why.label : "the model provider refused a request"}
        {why?.source ? ` (${why.source})` : ""}. Nothing new starts until work is resumed. {why?.advice}
        {tests}
      </span>
      <span style={{ flex: 1 }} />
      <button className="btn sm" onClick={() => navigate(`/w/${ws.id}/inbox/${hold.id}`)}>Details</button>
      {ws.role !== "viewer" && (
        <button className="btn sm primary" disabled={busy} onClick={resume}>{busy ? "…" : "Resume work"}</button>
      )}
    </div>
  );
}
