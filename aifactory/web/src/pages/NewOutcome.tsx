import { useEffect, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, type Outcome, type Repo } from "../api";
import { useOutcomeRequestId } from "../outcomeDraft";
import { useWs } from "../App";
import { Modal, useToast } from "../ui";
import { AttachmentPicker, pasteImages, useAttachmentQueue } from "../components/Attachments";

export function NewOutcome({ onClose }: { onClose: () => void }) {
  const { ws, meta } = useWs();
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const repos = useQuery({ queryKey: ["repos", ws.id], queryFn: () => api<Repo[]>(`/api/workspaces/${ws.id}/repositories`) });
  const [title, setTitle] = useState("");
  const [brief, setBrief] = useState("");
  const [criteria, setCriteria] = useState("");
  const [picked, setPicked] = useState<string[]>([]);
  const [harness, setHarness] = useState<string>("");
  const [priority, setPriority] = useState(2);
  const [more, setMore] = useState(false);
  const [busy, setBusy] = useState(false);
  const submitting = useRef(false);
  const attachments = useAttachmentQueue(ws.id);
  const draft = { title, brief, attachment_ids: attachments.ids, repo_ids: picked, harness: harness || null, priority,
    acceptance: criteria.split("\n").map((c) => c.replace(/^\s*[-*\d.]+\s*/, "").trim()).filter(Boolean) };
  const requestId = useOutcomeRequestId(draft);
  const titleRef = useRef<HTMLInputElement>(null);
  useEffect(() => titleRef.current?.focus(), []);
  useEffect(() => {
    if (repos.data?.length === 1 && picked.length === 0) setPicked([repos.data[0].id]);
  }, [repos.data, picked.length]);

  const submit = async () => {
    if (submitting.current || attachments.blocked || title.trim().length < 3 || picked.length === 0) return;
    submitting.current = true;
    attachments.submitting = true;
    setBusy(true);
    try {
      const o = await api<Outcome>(`/api/workspaces/${ws.id}/outcomes`, {
        method: "POST",
        json: { ...draft, request_id: requestId },
      });
      attachments.finishSubmission(true);
      qc.invalidateQueries({ queryKey: ["outcomes", ws.id] });
      toast(`#${o.number} queued for planning`);
      onClose();
      navigate(`/w/${ws.id}/outcomes/${o.id}`);
    } catch (err) {
      attachments.finishSubmission(false);
      toast((err as Error).message, true);
    } finally {
      submitting.current = false;
      setBusy(false);
    }
  };

  const defaultHarness = ws.settings.harness.builder;
  return (
    <Modal title="New outcome" onClose={onClose} footer={
      <>
        <span className="faint" style={{ fontSize: 12 }}>
          {ws.settings.autonomy.plan === "auto" ? "Plans start automatically" : "You'll approve the plan first"} · merges {ws.settings.autonomy.merge === "auto" ? "automatic after CI" : "need approval"}
        </span>
        <span className="spacer" />
        <button className="btn ghost" onClick={onClose}>Cancel</button>
        <button className="btn primary" onClick={submit} disabled={busy || attachments.blocked || title.trim().length < 3 || picked.length === 0}>
          {busy ? "Submitting…" : "Start"}
        </button>
      </>
    }>
      <div onKeyDown={(e) => { if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) { e.preventDefault(); void submit(); } }} style={{ display: "contents" }}>
        <input ref={titleRef} className="title-input" placeholder="What should be true when this is done?" value={title}
          onChange={(e) => setTitle(e.target.value)} aria-label="Outcome title" maxLength={300} />
        <textarea className="brief-input" placeholder="Context, constraints, links, who it's for… The planner reads the code, so describe the goal rather than the steps."
          onPaste={(event) => pasteImages(event, attachments)} value={brief} onChange={(e) => setBrief(e.target.value)} aria-label="Description" />
        <AttachmentPicker queue={attachments} />
        <div className="field">
          <span>Repositories</span>
          {repos.data?.length === 0 ? (
            <span className="hint">No repositories connected yet. Add one in Settings → Repositories.</span>
          ) : (
            <div className="pick">
              {(repos.data || []).map((r) => {
                const on = picked.includes(r.id);
                return (
                  <button key={r.id} type="button" className={`chip ${on ? "on" : ""}`} aria-pressed={on}
                    onClick={() => setPicked(on ? picked.filter((x) => x !== r.id) : [...picked, r.id])}>{r.full_name}</button>
                );
              })}
            </div>
          )}
        </div>
        {!more ? (
          <button className="linkish" style={{ alignSelf: "flex-start" }} onClick={() => setMore(true)}>Acceptance criteria, worker and priority</button>
        ) : (
          <>
            <label className="field">
              <span>Acceptance criteria <span className="faint" style={{ fontWeight: 400 }}>· optional, one per line</span></span>
              <textarea className="textarea" value={criteria} onChange={(e) => setCriteria(e.target.value)}
                placeholder={"Exports include every logged entry\nThe file opens in Excel without warnings"} />
              <span className="hint">The planner turns these into per-task criteria; an independent verifier checks each one.</span>
            </label>
            <div className="s-grid">
              <label className="field">
                <span>Worker</span>
                <select className="select" value={harness} onChange={(e) => setHarness(e.target.value)}>
                  <option value="">Workspace default ({defaultHarness})</option>
                  {meta.harnesses.map((h) => (
                    <option key={h.name} value={h.name} disabled={!h.available}>{h.label}{h.available ? "" : " — unavailable"}</option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span>Priority</span>
                <select className="select" value={priority} onChange={(e) => setPriority(Number(e.target.value))}>
                  <option value={0}>Urgent</option><option value={1}>High</option><option value={2}>Normal</option><option value={3}>Low</option>
                </select>
              </label>
            </div>
          </>
        )}
      </div>
    </Modal>
  );
}
