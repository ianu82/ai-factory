import { useEffect, useRef, useState, type ClipboardEvent, type DragEvent } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, type Artifact } from "../api";
import { ACCEPT_ATTACHMENTS, AttachmentQueue, artifactBlob, fileSize, safeDownloadName } from "../attachments";
import { Modal } from "../ui";

export function useAttachmentQueue(workspaceId: string) {
  const [, render] = useState(0);
  const [queue] = useState(() => new AttachmentQueue(workspaceId, () => render((n) => n + 1)));
  useEffect(() => {
    queue.disposed = false;
    return () => queue.dispose();
  }, [queue]);
  return queue;
}

export function pasteImages(event: ClipboardEvent<HTMLTextAreaElement>, queue: AttachmentQueue) {
  const images = Array.from(event.clipboardData.items).filter((item) => item.kind === "file" && item.type.startsWith("image/"))
    .map((item) => item.getAsFile()).filter((file): file is File => file !== null);
  if (images.length) {
    // Preserve ordinary text, including mixed text/image clipboard payloads.
    if (!event.clipboardData.getData("text/plain")) event.preventDefault();
    queue.add(images);
  }
}

export function AttachmentPicker({ queue }: { queue: AttachmentQueue }) {
  const picker = useRef<HTMLInputElement>(null);
  const fileDrag = (event: DragEvent) => Array.from(event.dataTransfer.types).includes("Files");
  return <section className="attachment-intake" aria-label="Attachments"
    onDragOver={(event) => { if (fileDrag(event)) { event.preventDefault(); event.dataTransfer.dropEffect = queue.submitting ? "none" : "copy"; } }}
    onDrop={(event) => { if (fileDrag(event)) { event.preventDefault(); queue.add(Array.from(event.dataTransfer.files)); } }}>
    <div className="attachment-actions">
      <button type="button" className="btn sm" disabled={queue.submitting} onClick={() => picker.current?.click()}>Attach files</button>
      <input ref={picker} type="file" multiple accept={ACCEPT_ATTACHMENTS} hidden onChange={(event) => {
        queue.add(Array.from(event.target.files || [])); event.target.value = "";
      }} />
      <span className="hint">Drop files here or paste an image into the description. Up to 10 files, 10 MiB each, 25 MiB total. PNG, JPEG, GIF, WebP, PDF, Markdown, text, CSV or JSON.</span>
    </div>
    <div role="alert">{queue.notices.map((notice, i) => <p className="attachment-error" key={i}>{notice}</p>)}</div>
    <ul className="attachment-list">
      {queue.items.map((item) => <li key={item.id} className="attachment-card">
        {item.preview ? <img className="attachment-thumb" src={item.preview} alt="" /> : <span className="attachment-doc" aria-hidden="true">Document</span>}
        <div className="attachment-info">
          <strong>{item.file.name}</strong><span className="hint">{fileSize(item.file.size)}</span>
          <span role="status" aria-live="polite">{item.state}{item.state === "uploading" ? ` · ${item.progress}%` : ""}</span>
          {item.state === "uploading" && <progress max={100} value={item.progress} aria-label={`Uploading ${item.file.name}`} />}
          {item.error && <span className="attachment-error" role="alert">{item.error}</span>}
        </div>
        {item.state === "error" && <button type="button" className="btn sm" disabled={queue.submitting} onClick={() => queue.retry(item.id)} aria-label={`Retry ${item.file.name}`}>Retry</button>}
        <button type="button" className="btn sm ghost" disabled={queue.submitting} onClick={() => queue.remove(item.id)} aria-label={`Remove ${item.file.name}`}>Remove</button>
      </li>)}
    </ul>
    {queue.blocked && <p className="hint" role="status">Wait for uploads to finish; retry or remove any failed files before starting.</p>}
  </section>;
}

export function OutcomeAttachments({ workspaceId, outcomeId, attachments }: { workspaceId: string; outcomeId: string; attachments: Artifact[] }) {
  if (!attachments.length) return null;
  return <section aria-label="Attachments"><h4>Attachments</h4><ul className="attachment-list">
    {attachments.map((artifact) => <AttachmentFile key={`${workspaceId}:${outcomeId}:${artifact.id}`} workspaceId={workspaceId} outcomeId={outcomeId} artifact={artifact} />)}
  </ul></section>;
}

function AttachmentFile({ workspaceId, outcomeId, artifact }: { workspaceId: string; outcomeId: string; artifact: Artifact }) {
  const image = ["image/png", "image/jpeg", "image/gif", "image/webp"].includes(artifact.data.mime_type);
  const element = useRef<HTMLLIElement>(null);
  const [visible, setVisible] = useState(false);
  const [requested, setRequested] = useState(false);
  const [full, setFull] = useState(false);
  const [url, setUrl] = useState("");
  useEffect(() => {
    if (!image) return;
    if (!window.IntersectionObserver) { setVisible(true); return; }
    const observer = new IntersectionObserver((entries) => { if (entries.some((entry) => entry.isIntersecting)) { setVisible(true); observer.disconnect(); } });
    if (element.current) observer.observe(element.current);
    return () => observer.disconnect();
  }, [image]);
  const content = useQuery({
    queryKey: ["attachment-content", workspaceId, outcomeId, artifact.id],
    queryFn: ({ signal }) => api<Artifact>(`/api/workspaces/${workspaceId}/artifacts/${artifact.id}`, { signal }),
    enabled: (image && visible) || requested,
    staleTime: Infinity,
    gcTime: 0,
  });
  useEffect(() => {
    if (!content.data) return;
    const blobUrl = URL.createObjectURL(artifactBlob(content.data));
    setUrl(blobUrl);
    return () => URL.revokeObjectURL(blobUrl);
  }, [content.data]);
  useEffect(() => {
    if (!requested || !url) return;
    if (image) { setFull(true); setRequested(false); return; }
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = safeDownloadName(artifact.name);
    document.body.append(anchor);
    anchor.click();
    anchor.remove();
    setRequested(false);
  }, [requested, url, image, artifact.name]);
  return <li className="attachment-card" ref={element}>
    <button type="button" className="attachment-open" onClick={() => {
      if (image && url) setFull(true);
      else { setRequested(true); if (content.isError) void content.refetch(); }
    }} aria-label={`${image ? "Open full-size image" : "Download"} ${artifact.name}`}>
      {image && url ? <img className="attachment-thumb" src={url} alt="" loading="lazy" /> : <span className="attachment-doc">{image ? "Image" : "Document"}</span>}
      <span className="attachment-info"><strong>{artifact.name}</strong><span className="hint">{fileSize(artifact.data.size_bytes ?? artifact.size)} · {artifact.data.mime_type}</span></span>
    </button>
    {content.isFetching && <span role="status">Loading…</span>}
    {content.isError && <span className="attachment-error" role="alert">{content.error.message} <button className="btn sm" onClick={() => content.refetch()}>Retry</button></span>}
    {full && url && <Modal title={artifact.name} onClose={() => setFull(false)}>
      <img className="attachment-full" src={url} alt={artifact.name} />
      <a className="btn" href={url} download={safeDownloadName(artifact.name)}>Download image</a>
    </Modal>}
  </li>;
}
