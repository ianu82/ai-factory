import { api, uploadJson, type Artifact } from "./api";

export const ACCEPT_ATTACHMENTS = ".png,.jpg,.jpeg,.gif,.webp,.pdf,.md,.txt,.csv,.json";
export const MAX_FILE_BYTES = 10 * 1024 * 1024;
export const MAX_TOTAL_BYTES = 25 * 1024 * 1024;
export type UploadState = "queued" | "reading" | "uploading" | "processing" | "ready" | "error";
export interface UploadItem {
  id: string; file: File; preview?: string; state: UploadState; progress: number;
  error?: string; artifact?: Artifact; controller?: AbortController;
}
export function attachmentLimit(files: { size: number }[], file: { size: number }): string | undefined {
  if (files.length >= 10) return "Up to 10 attachments are allowed.";
  if (!file.size || file.size > MAX_FILE_BYTES) return "Each attachment must contain between 1 byte and 10 MiB.";
  if (files.reduce((sum, f) => sum + f.size, file.size) > MAX_TOTAL_BYTES) return "Attachments must total at most 25 MiB.";
}
export function safeDownloadName(name: string): string {
  return name.replace(/\\/g, "/").split("/").pop()!.replace(/[^a-zA-Z0-9._-]/g, "_").replace(/^\.+/, "").slice(0, 180) || "attachment";
}
export function artifactBlob(artifact: Artifact): Blob {
  const bytes = Uint8Array.from(atob(artifact.content || ""), (c) => c.charCodeAt(0));
  return new Blob([bytes], { type: artifact.data.mime_type || "application/octet-stream" });
}
export function fileSize(size: number): string {
  return size < 1024 ? `${size} B` : size < 1024 * 1024 ? `${(size / 1024).toFixed(1)} KiB` : `${(size / 1024 / 1024).toFixed(1)} MiB`;
}
function readFile(file: File, signal: AbortSignal): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    const abort = () => reader.abort();
    const clean = () => signal.removeEventListener("abort", abort);
    reader.onload = () => { clean(); resolve(String(reader.result).split(",")[1]); };
    reader.onerror = () => { clean(); reject(reader.error || new Error("Could not read file")); };
    reader.onabort = () => { clean(); reject(new DOMException("Cancelled", "AbortError")); };
    if (signal.aborted) { reject(new DOMException("Cancelled", "AbortError")); return; }
    signal.addEventListener("abort", abort, { once: true });
    reader.readAsDataURL(file);
  });
}

/** Owns synchronous budget reservations, cancellation and late-response cleanup. */
export class AttachmentQueue {
  items: UploadItem[] = [];
  notices: string[] = [];
  disposed = false;
  submitting = false;
  linked = false;
  constructor(readonly workspaceId: string, readonly changed: () => void,
    readonly request = api, readonly upload = uploadJson, readonly read = readFile) {}
  get blocked() { return this.items.some((item) => item.state !== "ready"); }
  get ids() { return this.items.flatMap((item) => item.artifact ? [item.artifact.id] : []); }
  add(files: File[]) {
    if (this.submitting || this.disposed) return;
    this.notices = [];
    for (const file of files) {
      const error = attachmentLimit(this.items.map((i) => i.file), file);
      if (error) { this.notices.push(`${file.name}: ${error}`); continue; }
      const preview = /^image\/(png|jpeg|gif|webp)$/.test(file.type) ? URL.createObjectURL(file) : undefined;
      this.items.push({ id: crypto.randomUUID(), file, preview, state: "queued", progress: 0 });
    }
    this.changed();
    this.pump();
  }
  private patch(item: UploadItem, update: Partial<UploadItem>) {
    if (!this.disposed && this.items.includes(item)) { Object.assign(item, update); this.changed(); }
  }
  private pump() {
    if (this.disposed || this.items.some((i) => ["reading", "uploading", "processing"].includes(i.state))) return;
    const item = this.items.find((i) => i.state === "queued");
    if (item) void this.send(item);
  }
  private async send(item: UploadItem) {
    const controller = new AbortController();
    this.patch(item, { controller, state: "reading", error: undefined, progress: 0 });
    try {
      const content = await this.read(item.file, controller.signal);
      if (controller.signal.aborted) return;
      this.patch(item, { state: "uploading" });
      const artifact = await this.upload<Artifact>(`/api/workspaces/${this.workspaceId}/attachments`, { name: item.file.name, content }, controller.signal,
        (loaded, total) => this.patch(item, { progress: Math.round(100 * loaded / total), state: loaded >= total ? "processing" : "uploading" }));
      if (this.disposed || !this.items.includes(item)) this.delete(artifact.id);
      else this.patch(item, { artifact, state: "ready", progress: 100 });
    } catch (error) {
      this.patch(item, { state: "error", error: (error as Error).message });
    } finally { this.pump(); }
  }
  private delete(id: string) {
    void this.request(`/api/workspaces/${this.workspaceId}/attachments/${id}`, { method: "DELETE", keepalive: true }).catch(() => {});
  }
  remove(id: string) {
    if (this.submitting) return;
    const item = this.items.find((i) => i.id === id);
    if (!item) return;
    this.items = this.items.filter((i) => i !== item);
    item.controller?.abort();
    if (item.preview) URL.revokeObjectURL(item.preview);
    if (item.artifact) this.delete(item.artifact.id);
    this.changed();
    this.pump();
  }
  retry(id: string) {
    const item = this.items.find((i) => i.id === id);
    if (!item || item.state !== "error" || this.submitting) return;
    item.state = "queued";
    this.changed();
    this.pump();
  }
  finishSubmission(success: boolean) {
    this.linked = success;
    this.submitting = false;
    if (this.disposed && !success) this.ids.forEach((id) => this.delete(id));
  }
  dispose() {
    this.disposed = true;
    for (const item of this.items) {
      item.controller?.abort();
      if (item.preview) URL.revokeObjectURL(item.preview);
      if (!this.submitting && !this.linked && item.artifact) this.delete(item.artifact.id);
    }
  }
}
