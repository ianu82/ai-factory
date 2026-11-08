import { useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";

export type LiveState = "live" | "connecting" | "offline";

export const BACKOFF_MS = [1000, 2000, 5000, 10000, 30000];

interface SourceLike {
  readyState: number;
  onopen: ((e: unknown) => void) | null;
  onerror: ((e: unknown) => void) | null;
  onmessage: ((e: { data: string }) => void) | null;
  close(): void;
}

export interface LiveDeps {
  open(url: string): SourceLike;
  setTimeout(fn: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
  listen(type: "online" | "focus" | "visible", fn: () => void): () => void;
  onState(state: LiveState): void;
  onData(data: any): void;
  onResync(): void;
}

const CLOSED = 2;

/**
 * Keep one event stream open for as long as the page lives.
 *
 * The browser retries a dropped stream by itself, but gives up for good when a reconnect gets an error
 * response (for example a 502 while the server restarts during a deploy). We then reconnect with backoff,
 * immediately when the tab regains focus or the network returns, and ask for a resync once reconnected
 * because events may have been missed in the gap. Returns a function that stops everything.
 */
export function startLive(url: string, deps: LiveDeps): () => void {
  let source: SourceLike | null = null;
  let retry: unknown = null;
  let attempts = 0;
  let missed = false;
  let stopped = false;

  const schedule = () => {
    if (stopped || retry) return;
    const wait = BACKOFF_MS[Math.min(attempts, BACKOFF_MS.length - 1)];
    attempts += 1;
    retry = deps.setTimeout(() => { retry = null; connect(); }, wait);
  };
  const connect = () => {
    if (stopped) return;
    source?.close();
    const current = deps.open(url);
    source = current;
    current.onopen = () => {
      if (source !== current) return;
      attempts = 0;
      deps.onState("live");
      if (missed) {
        missed = false;
        deps.onResync();
      }
    };
    current.onerror = () => {
      if (source !== current) return;
      if (current.readyState === CLOSED) {
        missed = true;
        deps.onState("offline");
        schedule();
      } else {
        deps.onState("connecting");  // the browser is retrying on its own
      }
    };
    current.onmessage = (msg) => {
      if (source !== current) return;
      let data: any;
      try { data = JSON.parse(msg.data); } catch { return; }
      deps.onData(data);
    };
  };
  const now = () => {
    if (stopped || !(missed || source?.readyState === CLOSED)) return;
    if (retry) { deps.clearTimeout(retry); retry = null; }
    attempts = 0;
    connect();
  };
  const unlisten = [deps.listen("online", now), deps.listen("focus", now), deps.listen("visible", now)];
  deps.onState("connecting");
  connect();
  return () => {
    stopped = true;
    source?.close();
    if (retry) deps.clearTimeout(retry);
    unlisten.forEach((off) => off());
  };
}

/** Subscribe to workspace events and refresh affected queries. Returns connection health. */
export function useLive(workspaceId: string | undefined): LiveState {
  const qc = useQueryClient();
  const [state, setState] = useState<LiveState>("connecting");
  useEffect(() => {
    if (!workspaceId) return;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let pending = new Set<string>();
    const flush = () => {
      timer = null;
      const keys = pending;
      pending = new Set();
      keys.forEach((k) => qc.invalidateQueries({ queryKey: [k, workspaceId] }));
    };
    const touch = (...keys: string[]) => {
      keys.forEach((k) => pending.add(k));
      if (!timer) timer = setTimeout(flush, 350);
    };
    const stop = startLive(`/api/workspaces/${workspaceId}/stream`, {
      open: (url) => new EventSource(url) as unknown as SourceLike,
      setTimeout: (fn, ms) => window.setTimeout(fn, ms),
      clearTimeout: (handle) => window.clearTimeout(handle as number),
      listen: (type, fn) => {
        if (type === "visible") {
          const onVisible = () => { if (document.visibilityState === "visible") fn(); };
          document.addEventListener("visibilitychange", onVisible);
          return () => document.removeEventListener("visibilitychange", onVisible);
        }
        window.addEventListener(type, fn);
        return () => window.removeEventListener(type, fn);
      },
      onState: setState,
      onResync: () => qc.invalidateQueries({ predicate: (q) => q.queryKey[1] === workspaceId }),
      onData: (data) => {
        if (data.type === "event") {
          touch("outcome", "events");
          if (["decision_requested", "decision_resolved"].includes(data.kind)) touch("inbox", "outcomes");
          if (data.kind.startsWith("run_")) touch("runs");
          if (["outcome_completed", "merged", "plan_proposed", "plan_approved", "outcome_created", "task_completed", "stage"].includes(data.kind)) touch("outcomes");
          if (["published", "merged", "merged_outside", "ci_status", "task_completed"].includes(data.kind)) touch("pulls");
        } else if (data.type === "changed") {
          touch("outcome", "outcomes");
          if (data.entity === "decision") touch("inbox", "decisions");
          if (data.entity === "run") touch("runs");
          if (data.entity === "signal") touch("signals");
        }
      },
    });
    return () => { stop(); if (timer) clearTimeout(timer); };
  }, [workspaceId, qc]);
  return state;
}
