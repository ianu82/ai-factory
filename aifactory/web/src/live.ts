import { useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";

/** Subscribe to workspace events and refresh affected queries. Returns connection health. */
export function useLive(workspaceId: string | undefined): "live" | "connecting" | "offline" {
  const qc = useQueryClient();
  const [state, setState] = useState<"live" | "connecting" | "offline">("connecting");
  useEffect(() => {
    if (!workspaceId) return;
    let es: EventSource | null = null;
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
    const connect = () => {
      setState("connecting");
      es = new EventSource(`/api/workspaces/${workspaceId}/stream`);
      es.onopen = () => setState("live");
      es.onerror = () => setState(es?.readyState === EventSource.CLOSED ? "offline" : "connecting");
      es.onmessage = (msg) => {
        let data: any;
        try { data = JSON.parse(msg.data); } catch { return; }
        if (data.type === "event") {
          touch("outcome", "events");
          if (["decision_requested", "decision_resolved"].includes(data.kind)) touch("inbox", "outcomes");
          if (data.kind.startsWith("run_")) touch("runs");
          if (["outcome_completed", "merged", "plan_proposed", "plan_approved", "outcome_created", "task_completed", "stage"].includes(data.kind)) touch("outcomes");
        } else if (data.type === "changed") {
          touch("outcome", "outcomes");
          if (data.entity === "decision") touch("inbox", "decisions");
          if (data.entity === "run") touch("runs");
          if (data.entity === "signal") touch("signals");
        }
      };
    };
    connect();
    return () => { es?.close(); if (timer) clearTimeout(timer); };
  }, [workspaceId, qc]);
  return state;
}
