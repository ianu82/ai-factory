import { createContext, useCallback, useContext, useEffect, useRef, useState, type ReactNode } from "react";
import { Check, Minus, X } from "lucide-react";
import { containModalFocus } from "./modalFocus";
import type { Task } from "./api";
import { OUTCOME_STATUS, TASK_STATUS, type Tone } from "./format";

export function Dot({ tone, title }: { tone: Tone; title?: string }) {
  return <span className={`dot ${tone}`} title={title} aria-hidden={!title} />;
}

export function Status({ status, kind = "outcome", label }: { status: string; kind?: "outcome" | "task"; label?: string }) {
  const map = kind === "outcome" ? OUTCOME_STATUS : TASK_STATUS;
  const s = map[status] || { label: status, tone: "idle" as Tone };
  return (
    <span className={`status ${s.tone}`}>
      <Dot tone={s.tone} />
      {label || s.label}
    </span>
  );
}

export const GATES = [
  { key: "build", label: "Build" },
  { key: "checks", label: "Checks" },
  { key: "verify", label: "Verify" },
  { key: "pr", label: "PR" },
  { key: "ci", label: "CI" },
  { key: "merge", label: "Merge" },
  { key: "smoke", label: "Smoke" },
] as const;

// Repair and revalidation invalidate later gates even if evidence from the old candidate remains.
const STAGE_INDEX: Record<string, number> = {
  ci_repair: 0, revalidate: 0, revalidate_prepare: 0, revalidate_setup: 0, revalidate_checks: 1,
  revalidate_verify: 2, reverify: 2, revalidate_publish: 3,
  "": 0, prepare: 0, build: 0, revise: 0, checks: 1, verify: 2, publish: 3, ci: 4, review: 5, merge: 5, smoke: 6,
  release: 7, done: 7,
};

export type GateState = "done" | "active" | "human" | "bad" | "skip" | "todo";

/** Derive each delivery gate's state from a task's durable stage, status and evidence. */
export function gateStates(task: Task): GateState[] {
  // A parked task waits for CI on its head, then (stage "review", CI passed) for the pull request's reviewers.
  const idx = task.status === "waiting_ci" ? (task.stage === "review" ? 5 : 4) : STAGE_INDEX[task.stage] ?? 0;
  const ev = task.evidence || {};
  return GATES.map((g, i) => {
    if (g.key === "ci" && ev.ci?.state === "none" && idx > 4) return "skip";
    if (g.key === "smoke" && ev.smoke?.state === "not_configured") return "skip";
    if (task.status === "completed") return "done";
    if (task.status === "smoke_failed") return i < 6 ? "done" : "bad";
    if (i < idx) return "done";
    if (i > idx) return "todo";
    if (task.status === "running") return "active";
    if (task.status === "needs_input" || task.status === "awaiting_approval") return "human";
    if (["failed", "blocked"].includes(task.status)) return "bad";
    return "todo";
  });
}

export function GateTrack({ task, large = false }: { task: Task; large?: boolean }) {
  const states = gateStates(task);
  const parked = task.status === "waiting_ci";
  const waiting = !parked ? "" : task.stage === "review" ? "Waiting for review" : "Waiting for CI";
  const label = `${waiting ? `${waiting}. ` : ""}${GATES.map((g, i) => `${g.label}: ${states[i]}`).join(", ")}`;
  if (!large) {
    return (
      <span className="gates" role="img" aria-label={label} title={label}>
        {states.map((s, i) => <span key={i} className={`gate ${s === "todo" ? "" : s}`} />)}
        {waiting && <span className="muted" style={{ fontSize: 11, whiteSpace: "nowrap" }}>{waiting}</span>}
      </span>
    );
  }
  const cur = states.findIndex((s) => s === "active" || s === "human" || s === "bad");
  return (
    <div className="gates lg" role="img" aria-label={label}>
      {states.map((s, i) => (
        <div key={i} className={`gstep ${i === cur ? "cur" : ""} ${s}`}>
          <span className={`gate ${s === "todo" ? "" : s}`} />
          <span className="glabel">{GATES[i].label}{s === "skip" ? " · n/a" : ""}</span>
        </div>
      ))}
    </div>
  );
}

export function Mark({ status }: { status: string }) {
  if (status === "pass") return <span className="mark pass" aria-label="passed"><Check strokeWidth={3} /></span>;
  if (status === "fail") return <span className="mark fail" aria-label="failed"><X strokeWidth={3} /></span>;
  return <span className="mark none" aria-label="not verified"><Minus strokeWidth={3} /></span>;
}

export function Avatar({ name }: { name: string }) {
  const initials = (name || "?").split(/[\s@.]+/).filter(Boolean).slice(0, 2).map((p) => p[0]?.toUpperCase()).join("");
  return <span className="avatar">{initials || "?"}</span>;
}

export function Empty({ title, children, action }: { title: string; children?: ReactNode; action?: ReactNode }) {
  return (
    <div className="empty">
      <h3>{title}</h3>
      {children && <p>{children}</p>}
      {action}
    </div>
  );
}

export function Skeleton({ rows = 6 }: { rows?: number }) {
  return (
    <div style={{ padding: "14px 22px", display: "flex", flexDirection: "column", gap: 18 }} aria-busy="true">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} style={{ display: "flex", flexDirection: "column", gap: 7 }}>
          <div className="skeleton" style={{ width: `${55 + ((i * 17) % 35)}%` }} />
          <div className="skeleton" style={{ width: `${25 + ((i * 11) % 20)}%`, height: 10 }} />
        </div>
      ))}
    </div>
  );
}

export function Modal({ title, onClose, children, footer }: { title: string; onClose: () => void; children: ReactNode; footer?: ReactNode }) {
  const ref = useRef<HTMLDivElement>(null);
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  useEffect(() => {
    if (ref.current) return containModalFocus(ref.current, () => closeRef.current());
  }, []);
  return (
    <div className="scrim" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className="modal" role="dialog" aria-modal="true" aria-label={title} ref={ref} tabIndex={-1}>
        <div className="modal-head">
          <h2>{title}</h2>
          <span style={{ flex: 1 }} />
          <button className="icon-btn" onClick={onClose} aria-label="Close"><X /></button>
        </div>
        <div className="modal-body">{children}</div>
        {footer && <div className="modal-foot">{footer}</div>}
      </div>
    </div>
  );
}

type ToastItem = { id: number; text: string; bad?: boolean };
const ToastCtx = createContext<(text: string, bad?: boolean) => void>(() => {});

export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<ToastItem[]>([]);
  const push = useCallback((text: string, bad = false) => {
    const id = Date.now() + Math.random();
    setItems((xs) => [...xs, { id, text, bad }]);
    setTimeout(() => setItems((xs) => xs.filter((x) => x.id !== id)), bad ? 6000 : 3200);
  }, []);
  return (
    <ToastCtx.Provider value={push}>
      {children}
      <div className="toast-wrap" role="status" aria-live="polite">
        {items.map((t) => <div key={t.id} className={`toast ${t.bad ? "bad" : ""}`}>{t.text}</div>)}
      </div>
    </ToastCtx.Provider>
  );
}

export const useToast = () => useContext(ToastCtx);

const INLINE = /(\*\*[^*\n]+\*\*|`[^`\n]+`|https?:\/\/[^\s)]+)/g;

function inline(text: string, key: string): ReactNode[] {
  return text.split(INLINE).filter(Boolean).map((part, i) => {
    if (part.startsWith("**") && part.endsWith("**")) return <strong key={`${key}-${i}`}>{part.slice(2, -2)}</strong>;
    if (part.startsWith("`") && part.endsWith("`")) return <code key={`${key}-${i}`} className="mono inline-code">{part.slice(1, -1)}</code>;
    if (/^https?:\/\//.test(part)) return <a key={`${key}-${i}`} className="linkish" href={part} target="_blank" rel="noreferrer">{part.replace(/^https?:\/\//, "").slice(0, 60)}</a>;
    return <span key={`${key}-${i}`}>{part}</span>;
  });
}

/** Agent-written text: a tiny, safe markdown subset rendered as React elements (no HTML injection). */
export function Prose({ text, className = "" }: { text: string; className?: string }) {
  const blocks = (text || "").split(/```[a-z]*\n?/);
  return (
    <div className={`prose ${className}`}>
      {blocks.map((block, i) =>
        i % 2 === 1 ? <pre key={i} className="log" style={{ margin: "6px 0" }}>{block.replace(/\n$/, "")}</pre>
          : <span key={i}>{inline(block, String(i))}</span>)}
    </div>
  );
}

export function DiffView({ text }: { text: string }) {
  const lines = text.split("\n").slice(0, 4000);
  return (
    <div className="diff">
      {lines.map((l, i) => {
        const cls = l.startsWith("diff --git") ? "file" : l.startsWith("@@") ? "hunk" : l.startsWith("+") && !l.startsWith("+++") ? "add"
          : l.startsWith("-") && !l.startsWith("---") ? "del" : "";
        return <div key={i} className={cls}>{l || " "}</div>;
      })}
    </div>
  );
}
