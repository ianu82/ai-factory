import { createContext, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState, useSyncExternalStore, type ReactNode } from "react";
import { Link, useLocation } from "react-router-dom";
import { skipToken, useQuery } from "@tanstack/react-query";
import { api, apiResponse, type Me, type MindCard, type MindCitation, type MindContext, type Role } from "../api";
import { MindController, canConfirm, mindContextLabel, mindCost, mindLink, mindModelLine, mindRouteContext, mindSuggestions, readMindSSE, type MindTransport } from "../mind";
import { createMindHttpTransport } from "../mindHttp";
import { MindScroll } from "../mindScroll";
import { containModalFocus } from "../modalFocus";
import "./MindDock.css";

// The pure adapter can be exercised against T1/T3 fixtures without a browser.
export const mindHttpTransport = createMindHttpTransport(api, apiResponse, readMindSSE);
function hasOtherDialog(panel: HTMLElement | null): boolean {
  return [...document.querySelectorAll('[role="dialog"][aria-modal="true"], dialog[open]')].some((node) => node !== panel);
}
const MindCtx = createContext<{ controller: MindController; roles: Record<string, Role>; workspaces: Me["workspaces"] } | null>(null);
/** Mount once above page routes. Pass null on sign-out to erase private state and abort requests. */
export function MindProvider({ me, transport = mindHttpTransport, children }: { me: Me | null; transport?: MindTransport; children: ReactNode }) {
  const userId = me?.user.id ?? null;
  const roleKey = JSON.stringify((me?.workspaces || []).map((w) => [w.id, w.role]));
  const roles: Record<string, Role> = useMemo(() => Object.fromEntries(JSON.parse(roleKey)), [roleKey]);
  // A changed identity gets a fresh instance before children render, never a stale snapshot.
  const controller = useMemo(() => {
    const next = new MindController(transport);
    next.setIdentity(userId, roles);
    return next;
    // Role changes update the existing instance rather than discarding the thread.
  }, [userId, transport]);
  useEffect(() => { controller.setIdentity(userId, roles); }, [controller, userId, roles]);
  useEffect(() => () => { controller.setIdentity(null); }, [controller]);
  return <MindCtx.Provider value={{ controller, roles, workspaces: me?.workspaces || [] }}>{children}</MindCtx.Provider>;
}
export function useMind() {
  const value = useContext(MindCtx);
  if (!value) throw new Error("useMind requires MindProvider outside page routes");
  const state = useSyncExternalStore(value.controller.subscribe, value.controller.getSnapshot);
  return { ...value, state, setContext: (context: MindContext | null) => value.controller.setContext(context) };
}
/** Optional object metadata override; T5 can use setContext directly instead. */
export function useMindContext(context: MindContext) {
  const { controller } = useMind();
  const key = JSON.stringify(context);
  useEffect(() => { controller.setContext(JSON.parse(key)); }, [controller, key]);
}
/** Mount beside the dock when routing is owned by the app shell. Does not close the dock. */
export function MindRouteContext() {
  const location = useLocation();
  const { controller } = useMind();
  useEffect(() => { controller.navigate(location.pathname, mindRouteContext(location.pathname)); }, [controller, location.pathname]);
  return null;
}
function Citations({ citations = [] }: { citations?: MindCitation[] }) {
  return <ul className="mind-citations">{citations.map((citation, i) => {
    const kind = mindLink(citation.url);
    return <li key={i}>{kind === "internal" ? <Link to={citation.url}>{citation.label}</Link> : kind === "external" ? <a href={citation.url} target="_blank" rel="noopener noreferrer">{citation.label} ↗</a> : <span>{citation.label}</span>}</li>;
  })}</ul>;
}
function ActionCard({ card }: { card: MindCard }) {
  const { controller, state, roles, workspaces } = useMind();
  const [editing, setEditing] = useState(false);
  const [fields, setFields] = useState(card.fields);
  const pending = state.busy || state.pendingCards.includes(card.id);
  useEffect(() => { setFields(card.fields); setEditing(false); }, [card.revision, card.status, card.fields]);
  const target = workspaces.find((w) => w.id === card.workspace_id);
  const eligible = canConfirm(card, roles, state.pendingCards);
  return <section className="mind-card" aria-label={card.title} aria-busy={pending}>
    <h3>{card.title}</h3>
    <p className="muted">Target: {target?.name || card.workspace_id} · Revision {card.revision}</p>
    {editing ? <form onSubmit={(e) => { e.preventDefault(); void controller.revise(card, fields); }}>
      {Object.entries(fields).map(([key, value]) => <label className="field" key={key}><span>{key}</span><textarea className="textarea" value={value} disabled={pending} onChange={(e) => setFields({ ...fields, [key]: e.target.value })} /></label>)}
      <button className="btn sm" disabled={pending}>Save revision</button>
      <button type="button" className="btn sm ghost" disabled={pending} onClick={() => { setEditing(false); setFields(card.fields); }}>Cancel edit</button>
    </form> : <dl>{Object.entries(card.fields).map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{value}</dd></div>)}</dl>}
    {card.effect && <p className="mind-text">{card.effect}</p>}
    {card.status === "stale" && <p role="status">Target changed. Create a new proposal before confirming.</p>}
    {card.status === "executing" && <p role="status">Confirmation is executing. Reload for its result.</p>}
    {card.status === "proposed" && <>
      {!eligible && !pending && <p className="muted">Confirmation requires a member, admin or owner role in the target workspace.</p>}
      <div className="mind-actions">
        <button className="btn sm" disabled={pending || editing || !eligible} onClick={() => setEditing(true)}>Edit</button>
        <button className="btn sm ghost" disabled={pending || editing} onClick={() => void controller.dismiss(card)}>Dismiss</button>
        <button className="btn sm primary" disabled={pending || editing || !eligible} onClick={() => void controller.confirm(card)}>{pending ? "Pending…" : "Confirm"}</button>
      </div>
    </>}
    {card.status === "dismissed" && <p>Dismissed · no action taken.</p>}
    {card.status === "confirmed" && <div><b>Confirmed result</b><p>{card.result?.text || "Action completed."}</p><Citations citations={card.result?.citations} /></div>}
  </section>;
}

export function MindDock({ workspaceId }: { workspaceId?: string }) {
  const { state, controller, workspaces } = useMind();
  const panel = useRef<HTMLElement>(null), composer = useRef<HTMLTextAreaElement>(null), trigger = useRef<HTMLButtonElement>(null);
  const [mobile, setMobile] = useState(() => window.matchMedia("(max-width: 760px)").matches);
  const [text, setText] = useState("");
  const messages = useRef<HTMLDivElement>(null);
  const scroll = useRef(new MindScroll());
  const [newReply, setNewReply] = useState(false);
  useLayoutEffect(() => {
    scroll.current = new MindScroll();
    setNewReply(false);
  }, [state.active?.id, state.open]);
  useLayoutEffect(() => {
    if (messages.current) setNewReply(scroll.current.onOutput(messages.current));
  }, [state.messages, state.draft, state.progress, state.open]);
  const [focusRequest, setFocusRequest] = useState(0);
  const wasOpen = useRef(false);
  const currentWorkspace = workspaceId || state.context?.workspace_id || workspaces[0]?.id;
  // Label the context from what the page already loaded (same query keys); the dock never fetches for a label.
  const context = state.context;
  const outcome = useQuery<{ outcome?: { number: number; title: string } }>({
    queryKey: ["outcome", context?.workspace_id, context?.outcome_id], queryFn: skipToken });
  const inbox = useQuery<{ decisions?: { id: string; title: string }[] }>({
    queryKey: ["inbox", context?.workspace_id], queryFn: skipToken });
  const contextLabel = context ? mindContextLabel(context, {
    outcome: context.kind === "outcome" ? outcome.data?.outcome : null,
    decision: context.kind === "decision" ? inbox.data?.decisions?.find((d) => d.id === context.id) : null }) : "";
  useEffect(() => {
    const media = window.matchMedia("(max-width: 760px)");
    const change = () => setMobile(media.matches);
    media.addEventListener("change", change); return () => media.removeEventListener("change", change);
  }, []);
  useEffect(() => { setText(""); }, [state.userId, state.active?.id]);
  useEffect(() => { if (state.open && state.userId) void controller.list(); }, [controller, state.open, state.userId]);
  useEffect(() => {
    if (state.open && mobile && panel.current) return containModalFocus(panel.current, () => controller.setOpen(false), () => !hasOtherDialog(panel.current));
  }, [state.open, mobile, controller]);
  useEffect(() => {
    // Shortcuts can open the sheet without focusing its trigger first. Restore
    // to the visible trigger on collapse, but never underneath another modal.
    if (wasOpen.current && !state.open && !hasOtherDialog(panel.current)) trigger.current?.focus();
    wasOpen.current = state.open;
  }, [state.open, mobile]);
  useEffect(() => { if (state.open && focusRequest) composer.current?.focus(); }, [state.open, focusRequest]);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.defaultPrevented) return;
      // Do not steal shortcuts or Escape from another modal (including native dialogs).
      if (hasOtherDialog(panel.current)) return;
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault(); controller.setOpen(true); setFocusRequest((n) => n + 1);
      } else if (e.key === "Escape" && state.open && !mobile) {
        e.preventDefault(); controller.setOpen(false); trigger.current?.focus();
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [controller, state.open, mobile]);
  if (!state.userId) return null;
  const status = state.status;
  const blocked = !state.active || !status?.available || status.budget_state !== "ok";
  const billing = workspaces.find((w) => w.id === state.active?.workspace_id);
  return <>
    <button ref={trigger} className={`mind-trigger ${state.open ? "mind-trigger-hidden" : ""}`} aria-label="Open Mind" aria-expanded={state.open} aria-controls="mind-dock" onClick={() => { controller.setOpen(true); setFocusRequest((n) => n + 1); }}>Mind <kbd>⌘/Ctrl K</kbd></button>
    {state.open && <>
      {mobile && <div className="mind-scrim" onClick={() => controller.setOpen(false)} />}
      <aside id="mind-dock" ref={panel} className="mind-dock" role={mobile ? "dialog" : "complementary"} aria-modal={mobile ? true : undefined} aria-labelledby="mind-heading" tabIndex={-1}>
        <header><div><h2 id="mind-heading">Mind</h2><div className="muted">{mindModelLine(status)}</div></div><button className="btn sm ghost" aria-label="Collapse Mind" onClick={() => { controller.setOpen(false); if (!mobile) trigger.current?.focus(); }}>Collapse</button></header>
        <div className="mind-thread-controls">
          <label className="field"><span>Thread</span><select className="select" value={state.active?.id || ""} disabled={state.busy} onChange={(e) => { if (e.target.value) void controller.select(e.target.value); }}><option value="">Choose a thread</option>{state.threads.map((thread) => <option key={thread.id} value={thread.id}>{thread.title || "Untitled thread"}</option>)}</select></label>
          <div className="mind-actions"><button className="btn sm" disabled={state.busy || !currentWorkspace} onClick={() => { if (currentWorkspace) void controller.create(currentWorkspace); }}>New thread</button><button className="btn sm ghost" disabled={state.busy} onClick={() => void controller.reload()}>Reload</button>{state.busy && <button className="btn sm ghost" onClick={() => controller.cancel()}>Stop</button>}</div>
          {state.active && <p className="muted">Billing workspace: {billing?.name || state.active.workspace_id} · Thread cost: {mindCost(state.active)}</p>}
          {status && !status.available && <p role="status" className="err">Mind unavailable: {status.reason || "The selected model is not available."}</p>}
          {status?.budget_state === "budget_exceeded" && <p role="status" className="err">Daily budget reached. Spending resets at 00:00 UTC.</p>}
          {status?.budget_state === "pending_accounting" && <p role="status">Spending is still being accounted for. New messages are paused until accounting completes.</p>}
          {state.error && <p className="err" role="alert">{state.error}</p>}
        </div>
        <div ref={messages} className="mind-messages" aria-label="Mind messages" onScroll={() => {
          if (messages.current) setNewReply(scroll.current.onScroll(messages.current));
        }}>
          {state.messages.map((message) => <article key={message.id}><b>{message.role === "user" ? "You" : "Mind"}</b><p className="mind-text">{message.content}</p><Citations citations={message.citations} />{message.cards?.map((card) => <ActionCard key={card.id} card={card} />)}</article>)}
          {state.draft && <p className="mind-text">{state.draft}</p>}
          {state.busy && <p className="muted">{state.progress || "Loading…"}</p>}
        </div>
        {newReply && <button className="btn sm mind-new-reply" onClick={() => {
          if (messages.current) scroll.current.jump(messages.current);
          setNewReply(false);
        }}>New reply ↓</button>}
        <div className="mind-compose">
          {state.context && <div className="mind-context"><span>Context: {contextLabel}</span><button className="btn sm ghost" aria-label="Remove Mind context" onClick={() => controller.removeContext()}>Remove</button></div>}
          <div className="mind-suggestions">{mindSuggestions(state.context).map((ask) => <button className="btn sm ghost" key={ask} onClick={() => { setText(ask); composer.current?.focus(); }}>{ask}</button>)}</div>
          <form onSubmit={(e) => { e.preventDefault(); if (!blocked && !state.busy && text.trim()) { void controller.send(text); setText(""); } }}>
            <label className="field"><span>Message Mind</span><textarea ref={composer} className="textarea" rows={3} value={text} onChange={(e) => setText(e.target.value)} placeholder="Ask what is happening and why…" /></label>
            <button className="btn primary" disabled={blocked || state.busy || !text.trim()}>Send message</button>
          </form>
        </div>
      </aside>
    </>}
    <div className="sr" aria-live="polite" aria-atomic="true">{state.announcement && <span key={state.announcementId}>{state.announcement}</span>}</div>
  </>;
}
