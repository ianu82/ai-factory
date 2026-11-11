import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { Check, ChevronDown, CircleAlert, CircleCheck, CircleDot, ExternalLink, RotateCcw } from "lucide-react";
import type { MindCard, MindCitation, MindMessage, MindStep, Role } from "../../api";
import { canConfirm, formatDuration, mindLink, stepLabel } from "../../mind";
import { Markdown } from "../../markdown";

/** The person's side of a turn: their words, as written. */
export function UserTurn({ message }: { message: MindMessage }) {
  return <div className="mind-turn user"><div className="mind-bubble">{message.content}</div></div>;
}

/** Live "Working for 6s" line: ticks once a second while the turn runs. */
function Elapsed({ since }: { since: number }) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => { const id = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(id); }, []);
  return <span className="num">{formatDuration(now - since)}</span>;
}

/**
 * What Mind did for a reply, folded under one calm line. While the turn runs it shows the current step and a
 * live timer and opens the first time a step arrives; a finished reply starts folded ("Worked for 12s").
 */
export function WorkedFor({ steps, active, startedAt, durationMs }: { steps: MindStep[]; active: boolean; startedAt?: number | null; durationMs?: number }) {
  const [open, setOpen] = useState(active && steps.length > 0);
  const opened = useRef(false);
  useEffect(() => { if (active && steps.length && !opened.current) { setOpen(true); opened.current = true; } }, [active, steps.length]);
  useEffect(() => { if (!active) setOpen(false); }, [active]);
  if (!active && !steps.length) return null;
  const current = [...steps].reverse().find((s) => !s.done) || steps[steps.length - 1];
  const done = formatDuration(durationMs);
  const head = active
    ? <>{startedAt ? <>Working for <Elapsed since={startedAt} /></> : "Working"}{current && !open && <span className="mind-worked-now"> · {stepLabel(current.name, current.done)}</span>}</>
    : done ? `Worked for ${done}` : `${steps.length} step${steps.length === 1 ? "" : "s"}`;
  return <div className={`mind-worked${active ? " active" : ""}`}>
    <button type="button" className="mind-worked-head" aria-expanded={open} onClick={() => setOpen((v) => !v)} disabled={!steps.length}>
      {active && <span className="dot active" aria-hidden />}
      <span>{head}</span>
      {steps.length > 0 && <ChevronDown className={`chev${open ? " open" : ""}`} aria-hidden />}
    </button>
    {open && steps.length > 0 && <ol className="mind-steps">
      {steps.map((s, i) => <li key={i} className={s.done ? "done" : "running"}>
        {s.done ? <Check aria-hidden /> : <span className="dot active" aria-hidden />}
        <span>{stepLabel(s.name, s.done)}</span>
        {!!s.citations && <span className="faint num">{s.citations} item{s.citations === 1 ? "" : "s"}</span>}
      </li>)}
    </ol>}
  </div>;
}

export function citationKey(c: MindCitation) { return c.url || c.label; }

/** Factory objects a reply relied on, as links back into the app. */
export function Sources({ citations }: { citations: MindCitation[] }) {
  const unique = citations.filter((c, i) => citations.findIndex((d) => citationKey(d) === citationKey(c)) === i);
  if (!unique.length) return null;
  const shown = unique.slice(0, 6);
  return <div className="mind-sources" aria-label="Sources">
    {shown.map((c) => <SourceLink key={citationKey(c)} citation={c} />)}
    {unique.length > shown.length && <span className="faint">+{unique.length - shown.length} more</span>}
  </div>;
}

export function SourceLink({ citation }: { citation: MindCitation }) {
  const kind = mindLink(citation.url);
  if (kind === "internal") return <Link className="mind-source" to={citation.url}>{citation.label}</Link>;
  if (kind === "external") return <a className="mind-source" href={citation.url} target="_blank" rel="noopener noreferrer">{citation.label}<ExternalLink aria-hidden /></a>;
  return <span className="mind-source">{citation.label}</span>;
}

/** Mind's side of a turn: its steps, its answer, the actions it proposes and what it relied on. */
export function AssistantTurn({ message, roles, pending, busy, onRetry, actions }: {
  message: MindMessage; roles: Record<string, Role>; pending: string[]; busy: boolean; onRetry?: () => void;
  actions: { confirm: (card: MindCard) => void; dismiss: (card: MindCard) => void; revise: (card: MindCard, fields: Record<string, string>) => void };
}) {
  const cards = message.cards || [];
  return <div className="mind-turn assistant">
    <WorkedFor steps={message.steps || []} active={false} durationMs={message.durationMs} />
    {message.content && <Markdown text={message.content} />}
    {cards.map((card) => <ActionApproval key={card.id} card={card} roles={roles} pending={pending.includes(card.id)} busy={busy} actions={actions} />)}
    <Sources citations={message.citations || []} />
    {message.failed && <div className="mind-failed"><CircleAlert aria-hidden />
      <span>{message.content ? "This reply stopped before it finished." : "Mind could not answer this one."}</span>
      {onRetry && <button type="button" className="btn sm ghost" onClick={onRetry} disabled={busy}><RotateCcw /> Try again</button>}
    </div>}
  </div>;
}

/** The turn in progress: live steps, then the answer as it streams. */
export function LiveTurn({ draft, steps, startedAt }: { draft: string; steps: MindStep[]; startedAt: number | null }) {
  return <div className="mind-turn assistant live" aria-busy>
    <WorkedFor steps={steps} active startedAt={startedAt} />
    {draft && <Markdown text={draft} />}
  </div>;
}

/**
 * An action Mind proposes, waiting on the person. Nothing runs until they approve it here; the effect line says
 * exactly what will happen, and the text can be edited first.
 */
/** What each card field means. Only `text` is editable (T3 accepts only fields.text); the rest are what Mind proposed. */
const FIELD_LABELS: Record<string, string> = {
  option: "Choice", title: "Title", repository: "Repository", repositories: "Repositories", priority: "Priority", plan: "Plan",
};
const TEXT_LABELS: Record<string, string> = {
  resolve_decision: "Comment", send_guidance: "Guidance", create_outcome: "Brief", build_plan: "Summary",
};

function CardFacts({ card }: { card: MindCard }) {
  const facts = Object.entries(card.fields).filter(([key, value]) => key !== "text" && key !== "plan" && value);
  const plan = (card.fields.plan || "").split("\n").map((line) => line.trim()).filter(Boolean);
  return <>
    {facts.length > 0 && <dl className="mind-action-facts">
      {facts.map(([key, value]) => <div key={key}><dt>{FIELD_LABELS[key] || key}</dt><dd>{value}</dd></div>)}
    </dl>}
    {plan.length > 0 && <div className="mind-action-plan">
      <span className="mind-action-label">{FIELD_LABELS.plan}</span>
      <ol>{plan.map((line, i) => <li key={i}>{line}</li>)}</ol>
    </div>}
  </>;
}

export function ActionApproval({ card, roles, pending, busy, actions }: {
  card: MindCard; roles: Record<string, Role>; pending: boolean; busy: boolean;
  actions: { confirm: (card: MindCard) => void; dismiss: (card: MindCard) => void; revise: (card: MindCard, fields: Record<string, string>) => void };
}) {
  const [editing, setEditing] = useState(false);
  const [text, setText] = useState(card.fields.text ?? "");
  useEffect(() => { setText(card.fields.text ?? ""); setEditing(false); }, [card.revision, card.status, card.fields]);
  const eligible = canConfirm(card, roles, pending ? [card.id] : []);
  const disabled = pending || busy;
  const editable = "text" in card.fields;
  const textLabel = TEXT_LABELS[card.kind] || "Text";
  if (card.status === "confirmed") return <div className="mind-action done">
    <CircleCheck aria-hidden /><div><b>{card.title}</b><p>{card.result?.text || "Done."}</p>
      {!!card.result?.citations?.length && <Sources citations={card.result.citations} />}</div>
  </div>;
  if (card.status === "dismissed") return <div className="mind-action quiet"><span className="muted">Dismissed · {card.title}</span></div>;
  return <section className={`mind-action${card.status === "stale" ? " stale" : ""}`} aria-label={`Proposed: ${card.title}`} aria-busy={pending}>
    <header><CircleDot aria-hidden /><span className="mind-action-kind">Needs your approval</span></header>
    <h4>{card.title}</h4>
    {card.effect && <p className="mind-action-effect">{card.effect}</p>}
    <CardFacts card={card} />
    {editing ? <form onSubmit={(e) => { e.preventDefault(); actions.revise(card, { ...card.fields, text }); }}>
      <label className="mind-action-field">
        <span className="mind-action-label">{textLabel}</span>
        <textarea className="textarea" value={text} disabled={disabled} rows={Math.min(10, Math.max(3, text.split("\n").length + 1))}
          onChange={(e) => setText(e.target.value)} autoFocus />
      </label>
      <div className="mind-action-buttons">
        <button className="btn sm primary" disabled={disabled}>Save changes</button>
        <button type="button" className="btn sm ghost" disabled={disabled} onClick={() => { setEditing(false); setText(card.fields.text ?? ""); }}>Cancel</button>
      </div>
    </form> : !!card.fields.text && <blockquote className="mind-action-text" aria-label={textLabel}>{card.fields.text}</blockquote>}
    {card.status === "stale" && <p role="status" className="mind-action-note">The target changed since Mind proposed this. Ask Mind to look again.</p>}
    {card.status === "executing" && <p role="status" className="mind-action-note">Running… reload in a moment for the result.</p>}
    {card.status === "proposed" && !editing && <div className="mind-action-buttons">
      <button className="btn sm primary" disabled={disabled || !eligible} onClick={() => actions.confirm(card)}>{pending ? "Working…" : "Approve"}</button>
      {editable && <button className="btn sm" disabled={disabled || !eligible} onClick={() => setEditing(true)}>Edit {textLabel.toLowerCase()}</button>}
      <button className="btn sm ghost" disabled={disabled} onClick={() => actions.dismiss(card)}>Dismiss</button>
      {!eligible && !pending && <span className="muted mind-action-role">You need member access in that workspace to approve.</span>}
    </div>}
  </section>;
}
