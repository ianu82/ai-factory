import { useMemo } from "react";
import { useInfiniteQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import { ArrowUpRight, Brain, ClipboardCheck, ClipboardList, Cog, Compass, GitMerge, Hammer, ListChecks, Radar, Scale,
  Send, ShieldCheck, User, Users, Workflow, X } from "lucide-react";
import { api, type AgentMessage, type MessagePage, type MessageParty } from "../api";
import { useWs } from "../App";
import { clock, type Tone } from "../format";
import { Dot, Empty, Skeleton } from "../ui";
import { INTENT_FILTERS, PARTY_FILTERS, byDay, messagesUrl, verb } from "../messageLog";

const ICONS: Record<string, typeof Brain> = {
  mind: Brain, lead: Compass, person: User, people: Users, operator: Send, factory: Cog, planner: ClipboardList,
  plan_reviewer: ClipboardCheck, builder: Hammer, checks: ListChecks, verifier: ShieldCheck, ci: Workflow,
  judge: Scale, supervisor: Radar, merge_manager: GitMerge,
};
const TONES: Record<string, Tone> = { success: "good", warn: "human", error: "bad" };

/** The Message Log: who told whom what across the Factory, newest first, live. */
export function MessageLogPage() {
  const { ws } = useWs();
  const [params, setParams] = useSearchParams();
  const party = params.get("party") || "";
  const intent = params.get("intent") || "";
  const outcome = params.get("outcome") || "";
  const choose = (key: string, value: string) => {
    const next = new URLSearchParams(params);
    if (value) next.set(key, value); else next.delete(key);
    setParams(next, { replace: true });
  };
  const log = useInfiniteQuery({
    queryKey: ["messages", ws.id, party, intent, outcome],
    initialPageParam: 0,
    queryFn: ({ pageParam }) => api<MessagePage>(messagesUrl(ws.id, { party, intent, outcome }, pageParam || undefined)),
    getNextPageParam: (page) => page.next_cursor ?? undefined,
    refetchInterval: 15_000,
  });
  const messages = useMemo(() => (log.data?.pages || []).flatMap((p) => p.items), [log.data]);
  const days = byDay(messages);
  const focused = outcome ? messages.find((m) => m.about.outcome?.id === outcome)?.about.outcome : null;

  return (
    <>
      <div className="page-head">
        <h1>Message Log</h1>
        <span className="msg-lede hide-sm">Who told whom what, across the Factory's agents and people</span>
      </div>
      <div className="toolbar msg-toolbar">
        <div role="tablist" aria-label="Filter by party" className="msg-chips">
          {PARTY_FILTERS.map((f) => (
            <button key={f.key || "all"} role="tab" aria-selected={party === f.key} className={`chip ${party === f.key ? "on" : ""}`}
              onClick={() => choose("party", f.key)}>{f.label}</button>
          ))}
        </div>
        <span className="msg-divider" aria-hidden />
        <div role="tablist" aria-label="Filter by intent" className="msg-chips">
          {INTENT_FILTERS.map((f) => (
            <button key={f.key || "all"} role="tab" aria-selected={intent === f.key} className={`chip ${intent === f.key ? "on" : ""}`}
              onClick={() => choose("intent", f.key)}>{f.label}</button>
          ))}
        </div>
        {outcome && (
          <button className="chip on msg-focus" onClick={() => choose("outcome", "")} aria-label="Show every outcome">
            {focused ? `#${focused.number} ${focused.title}` : "One outcome"} <X size={13} />
          </button>
        )}
      </div>
      <div className="page-body">
        {log.isLoading ? <Skeleton /> : log.isError ? (
          <Empty title="Couldn't load the Message Log">{(log.error as Error).message}</Empty>
        ) : messages.length === 0 ? (
          <Empty title={party || intent || outcome ? "No messages match" : "No messages yet"}
            action={log.hasNextPage ? <OlderButton loading={log.isFetchingNextPage} onClick={() => void log.fetchNextPage()} /> : undefined}>
            {log.hasNextPage ? "None among the latest messages; older ones may match."
              : party || intent || outcome ? "Try another filter."
              : "Plans sent for review, verifier findings, decisions asked and answered, and relayed commands appear here as they happen."}
          </Empty>
        ) : (
          <>
            {days.map((group) => (
              <section key={group.key} aria-label={group.day}>
                <h2 className="list-section">{group.day}</h2>
                <ol className="rows msg-rows">
                  {group.messages.map((m) => <MessageRow key={m.id} m={m} workspaceId={ws.id} onOutcome={(id) => choose("outcome", id)} />)}
                </ol>
              </section>
            ))}
            {log.hasNextPage && (
              <div className="msg-more"><OlderButton loading={log.isFetchingNextPage} onClick={() => void log.fetchNextPage()} /></div>
            )}
          </>
        )}
      </div>
    </>
  );
}

function OlderButton({ loading, onClick }: { loading: boolean; onClick: () => void }) {
  return <button className="btn" disabled={loading} onClick={onClick}>{loading ? "Loading…" : "Load older messages"}</button>;
}

function Party({ p }: { p: MessageParty }) {
  const Icon = ICONS[p.role] || Cog;
  return (
    <span className={`msg-party ${p.role}`} title={p.for ? `${p.label}, acting for ${p.for}` : p.label}>
      <Icon aria-hidden /> {p.label}
    </span>
  );
}

function MessageRow({ m, workspaceId, onOutcome }: { m: AgentMessage; workspaceId: string; onOutcome: (id: string) => void }) {
  const tone = TONES[m.level] || "idle";
  const { outcome, task, pr } = m.about;
  return (
    <li className="msg-row">
      <Dot tone={tone} title={m.level === "info" ? undefined : m.level} />
      <time className="msg-time" dateTime={m.at} title={new Date(m.at).toLocaleString()}>{clock(m.at)}</time>
      <div className="msg-body">
        <div className="msg-line">
          <Party p={m.from} /> <span className="msg-verb">{verb(m.intent)}</span> <Party p={m.to} />
        </div>
        <p className="msg-says">{m.says}</p>
        {outcome && (
          <div className="msg-about">
            <button className="msg-outcome" onClick={() => onOutcome(outcome.id)} title="Show this outcome's conversation">
              <span className="ref">#{outcome.number}</span> {outcome.title}
            </button>
            {task && <span className="ref">{task.key}</span>}
            {pr && <span className="ref">PR #{pr}</span>}
            <Link className="msg-open" to={`/w/${workspaceId}/outcomes/${outcome.id}`} aria-label={`Open outcome #${outcome.number}`}>
              <ArrowUpRight aria-hidden />
            </Link>
          </div>
        )}
      </div>
    </li>
  );
}
