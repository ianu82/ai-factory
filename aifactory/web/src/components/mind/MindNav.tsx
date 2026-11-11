import { useEffect } from "react";
import { NavLink, useMatch, useNavigate } from "react-router-dom";
import { Brain, Plus } from "lucide-react";
import { useMind } from "./MindProvider";

const SHOWN = 6;

/**
 * "My Mind" in the sidebar, above Inbox: one click (or ⌘K / Ctrl+K) starts a conversation, and the person's
 * recent conversations sit nested underneath it. Conversations are personal and span workspaces.
 */
export function MindNav({ workspaceId }: { workspaceId: string }) {
  const { controller, state } = useMind();
  const navigate = useNavigate();
  const base = `/w/${workspaceId}`;
  const onHome = !!useMatch(`${base}/mind`);
  useEffect(() => { if (state.userId) void controller.list(); }, [controller, state.userId, state.active?.id, state.announcementId]);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.defaultPrevented || !(e.metaKey || e.ctrlKey) || e.key.toLowerCase() !== "k") return;
      if (document.querySelector('[role="dialog"][aria-modal="true"], dialog[open]')) return;
      e.preventDefault();
      navigate(`${base}/mind`);
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [navigate, base]);
  if (!state.userId) return null;
  const started = state.threads.filter((t) => t.title); // a conversation without a first message is not listed
  const threads = started.slice(0, SHOWN);
  return <div className="mind-nav">
    <div className="mind-nav-row">
      <NavLink to={`${base}/mind`} end className={({ isActive }) => `nav-item${isActive || onHome ? " active" : ""}`}><Brain /> My Mind</NavLink>
      <button type="button" className="mind-nav-new" title="New conversation (⌘K)" aria-label="New Mind conversation" onClick={() => navigate(`${base}/mind`)}><Plus /></button>
    </div>
    {threads.length > 0 && <ul className="mind-nav-threads">
      {threads.map((t) => <li key={t.id}>
        <NavLink to={`/w/${t.workspace_id}/mind/${t.id}`} className={({ isActive }) => `mind-nav-thread${isActive ? " active" : ""}`} title={t.title}>
          <span className="mind-nav-title">{t.title || "Untitled conversation"}</span>
          {t.outcome && <span className="ref mind-nav-badge">#{t.outcome.number}</span>}
        </NavLink>
      </li>)}
      {started.length > SHOWN && <li><NavLink to={`${base}/mind?all`} end className="mind-nav-thread more">All conversations</NavLink></li>}
    </ul>}
  </div>;
}
