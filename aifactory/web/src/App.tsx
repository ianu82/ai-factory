import { createContext, useContext, useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Navigate, NavLink, Route, Routes, useLocation, useNavigate, useParams } from "react-router-dom";
import { Activity, GitPullRequest, Inbox as InboxIcon, LayoutDashboard, Library, ListChecks, LogOut, Menu, MessagesSquare, Plus, Radio, Settings as SettingsIcon, Target } from "lucide-react";
import { api, ApiError, type Me, type Meta, type Workspace } from "./api";
import { useLive } from "./live";
import { Avatar } from "./ui";
import { SignIn } from "./pages/SignIn";
import { InboxPage } from "./pages/Inbox";
import { OutcomesPage } from "./pages/Outcomes";
import { RunsPage } from "./pages/Runs";
import { DashboardPage } from "./pages/Dashboard";
import { MessageLogPage } from "./pages/MessageLog";
import { SignalsPage } from "./pages/Signals";
import { PullRequestsPage, usePulls } from "./pages/PullRequests";
import { DecisionsPage } from "./pages/Decisions";
import { SettingsPage } from "./pages/Settings";
import { NewOutcome } from "./pages/NewOutcome";
import { Onboarding } from "./onboarding/Onboarding";
import { memberHasDeferred, STEP_LABELS, useOnboarding } from "./onboarding/state";
import { MindProvider } from "./components/mind/MindProvider";
import { MindNav } from "./components/mind/MindNav";
import { MindConversation, MindHome } from "./pages/Mind";

type Ctx = { me: Me; meta: Meta; ws: Workspace; live: string; openCapture: () => void };
const WorkspaceCtx = createContext<Ctx | null>(null);
export const useWs = () => useContext(WorkspaceCtx)!;

export function App() {
  const me = useQuery({ queryKey: ["me"], queryFn: () => api<Me>("/api/me"), retry: false });
  const meta = useQuery({ queryKey: ["meta"], queryFn: () => api<Meta>("/api/meta"), staleTime: 60_000 });
  const unauth = me.error instanceof ApiError && me.error.status === 401;
  // One Mind for the signed-in person, above every route: threads survive navigation and workspace switches,
  // and signing out (no person) erases private state.
  return (
    <MindProvider me={unauth ? null : me.data ?? null}>
    <Routes>
      <Route path="/signin" element={<SignIn meta={meta.data} />} />
      <Route
        path="/w/:wsId/*"
        element={unauth ? <Navigate to="/signin" replace /> : me.data && meta.data ? <Shell me={me.data} meta={meta.data} /> : <Loading error={me.error} />}
      />
      <Route
        path="/go/:section"
        element={unauth ? <Navigate to="/signin" replace /> : me.data
          ? me.data.workspaces[0] ? <GoTo workspaceId={me.data.workspaces[0].id} /> : <NoWorkspace />
          : <Loading error={me.error} />}
      />
      <Route
        path="*"
        element={unauth ? <Navigate to="/signin" replace /> : me.data
          ? me.data.workspaces[0] ? <Navigate to={`/w/${me.data.workspaces[0].id}/inbox`} replace /> : <NoWorkspace />
          : <Loading error={me.error} />}
      />
    </Routes>
    </MindProvider>
  );
}

function Loading({ error }: { error?: unknown }) {
  return (
    <div style={{ display: "grid", placeItems: "center", height: "100%", color: "var(--muted)" }}>
      {error ? `Could not reach the Factory: ${(error as Error).message}` : "Loading…"}
    </div>
  );
}

/** Stable deep links (used by runtime QA and docs): /go/outcomes → the first workspace's Outcomes. */
function GoTo({ workspaceId }: { workspaceId: string }) {
  const { section = "inbox" } = useParams();
  const allowed = ["mind", "inbox", "outcomes", "pulls", "runs", "signals", "decisions", "dashboard", "messages", "settings", "onboarding"];
  return <Navigate to={`/w/${workspaceId}/${allowed.includes(section) ? section : "inbox"}`} replace />;
}

function MindHomeRoute() {
  const { ws } = useWs();
  return <MindHome workspaceId={ws.id} />;
}

function MindThreadRoute() {
  const { ws } = useWs();
  const { threadId = "" } = useParams();
  return <MindConversation workspaceId={ws.id} threadId={threadId} />;
}

function NoWorkspace() {
  return <div className="empty"><h3>No workspace yet</h3><p>Ask an organization admin to add you to a workspace.</p></div>;
}

function Shell({ me, meta }: { me: Me; meta: Meta }) {
  const { wsId } = useParams();
  const ws = me.workspaces.find((w) => w.id === wsId);
  const [capture, setCapture] = useState(false);
  const [navOpen, setNavOpen] = useState(false);
  const live = useLive(ws?.id);
  const location = useLocation();
  const onboarding = useOnboarding(ws?.id || "");
  const base = `/w/${ws?.id}`;
  const onOnboarding = location.pathname === `${base}/onboarding` || location.pathname === `${base}/onboarding/`;
  // Only default Inbox entry is gated. Explicit decision, Settings and outcome
  // deep links remain available even while readiness is loading or unresolved.
  const inboxEntry = location.pathname === `${base}/inbox` || location.pathname === `${base}/inbox/`;
  const awaitingEntry = inboxEntry && !onboarding.data;
  const shouldOnboard = inboxEntry && onboarding.data && !onboarding.data.ready && !onboarding.data.deferred
    && ((ws?.role === "owner" || ws?.role === "admin") || !memberHasDeferred(me.user.id, ws?.id || ""));
  const blockCapture = onOnboarding || awaitingEntry || !!shouldOnboard;
  useEffect(() => setNavOpen(false), [location.pathname]);
  useEffect(() => { setCapture(false); }, [location.pathname, ws?.id]);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const typing = (e.target as HTMLElement)?.closest("input, textarea, select, [contenteditable]");
      if (!blockCapture && !typing && e.key === "c" && !e.metaKey && !e.ctrlKey && !e.altKey) { e.preventDefault(); setCapture(true); }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [blockCapture]);
  if (!ws) return <Navigate to="/" replace />;
  const ctx: Ctx = { me, meta, ws, live, openCapture: () => { if (!blockCapture) setCapture(true); } };
  return (
    <WorkspaceCtx.Provider value={ctx}>
      {onOnboarding ? <Routes><Route path="onboarding" element={<Onboarding key={ws.id} />} /></Routes> : awaitingEntry ? <Loading error={onboarding.error} /> : shouldOnboard ? <Navigate to={`${base}/onboarding`} replace /> : <>
      <div className={`shell ${navOpen ? "nav-open" : ""}`}>
        <Sidebar />
        <div className="main">
          <div className="mobile-bar">
            <button className="icon-btn" onClick={() => setNavOpen(true)} aria-label="Open navigation"><Menu /></button>
            <span style={{ fontWeight: 600 }}>{ws.name}</span>
            <span style={{ flex: 1 }} />
            <button className="btn sm primary" onClick={() => setCapture(true)}><Plus /> New</button>
          </div>
          {ws.paused && <div className="banner">This workspace is paused. Running work finishes its current step; nothing new starts.</div>}
          {live === "offline" && <div className="banner">Live updates disconnected. Reconnecting…</div>}
          <Routes>
            <Route path="mind" element={<MindHomeRoute />} />
            <Route path="mind/:threadId" element={<MindThreadRoute />} />
            <Route path="inbox" element={<InboxPage />} />
            <Route path="inbox/:decisionId" element={<InboxPage />} />
            <Route path="outcomes" element={<OutcomesPage />} />
            <Route path="outcomes/:outcomeId" element={<OutcomesPage />} />
            <Route path="pulls" element={<PullRequestsPage />} />
            <Route path="runs" element={<RunsPage />} />
            <Route path="dashboard" element={<DashboardPage />} />
            <Route path="messages" element={<MessageLogPage />} />
            <Route path="signals" element={<SignalsPage />} />
            <Route path="decisions" element={<DecisionsPage />} />
            <Route path="settings" element={<SettingsPage />} />
            <Route path="settings/:section" element={<SettingsPage />} />
            <Route path="*" element={<Navigate to="inbox" replace />} />
          </Routes>
        </div>
        {navOpen && <div className="scrim" style={{ zIndex: 30 }} onClick={() => setNavOpen(false)} />}
      </div>
      {capture && !blockCapture && <NewOutcome onClose={() => setCapture(false)} />}
      </>}
    </WorkspaceCtx.Provider>
  );
}

function Sidebar() {
  const { me, ws, openCapture } = useWs();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const onboarding = useOnboarding(ws.id);
  // Steps still missing, plus completed ones whose credential could not be confirmed (for example a GitHub token
  // tested before repository-scoped checks existed). Nothing left: no link.
  const setupSteps = onboarding.data ? Array.from(new Set([...(onboarding.data.missing_steps || []),
    ...(onboarding.data.validation_uncertain_steps || [])])) : [];
  const setupLeft = onboarding.data && (!onboarding.data.ready || onboarding.data.deferred) ? Math.max(setupSteps.length, 1)
    : setupSteps.length;
  const setupTitle = setupSteps.length ? `Left to do: ${setupSteps.map((s) => STEP_LABELS[s] || s).join(", ")}` : "Finish setting up this workspace";
  const inbox = useQuery({
    queryKey: ["inbox", ws.id],
    queryFn: () => api<{ decisions: unknown[]; alerts?: unknown[] }>(`/api/workspaces/${ws.id}/inbox`),
    refetchInterval: 30_000,
  });
  const pulls = usePulls("open");
  const needsYou = (inbox.data?.decisions.length || 0) + (inbox.data?.alerts?.length || 0);
  const pullsNeedingYou = (pulls.data?.counts.ready || 0) + (pulls.data?.counts.attention || 0);
  const [switching, setSwitching] = useState(false);
  const base = `/w/${ws.id}`;
  const signOut = async () => {
    const r = await api<{ logout_url?: string }>("/auth/logout", { method: "POST" });
    qc.clear();
    if (r.logout_url) window.location.href = r.logout_url;
    else navigate("/signin");
  };
  return (
    <aside className="sidebar" aria-label="Main">
      <div className="brand">
        <img src="/minds-icon.svg" alt="" />
        <span className="word">FACTORY</span>
      </div>
      <div style={{ position: "relative" }}>
        <button className="ws-switch" onClick={() => setSwitching((v) => !v)} aria-expanded={switching} aria-haspopup="listbox">
          <span className="ws-mark">{ws.name.slice(0, 1).toUpperCase()}</span>
          <span style={{ minWidth: 0 }}>
            <div className="name">{ws.name}</div>
            <div className="org">{ws.org_name}</div>
          </span>
        </button>
        {switching && (
          <div role="listbox" style={{ position: "absolute", top: "100%", left: 0, right: 0, background: "var(--surface)", borderRadius: 8, boxShadow: "var(--shadow-pop)", zIndex: 20, padding: 4 }}>
            {me.workspaces.map((w) => (
              <button key={w.id} role="option" aria-selected={w.id === ws.id} className="ws-switch" style={{ marginBottom: 0 }}
                onClick={() => { setSwitching(false); navigate(`/w/${w.id}/inbox`); }}>
                <span className="ws-mark">{w.name.slice(0, 1).toUpperCase()}</span>
                <span style={{ minWidth: 0 }}><div className="name">{w.name}</div><div className="org">{w.org_name} · {w.role}</div></span>
              </button>
            ))}
          </div>
        )}
      </div>
      <button className="capture" onClick={openCapture}><Plus size={15} /> New outcome <kbd>C</kbd></button>
      <MindNav workspaceId={ws.id} />
      <NavLink to={`${base}/inbox`} className="nav-item"><InboxIcon /> Inbox
        {!!needsYou && <span className="count" aria-label={`${needsYou} need you`}>{needsYou}</span>}
      </NavLink>
      <NavLink to={`${base}/outcomes`} className="nav-item"><Target /> Outcomes</NavLink>
      <NavLink to={`${base}/pulls`} className="nav-item"><GitPullRequest /> Pull requests
        {!!pullsNeedingYou && <span className="count" aria-label={`${pullsNeedingYou} pull requests ready or needing attention`}>{pullsNeedingYou}</span>}
      </NavLink>
      <NavLink to={`${base}/runs`} className="nav-item"><Activity /> Runs</NavLink>
      <NavLink to={`${base}/signals`} className="nav-item"><Radio /> Signals</NavLink>
      <NavLink to={`${base}/decisions`} className="nav-item"><Library /> Decisions</NavLink>
      <div className="sidebar-foot">
        {setupLeft > 0 && (
          <NavLink to={`${base}/onboarding`} className="nav-item" title={setupTitle}>
            <ListChecks /> {onboarding.data?.deferred ? "Resume setup" : "Finish setup"}
            <span className="count" aria-label={`${setupLeft} setup ${setupLeft === 1 ? "step" : "steps"} left`}>{setupLeft}</span>
          </NavLink>
        )}
        <NavLink to={`${base}/dashboard`} className="nav-item"><LayoutDashboard /> Dashboard</NavLink>
        <NavLink to={`${base}/messages`} className="nav-item"><MessagesSquare /> Message Log</NavLink>
        <NavLink to={`${base}/settings`} className="nav-item"><SettingsIcon /> Settings</NavLink>
        <div className="nav-sep" />
        <div className="me">
          <Avatar name={me.user.name || me.user.email || ""} />
          <span className="who">{me.user.name || me.user.email}</span>
          <span style={{ flex: 1 }} />
          <button className="icon-btn" onClick={signOut} aria-label="Sign out" title="Sign out"><LogOut /></button>
        </div>
      </div>
    </aside>
  );
}
