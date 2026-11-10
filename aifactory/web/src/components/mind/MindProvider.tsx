import { createContext, useContext, useEffect, useMemo, useSyncExternalStore, type ReactNode } from "react";
import { api, apiResponse, type Me, type Role } from "../../api";
import { MindController, readMindSSE, type MindTransport } from "../../mind";
import { createMindHttpTransport } from "../../mindHttp";

// The pure adapter is exercised against T1/T3 fixtures without a browser (mind.test.ts).
export const mindHttpTransport = createMindHttpTransport(api, apiResponse, readMindSSE);

const MindCtx = createContext<{ controller: MindController; roles: Record<string, Role>; workspaces: Me["workspaces"] } | null>(null);

/** Mount once above the routes. Pass null on sign-out to erase private state and abort requests. */
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
  if (!value) throw new Error("useMind requires MindProvider above the routes");
  const state = useSyncExternalStore(value.controller.subscribe, value.controller.getSnapshot);
  return { ...value, state };
}
