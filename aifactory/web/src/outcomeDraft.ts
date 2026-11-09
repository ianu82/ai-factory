import { useRef } from "react";

/** One id per exact draft, retained after network errors (including lost responses). */
export function useOutcomeRequestId(draft: unknown) {
  const attempt = useRef<{ draft: string; id: string } | null>(null);
  const serialized = JSON.stringify(draft);
  if (!attempt.current || attempt.current.draft !== serialized) {
    attempt.current = { draft: serialized, id: crypto.randomUUID() };
  }
  return attempt.current.id;
}
