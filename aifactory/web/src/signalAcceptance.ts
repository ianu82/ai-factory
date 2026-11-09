import type { Signal } from "./api";

export function signalAcceptanceReason(signal: Pick<Signal, "status" | "data">): string | undefined {
  return signal.status === "new" ? signal.data.acceptance_reason : undefined;
}
