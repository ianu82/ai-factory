/** Remove dismissed alerts without changing unrelated inbox data or needless cache references. */
export function withoutAlerts<T extends { alerts?: { id: string }[] }>(data: T | undefined, ids: ReadonlySet<string>): T | undefined {
  if (!data?.alerts?.some((alert) => ids.has(alert.id))) return data;
  return { ...data, alerts: data.alerts.filter((alert) => !ids.has(alert.id)) };
}
