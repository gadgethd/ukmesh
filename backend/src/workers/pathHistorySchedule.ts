export const PATH_HISTORY_REFRESH_INTERVAL_MS = 60 * 60 * 1_000;

export function pathHistoryRetryIntervalMs(rawValue: string | undefined): number {
  const parsed = Number(rawValue ?? 60_000) || 60_000;
  return Math.min(5 * 60_000, Math.max(10_000, parsed));
}

export function pathHistoryNextDelayMs(retrySoon: boolean, retryIntervalMs: number): number {
  return retrySoon ? retryIntervalMs : PATH_HISTORY_REFRESH_INTERVAL_MS;
}
