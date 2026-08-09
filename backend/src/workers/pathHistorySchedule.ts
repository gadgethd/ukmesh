export const PATH_HISTORY_REFRESH_INTERVAL_MS = 60 * 60 * 1_000;
export const PATH_HISTORY_MAX_RETRY_INTERVAL_MS = 30 * 60 * 1_000;

export function pathHistoryRetryIntervalMs(rawValue: string | undefined): number {
  const parsed = Number(rawValue ?? 5 * 60_000) || 5 * 60_000;
  return Math.min(PATH_HISTORY_MAX_RETRY_INTERVAL_MS, Math.max(5 * 60_000, parsed));
}

export function pathHistoryRetryDelayMs(retryAttempt: number, retryIntervalMs: number): number {
  const exponent = Math.max(0, Math.min(10, Math.trunc(retryAttempt) - 1));
  return Math.min(PATH_HISTORY_MAX_RETRY_INTERVAL_MS, retryIntervalMs * (2 ** exponent));
}

export function pathHistoryNextDelayMs(
  retrySoon: boolean,
  retryIntervalMs: number,
  retryAttempt = 1,
): number {
  return retrySoon
    ? pathHistoryRetryDelayMs(retryAttempt, retryIntervalMs)
    : PATH_HISTORY_REFRESH_INTERVAL_MS;
}
