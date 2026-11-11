import { ApiResponseError } from './api.js';

/** Shared by the live map and packet detail: count HTTP attempts, including 202 polls. */
export function createPathResolutionFetcher(
  fetcher: typeof fetch = (...args) => fetch(...args),
  now = () => Date.now(),
): typeof fetch {
  let attempts: number[] = [];
  let inFlight = 0;
  let blockedUntil = 0;
  let backoffMs = 60_000;

  return async (input, init = {}) => {
    const stamp = now();
    attempts = attempts.filter((time) => stamp - time < 60_000);
    if (stamp < blockedUntil || attempts.length >= 30 || inFlight >= 2) {
      throw new ApiResponseError('Path resolution is temporarily rate limited', 429, String(input));
    }
    attempts.push(stamp);
    inFlight += 1;
    try {
      const headers = new Headers(init.headers);
      if (!headers.has('Accept')) headers.set('Accept', 'application/json');
      const timeout = AbortSignal.timeout(12_000);
      const response = await fetcher(input, {
        ...init,
        headers,
        signal: init.signal ? AbortSignal.any([init.signal, timeout]) : timeout,
      });
      if (response.status === 429) {
        const retryAfter = response.headers.get('Retry-After');
        const seconds = Number(retryAfter);
        const retryMs = Number.isFinite(seconds)
          ? Math.max(0, seconds * 1_000)
          : Math.max(0, Date.parse(retryAfter ?? '') - now()) || 0;
        blockedUntil = Math.max(blockedUntil, now() + Math.max(backoffMs, retryMs));
        backoffMs = Math.min(300_000, backoffMs * 2);
      } else if (response.ok && now() >= blockedUntil) {
        backoffMs = 60_000;
      }
      return response;
    } finally {
      inFlight -= 1;
    }
  };
}

export const fetchPathResolution = createPathResolutionFetcher();
