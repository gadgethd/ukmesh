import http from 'node:http';

export const LOOKUP_HTTP_REQUEST_TIMEOUT_MS = 30_000;
const KEEP_ALIVE_AGENT_TIMEOUT_MS = 5_000;

export function createLookupAgents(): http.Agent[] {
  // A positive agent timeout lets Node shorten its free-socket lease using the
  // server's Keep-Alive hint. Zero otherwise disables that protection.
  return Array.from({ length: 128 }, (_, index) => new http.Agent({ keepAlive: true,
    timeout: KEEP_ALIVE_AGENT_TIMEOUT_MS, maxSockets: 1, localAddress: `127.0.1.${index + 1}` }));
}

export function setLookupRequestDeadline(request: http.ClientRequest): void {
  // Active requests retain their existing deadline, independently of idle reuse.
  request.setTimeout(LOOKUP_HTTP_REQUEST_TIMEOUT_MS, () => request.destroy(new Error('request timed out')));
}

/** A failed batch stops admission, settles its active slots, then rethrows the
 * original failure before its caller closes routes, sockets or database pools. */
export async function runLookupSamples<T>(samples: number, concurrency: 1 | 8,
  work: (index: number) => Promise<T>): Promise<T[]> {
  const results: T[] = [];
  let next = 0, failed = false, firstError: unknown;
  await Promise.all(Array.from({ length: concurrency }, async () => {
    while (!failed && next < samples) {
      const index = next++;
      try { results.push(await work(index)); }
      catch (error) { if (!failed) { failed = true; firstError = error; } }
    }
  }));
  if (failed) throw firstError;
  return results;
}
