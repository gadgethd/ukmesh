import { LINK_V3_KEYS } from '../queue/linkQueueV3.js';

/** The link worker stores Unix seconds here independently of RF/DB writes. */
export const LINK_WORKER_HEARTBEAT_KEY = LINK_V3_KEYS.workerHeartbeat;

export function workerHeartbeatAge(value: string | null, nowSeconds = Date.now() / 1_000): number {
  const timestamp = value == null || value.trim() === '' ? NaN : Number(value);
  if (!Number.isFinite(timestamp) || timestamp <= 0 || timestamp > nowSeconds + 60) return -1;
  return Math.max(0, nowSeconds - timestamp);
}

export async function readLinkWorkerHeartbeatAge(
  client: { get(key: string): Promise<string | null> },
  now: () => number = Date.now,
): Promise<number> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error('worker heartbeat read timed out')), 2_000);
      timer.unref();
    });
    const timestamp = await Promise.race([client.get(LINK_WORKER_HEARTBEAT_KEY), timeout]);
    return workerHeartbeatAge(timestamp, now() / 1_000);
  } catch {
    // Never retain a previously healthy reading when Redis cannot confirm it.
    return -1;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export function linkWorkerHeartbeatCollector(
  client: () => { get(key: string): Promise<string | null> },
  gauge: { set(labels: { worker: string }, value: number): void },
  now: () => number = Date.now,
): () => Promise<void> {
  return async () => {
    let age = -1;
    try {
      age = await readLinkWorkerHeartbeatAge(client(), now);
    } catch {
      // Client initialization can fail before the bounded Redis read begins.
    }
    gauge.set({ worker: 'link' }, age);
  };
}
